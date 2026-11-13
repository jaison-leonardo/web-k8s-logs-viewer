"""Recolector: sigue los logs de cada (pod, contenedor) y los entrega al storage.

Cada stream corre en su propio hilo con:
- reintento con backoff exponencial ante cortes, timeouts o expiración de sesión;
- reanudación desde el último timestamp recibido (since_seconds + deduplicación
  en el storage), así un reinicio no repite ni pierde líneas;
- detección de formato y agrupación multilínea (logformats.StreamParser);
- estado visible en la UI: starting, following, reconnecting, ended, stopped, error.
"""

from __future__ import annotations

import logging
import queue
import re
import threading
import time
from datetime import datetime, timezone
from typing import NamedTuple

from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import ReadTimeoutError

from webapp import k8s
from webapp.config import Settings
from webapp.logformats import FormatRegistry, StreamParser
from webapp.storage import Storage, normalize_ts, parse_ts

log = logging.getLogger("pods.collector")

_K8S_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))\s?(.*)$")


# Errores que kubelet/runtime devuelve dentro del cuerpo de la respuesta de logs
# (HTTP 200, sin timestamp), p. ej. cuando el contenedor anterior ya fue eliminado.
_KUBELET_ERROR_RE = re.compile(
    r"^(?:unable to retrieve container logs for |failed to try resolving symlinks in path |"
    r"rpc error: code = |Internal error occurred: |failed to get parse function: )"
)


def split_k8s_timestamp(line: str):
    """kubectl/API con timestamps=True antepone 'RFC3339Nano ' a cada línea."""
    match = _K8S_TS_RE.match(line)
    if match:
        return normalize_ts(match.group(1)), match.group(2)
    return None, line


class TailKey(NamedTuple):
    environment: str
    namespace: str
    pod: str
    container: str

    @property
    def id(self):
        return "/".join(self)


class PodTail:
    def __init__(self, key: TailKey, workload: str, gateway: k8s.KubeGateway, settings: Settings,
                 parser: StreamParser, sink, created: str | None = None, on_deleted=None):
        self.key = key
        self.workload = workload
        self.created = created          # creationTimestamp del pod
        self.on_deleted = on_deleted    # callback(tail) cuando el pod desaparece
        self.gateway = gateway
        self.settings = settings
        self.parser = parser
        self.sink = sink
        self.state = "starting"
        self.message = None
        self.lines = 0
        self.kubelet_errors: list[str] = []
        self.last_ts: str | None = None
        self._started_at = time.time()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._response = None
        self._thread = threading.Thread(target=self._run, name=f"tail-{key.pod}-{key.container}", daemon=True)

    # ------------------------------------------------------------------ ciclo
    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        response = self._response
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
        self._thread.join(timeout=5)
        self._emit(self.parser.flush())
        self.state = "stopped"

    def is_active(self):
        return self.state in ("starting", "following", "reconnecting")

    def _since_seconds(self):
        reference = parse_ts(self.last_ts)
        if reference is None:
            seconds = time.time() - self._started_at
        else:
            seconds = (datetime.now(timezone.utc) - reference).total_seconds()
        # Margen por desfase de reloj; los duplicados los descarta el storage.
        return int(max(seconds, 0)) + 5

    def _run(self):
        backoff = 1.0
        first = True
        while not self._stop.is_set():
            opened_at, lines_before = time.monotonic(), self.lines
            try:
                self._response = self.gateway.open_log_stream(
                    self.key.namespace, self.key.pod, self.key.container,
                    tail_lines=self.settings.tail_lines if first else None,
                    since_seconds=None if first else self._since_seconds(),
                )
                first = False
                self._set_state("following", None)
                for line in k8s.iter_lines(self._response):
                    if self._stop.is_set():
                        break
                    self._handle_line(line)
                # El servidor cerró el stream: contenedor terminado/reiniciado.
            except ReadTimeoutError:
                # Pod sin actividad durante STREAM_TIMEOUT: se reconecta sin
                # marcarlo como error (since_seconds evita huecos).
                pass
            except ApiException as exc:
                if self._stop.is_set():
                    break
                if exc.status == 404:
                    self._set_state("ended", "El pod ya no existe.")
                    self._notify_deleted()
                    break
                if exc.status == 400 and self._pod_gone_or_finished():
                    break
                self._set_state("reconnecting" if exc.status not in (401, 403) else "error",
                                k8s.describe_api_error(exc))
            except Exception as exc:  # timeouts, cortes de red, plugin exec
                if self._stop.is_set():
                    break
                self._set_state("reconnecting", f"Conexión interrumpida: {exc.__class__.__name__}")
            finally:
                self._close_response()
                with self._lock:
                    self._emit(self.parser.flush())

            if self._stop.is_set():
                break
            if self.state == "following":
                if self._pod_gone_or_finished():
                    break
                healthy = time.monotonic() - opened_at > 5 or self.lines > lines_before
                if healthy:
                    backoff = 1.0
                    if self._stop.wait(0.5):
                        break
                    continue
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, self.settings.max_backoff_seconds)

    def _pod_gone_or_finished(self) -> bool:
        try:
            pod = self.gateway.get_pod(self.key.namespace, self.key.pod)
        except k8s.KubeError as exc:
            self._set_state("reconnecting", str(exc))
            return False
        if pod is None:
            self._set_state("ended", "El pod fue eliminado (rollout, escalado o reinicio).")
            self._notify_deleted()
            return True
        if pod["status"] in ("Succeeded", "Failed", "Completed"):
            self._set_state("ended", f"El pod terminó ({pod['status']}).")
            return True
        return False

    def _notify_deleted(self):
        if self.on_deleted and not self._stop.is_set():
            try:
                self.on_deleted(self)
            except Exception:
                log.exception("Error buscando el reemplazo de %s", self.key.pod)

    def _close_response(self):
        response, self._response = self._response, None
        if response is not None:
            try:
                response.close()
            except Exception:
                pass

    def _set_state(self, state, message):
        self.state = state
        self.message = message

    # ---------------------------------------------------------------- líneas
    def _handle_line(self, line: str):
        if not line.strip():
            return
        ts, text = split_k8s_timestamp(line)
        if ts is None and _KUBELET_ERROR_RE.match(text):
            # No es un log de la aplicación: se informa como estado del stream.
            self.kubelet_errors.append(text)
            self.message = f"Kubelet: {text[:200]}"
            return
        if ts:
            self.last_ts = ts
        with self._lock:
            self._emit(self.parser.feed(ts, text, text))

    def flush_if_idle(self):
        with self._lock:
            self._emit(self.parser.flush_if_idle())

    def _emit(self, entries):
        for entry in entries:
            self.lines += 1
            rec = entry.record
            self.sink({
                "environment": self.key.environment,
                "namespace": self.key.namespace,
                "workload": self.workload,
                "pod": self.key.pod,
                "container": self.key.container,
                "ts": entry.ts,
                "level": rec.level,
                "format": rec.format,
                "logger": rec.logger,
                "thread": rec.thread,
                "trace_id": rec.trace_id,
                "message": rec.message,
                "raw": entry.raw,
            })

    def status(self):
        return {
            "id": self.key.id,
            **self.key._asdict(),
            "workload": self.workload,
            "state": self.state,
            "message": self.message,
            "format": self.parser.dominant_format,
            "lines": self.lines,
            "orphans_skipped": self.parser.orphans_skipped,
            "last_ts": self.last_ts,
        }


class Collector:
    def __init__(self, settings: Settings, storage: Storage):
        self.settings = settings
        self.storage = storage
        self.registry = FormatRegistry(settings.custom_formats)
        self._tails: dict[TailKey, PodTail] = {}
        self._lock = threading.Lock()
        self._subscribers: list[queue.Queue] = []
        self._sub_lock = threading.Lock()
        self._stop = threading.Event()
        storage.on_inserted = self._broadcast
        self._ticker = threading.Thread(target=self._tick_loop, name="collector-ticker", daemon=True)
        self._ticker.start()

    # ----------------------------------------------------------- suscriptores
    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=5000)
        with self._sub_lock:
            self._subscribers.append(q)
        return q

    def unsubscribe(self, q):
        with self._sub_lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def subscriber_count(self):
        with self._sub_lock:
            return len(self._subscribers)

    def _broadcast(self, rows):
        with self._sub_lock:
            subs = list(self._subscribers)
        for q in subs:
            for row in rows:
                try:
                    q.put_nowait(row)
                except queue.Full:
                    # Cliente lento: se descarta lo más viejo para no frenar a los demás.
                    try:
                        q.get_nowait()
                        q.put_nowait(row)
                    except (queue.Empty, queue.Full):
                        pass

    # ------------------------------------------------------------- formatos
    def forced_format(self, workload: str, container: str) -> str | None:
        for override in self.settings.format_overrides:
            if not re.search(override.workload, workload):
                continue
            if override.container and not re.search(override.container, container):
                continue
            return None if override.format == "auto" else override.format
        return None

    def _new_parser(self, workload, container):
        return StreamParser(self.registry, self.forced_format(workload, container),
                            idle_seconds=self.settings.multiline_idle_seconds)

    # ---------------------------------------------------------------- tails
    def default_containers(self, pod: dict) -> list[str]:
        names = [c["name"] for c in pod["containers"]]
        selected = [n for n in names if n not in self.settings.exclude_containers]
        return selected or names[:1]

    def start(self, gateway: k8s.KubeGateway, environment: str, pod: dict,
              containers: list[str] | None = None) -> list[str]:
        valid = {c["name"] for c in pod["containers"]}
        wanted = [c for c in (containers or self.default_containers(pod)) if c in valid]
        started = []
        with self._lock:
            for container in wanted:
                key = TailKey(environment, pod["namespace"], pod["name"], container)
                existing = self._tails.get(key)
                if existing and existing.is_active():
                    continue
                tail = PodTail(key, pod["workload"], gateway, self.settings,
                               self._new_parser(pod["workload"], container), self.storage.enqueue,
                               created=pod.get("created"), on_deleted=self._on_pod_deleted)
                tail.start()
                self._tails[key] = tail
                started.append(key.id)
        return started

    # ------------------------------------------------------------- rollouts
    def _on_pod_deleted(self, tail: PodTail):
        if self.settings.follow_rollouts:
            threading.Thread(target=self._follow_replacement, args=(tail,),
                             name=f"replace-{tail.key.pod}", daemon=True).start()

    def _follow_replacement(self, tail: PodTail, delays=(1, 5, 15, 30, 60)):
        """Cuando un pod seguido desaparece (rollout, escalado, eliminación), sigue
        un pod más nuevo del mismo workload que aún no se esté siguiendo. Así QA
        no pierde los logs tras un despliegue durante la prueba."""
        for delay in delays:
            if self._stop.wait(delay):
                return
            with self._lock:
                if tail.key not in self._tails or self._tails[tail.key] is not tail:
                    return  # el usuario lo quitó de la lista
            try:
                pods = tail.gateway.list_pods(tail.key.namespace)
            except k8s.KubeError:
                continue
            with self._lock:
                followed = {(k.environment, k.namespace, k.pod) for k, t in self._tails.items() if t.is_active()}
            candidates = sorted(
                (p for p in pods
                 if p["workload"] == tail.workload and p["name"] != tail.key.pod
                 and p["status"] != "Terminating"
                 and (tail.key.environment, p["namespace"], p["name"]) not in followed
                 and tail.key.container in {c["name"] for c in p["containers"]}
                 and (p.get("created") or "") >= (tail.created or "")),
                key=lambda p: p.get("created") or "",
            )
            for candidate in candidates:
                if self.start(tail.gateway, tail.key.environment, candidate, [tail.key.container]):
                    tail.message = f"{tail.message} Se sigue automáticamente el reemplazo {candidate['name']}."
                    return

    def stop(self, ids: list[str]) -> list[str]:
        with self._lock:
            targets = [(k, t) for k, t in self._tails.items() if k.id in ids]
            for key, _ in targets:
                self._tails.pop(key, None)
        for _, tail in targets:
            tail.stop()
        return [k.id for k, _ in targets]

    def stop_all(self):
        with self._lock:
            tails = list(self._tails.values())
            self._tails.clear()
        for tail in tails:
            tail.stop()
        self._stop.set()

    def statuses(self) -> list[dict]:
        with self._lock:
            tails = list(self._tails.values())
        return sorted((t.status() for t in tails), key=lambda s: s["id"])

    def fetch_previous(self, gateway: k8s.KubeGateway, environment: str, pod: dict, container: str) -> int:
        """Descarga una vez los logs de la instancia anterior del contenedor
        (la que se cayó), útil para ver la causa de un CrashLoopBackOff."""
        response = gateway.open_log_stream(pod["namespace"], pod["name"], container,
                                           follow=False, previous=True)
        parser = self._new_parser(pod["workload"], container)
        records = []

        def sink(record):
            records.append(record)

        key = TailKey(environment, pod["namespace"], pod["name"], container)
        tail = PodTail(key, pod["workload"], gateway, self.settings, parser, sink)
        try:
            for line in k8s.iter_lines(response):
                tail._handle_line(line)
        finally:
            response.close()
        tail._emit(parser.flush())
        if not records and tail.kubelet_errors:
            raise k8s.KubeError("El clúster ya no conserva los logs de la instancia anterior "
                                f"({tail.kubelet_errors[0][:160]}).")
        for record in records:
            self.storage.enqueue(record)
        return len(records)

    def _tick_loop(self):
        while not self._stop.wait(0.1):
            with self._lock:
                tails = list(self._tails.values())
            for tail in tails:
                try:
                    tail.flush_if_idle()
                except Exception:
                    log.exception("Error vaciando el buffer multilínea")
