"""Validación reproducible de la herramienta contra el entorno demo.

Requiere: la herramienta corriendo (main.py), un clúster con
deploy/demo/log-generators.yaml aplicado, y kubectl en el PATH. Para la prueba
de caída de la API y el reinicio de contenedor se usa `docker exec` sobre el nodo
(kind y minikube con driver docker ejecutan el nodo como contenedor).

    python tools/validate_demo.py --base http://127.0.0.1:8765 --environment demo-kind \
        --context kind-pods-demo --node docker:pods-demo-control-plane --out resultados.json

Mide: exactitud de formato/nivel, completitud (pérdidas/duplicados) en régimen
normal, tras reiniciar un contenedor, tras una caída de la API de Kubernetes y al
eliminar un pod; logs de la instancia anterior; trazabilidad; latencia en vivo y
rendimiento bajo carga.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import subprocess
import threading
import time
from collections import Counter
from datetime import datetime, timezone

import httpx
from websockets.sync.client import connect as ws_connect

NS = "demo-logs"

# Claves únicas por línea para medir completitud (por workload).
KEY_PATTERNS = {
    "orders-service": re.compile(r"Pedido recibido id=(\d+)"),
    "payments-api": re.compile(r'"traceId":"(demo-\d+)"'),
    "inventory-worker": re.compile(r"Reserva procesada trace_id=(demo-\d+)"),
    "notifications": re.compile(r"Correo enviado al cliente (\d+)"),
    "api-gateway": re.compile(r"^(10\.0\.0\.\d+ - - \[[^\]]+\] .*)$"),
    "loadgen": re.compile(r"seq=(\d+)\b"),
}

EXPECTED_FORMAT = {
    "orders-service": "spring_boot", "payments-api": "json", "inventory-worker": "python",
    "notifications": "dotnet", "api-gateway": "http_access", "flaky-service": "zap_console",
    "loadgen": "logfmt",
}


def expected_level(workload: str, message: str) -> str | None:
    first = message.split("\n", 1)[0]
    rules = {
        "orders-service": [("Pedido recibido", "INFO"), ("No fue posible reservar", "ERROR"), ("Reintentando", "WARN")],
        "payments-api": [("Pago autorizado", "INFO"), ("Pago rechazado", "ERROR")],
        "inventory-worker": [("Reserva procesada", "INFO"), ("Fallo al actualizar", "ERROR")],
        "notifications": [("Correo enviado", "INFO"), ("No se pudo conectar", "ERROR")],
        "flaky-service": [("procesando lote", "INFO"), ("configuración inválida", "FATAL")],
        "loadgen": [("carga", "INFO")],
    }
    if workload == "api-gateway":
        match = re.search(r'" (\d{3}) ', first)
        if not match:
            return None
        code = int(match.group(1))
        return "ERROR" if code >= 500 else "WARN" if code >= 400 else "INFO"
    for needle, level in rules.get(workload, []):
        if needle in first:
            return level
    return None


EXPECTED_MULTILINE = {  # (workload, texto) -> mínimo de líneas del registro
    ("orders-service", "No fue posible reservar"): 5,
    ("inventory-worker", "Fallo al actualizar"): 4,
    ("payments-api", "Pago rechazado"): 2,
}


class Validator:
    def __init__(self, args):
        self.args = args
        self.http = httpx.Client(base_url=args.base, timeout=60)
        self.results: dict = {"inicio": datetime.now(timezone.utc).isoformat()}

    # ------------------------------------------------------------ utilidades
    def log(self, *msg):
        print(time.strftime("%H:%M:%S"), *msg, flush=True)

    def kubectl(self, *args, check=True):
        cmd = ["kubectl", "--context", self.args.context, "-n", NS, *args]
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
        if check and proc.returncode != 0:
            raise RuntimeError(f"{' '.join(cmd)}: {proc.stderr}")
        return proc.stdout

    def node_exec(self, command: str):
        # kind y minikube (driver docker) ejecutan el nodo como contenedor Docker.
        _, name = self.args.node.split(":", 1)
        cmd = ["docker", "exec", name, "sh", "-c", command]
        return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")

    def clock_offset(self) -> float:
        """Segundos que el reloj del nodo va adelantado respecto al local."""
        before = time.time()
        out = self.node_exec("date +%s.%N").stdout.strip()
        after = time.time()
        try:
            return float(out) - (before + after) / 2
        except ValueError:
            return 0.0

    def pods(self):
        data = self.http.get("/api/pods", params={"namespace": NS}).raise_for_status().json()
        return [p for w in data["workloads"] for p in w["pods"]]

    def tails(self):
        return self.http.get("/api/tail/status").raise_for_status().json()

    def fetch_rows(self, **params):
        rows, offset = [], 0
        while True:
            page = self.http.get("/api/logs", params={**params, "namespaces": NS, "limit": 5000,
                                                      "offset": offset, "order": "asc"}).raise_for_status().json()
            rows.extend(page["rows"])
            offset += len(page["rows"])
            if not page["rows"] or offset >= page["total"]:
                return rows

    def start_all(self, only: set[str] | None = None, exclude: set[str] = frozenset()):
        targets = [{"pod": p["name"]} for p in self.pods()
                   if (only is None or p["workload"] in only) and p["workload"] not in exclude]
        return self.http.post("/api/tail/start", json={"namespace": NS, "targets": targets}).raise_for_status().json()

    # ------------------------------------------------------------ medidas
    def completeness(self, workloads, settle_lines=3, include_previous=()):
        """Compara las claves que muestra kubectl con las guardadas por la herramienta."""
        rows = self.fetch_rows(workloads=",".join(workloads))
        report = {}
        for pod in self.pods():
            wl = pod["workload"]
            if wl not in workloads or wl not in KEY_PATTERNS:
                continue
            for container in pod["default_containers"]:
                pattern = KEY_PATTERNS[wl]
                text = ""
                if (pod["name"], container) in include_previous or wl in include_previous:
                    text += self.kubectl("logs", pod["name"], "-c", container, "--previous", check=False)
                text += self.kubectl("logs", pod["name"], "-c", container)
                expected = [m.group(1) for line in text.splitlines() if (m := pattern.search(line))]
                stored = Counter(
                    m.group(1)
                    for r in rows if r["pod"] == pod["name"] and r["container"] == container
                    for line in r["raw"].split("\n") if (m := pattern.search(line))
                )
                first = next((i for i, k in enumerate(expected) if k in stored), None)
                if first is None:
                    report[f"{pod['name']}/{container}"] = {"esperadas": 0, "error": "sin datos"}
                    continue
                window = expected[first:len(expected) - settle_lines]
                missing = [k for k in window if k not in stored]
                dups = sum(c - 1 for c in stored.values() if c > 1)
                report[f"{pod['name']}/{container}"] = {
                    "esperadas": len(window), "perdidas": len(missing), "duplicadas": dups,
                    "ejemplo_perdidas": missing[:3],
                }
        total = sum(v.get("esperadas", 0) for v in report.values())
        lost = sum(v.get("perdidas", 0) for v in report.values())
        dup = sum(v.get("duplicadas", 0) for v in report.values())
        return {"lineas_verificadas": total, "perdidas": lost, "duplicadas": dup, "por_contenedor": report}

    def accuracy(self):
        rows = self.fetch_rows()
        total = ok_fmt = ok_lvl = 0
        stray = 0
        multiline_ok = multiline_total = 0
        errors = Counter()
        by_workload = {}
        for r in rows:
            wl = r["workload"]
            if wl not in EXPECTED_FORMAT or r["container"] == "istio-proxy":
                continue
            exp_lvl = expected_level(wl, r["message"])
            if exp_lvl is None:
                stray += 1  # fragmento suelto (multilínea mal agrupada) o línea desconocida
                errors[f"{wl}: fragmento '{r['message'][:40]}'"] += 1
                continue
            total += 1
            f_ok = r["format"] == EXPECTED_FORMAT[wl]
            l_ok = r["level"] == exp_lvl
            ok_fmt += f_ok
            ok_lvl += l_ok
            stats = by_workload.setdefault(wl, {"registros": 0, "formato_ok": 0, "nivel_ok": 0})
            stats["registros"] += 1
            stats["formato_ok"] += f_ok
            stats["nivel_ok"] += l_ok
            if not f_ok:
                errors[f"{wl}: formato {r['format']}"] += 1
            if not l_ok:
                errors[f"{wl}: nivel {r['level']} (esperado {exp_lvl})"] += 1
            for (w, needle), min_lines in EXPECTED_MULTILINE.items():
                if w == wl and needle in r["message"].split("\n", 1)[0]:
                    multiline_total += 1
                    multiline_ok += r["message"].count("\n") + 1 >= min_lines
        return {
            "registros_evaluados": total,
            "formato_correcto_pct": round(100 * ok_fmt / total, 2) if total else None,
            "nivel_correcto_pct": round(100 * ok_lvl / total, 2) if total else None,
            "fragmentos_sueltos": stray,
            "multilinea_agrupada": f"{multiline_ok}/{multiline_total}",
            "por_workload": by_workload,
            "errores": dict(errors.most_common(10)),
        }

    def measure_latency(self, seconds, params="", offset=0.0):
        url = self.args.base.replace("http", "ws", 1) + "/ws/logs" + (f"?{params}" if params else "")
        deltas, stop = [], threading.Event()

        def run():
            with ws_connect(url, open_timeout=10) as ws:
                end = time.time() + seconds
                while time.time() < end:
                    try:
                        msg = json.loads(ws.recv(timeout=1))
                    except TimeoutError:
                        continue
                    now = time.time()
                    ts = msg.get("ts")
                    if ts:
                        t = datetime.strptime(ts[:26], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()
                        deltas.append(now - (t - offset))
            stop.set()

        thread = threading.Thread(target=run)
        thread.start()
        thread.join(seconds + 30)
        if not deltas:
            return {"muestras": 0}
        deltas.sort()
        pct = lambda p: round(deltas[min(len(deltas) - 1, int(p * len(deltas)))], 3)
        return {"muestras": len(deltas), "p50_s": pct(0.50), "p95_s": pct(0.95), "p99_s": pct(0.99),
                "max_s": round(deltas[-1], 3), "media_s": round(statistics.mean(deltas), 3)}

    def wait_api(self, timeout=240):
        end = time.time() + timeout
        while time.time() < end:
            try:
                self.pods()
                return True
            except Exception:
                time.sleep(3)
        return False

    # ---------------------------------------------------------------- fases
    def run(self):
        a = self.args
        t0 = time.time()
        conn = self.http.post("/api/connect", json={"environment": a.environment}).raise_for_status().json()
        self.results["conexion"] = {"segundos": round(time.time() - t0, 3), "namespaces": conn["namespaces"]}
        offset = self.clock_offset()
        self.results["desfase_reloj_nodo_s"] = round(offset, 3)
        self.log("Conectado", conn, "desfase reloj", offset)

        started = self.start_all(exclude={"loadgen"})
        self.results["streams_iniciados"] = len(started["started"])
        self.log("Streams:", len(started["started"]), "; estabilizando", a.warmup, "s")
        time.sleep(a.warmup)

        base_workloads = ["orders-service", "payments-api", "inventory-worker", "notifications", "api-gateway"]
        self.results["completitud_normal"] = self.completeness(base_workloads)
        self.log("Completitud normal:", {k: v for k, v in self.results["completitud_normal"].items() if k != "por_contenedor"})

        # Latencia en régimen normal
        self.results["latencia_normal"] = self.measure_latency(a.latency_seconds, offset=offset)
        self.log("Latencia normal:", self.results["latencia_normal"])

        # Reinicio de contenedor: se detiene desde el runtime (como un OOMKill o un
        # crash) y kubelet lo vuelve a crear. (`kill 1` dentro del contenedor no
        # sirve: el PID 1 ignora señales sin manejador.)
        gw = next(p for p in self.pods() if p["workload"] == "api-gateway")
        cid = self.node_exec(
            f"crictl ps -q --name '^nginx$' --label io.kubernetes.pod.name={gw['name']}").stdout.strip()
        self.node_exec(f"crictl stop {cid}")
        self.log("Contenedor nginx detenido", cid[:12], "; esperando 40 s")
        time.sleep(40)
        state = next((t for t in self.tails()["tails"] if t["pod"] == gw["name"] and t["container"] == "nginx"), {})
        comp = self.completeness(["api-gateway"], include_previous=("api-gateway",))
        restarts = next(p for p in self.pods() if p["name"] == gw["name"])["containers"][0]["restarts"]
        self.results["reinicio_contenedor"] = {"reinicios": restarts, "estado_stream": state.get("state"),
                                               "perdidas": comp["perdidas"], "duplicadas": comp["duplicadas"],
                                               "lineas_verificadas": comp["lineas_verificadas"]}
        self.log("Reinicio contenedor:", self.results["reinicio_contenedor"])

        # Caída de la API de Kubernetes (los pods siguen escribiendo logs)
        if not a.skip_outage:
            self.node_exec("mv /etc/kubernetes/manifests/kube-apiserver.yaml /tmp/kube-apiserver.yaml")
            outage_start = time.time()
            self.log("API detenida; esperando", a.outage_seconds, "s")
            time.sleep(a.outage_seconds)
            during = Counter(t["state"] for t in self.tails()["tails"])
            self.node_exec("mv /tmp/kube-apiserver.yaml /etc/kubernetes/manifests/kube-apiserver.yaml")
            back = self.wait_api()
            api_back_after = time.time() - outage_start
            self.log("API de vuelta:", back, round(api_back_after, 1), "s; esperando reconexión 45 s")
            time.sleep(45)
            after = Counter(t["state"] for t in self.tails()["tails"])
            comp = self.completeness(base_workloads)
            self.results["caida_api"] = {"duracion_caida_s": round(api_back_after, 1),
                                         "estados_durante": dict(during), "estados_despues": dict(after),
                                         "perdidas": comp["perdidas"], "duplicadas": comp["duplicadas"],
                                         "lineas_verificadas": comp["lineas_verificadas"]}
            self.log("Caída API:", self.results["caida_api"])

        # Pod eliminado (como en un rollout)
        before = {p["name"] for p in self.pods() if p["workload"] == "orders-service"}
        victim = sorted(before)[0]
        self.kubectl("delete", "pod", victim, "--wait=false")
        deleted_at = time.time()
        self.log("Pod eliminado", victim, "; esperando a que termine y se siga el reemplazo")
        st, new_tail = {}, None
        while time.time() - deleted_at < 120:
            tails = self.tails()["tails"]
            st = next((t for t in tails if t["pod"] == victim), {})
            new_tail = next((t for t in tails if t["workload"] == "orders-service" and t["pod"] not in before), None)
            if st.get("state") == "ended" and new_tail and new_tail["state"] == "following":
                break
            time.sleep(2)
        followed_after = round(time.time() - deleted_at, 1)
        time.sleep(15)
        comp = self.completeness(["orders-service"])
        replacement_comp = {k: v for k, v in comp["por_contenedor"].items() if new_tail and new_tail["pod"] in k}
        self.results["pod_eliminado"] = {
            "estado_stream_eliminado": st.get("state"), "mensaje": st.get("message"),
            "reemplazo_seguido": new_tail["pod"] if new_tail else None,
            "segundos_hasta_seguir_reemplazo": followed_after if new_tail else None,
            "completitud_reemplazo": replacement_comp,
        }
        self.log("Pod eliminado:", self.results["pod_eliminado"])

        # Instancia anterior (CrashLoopBackOff)
        flaky = next(p for p in self.pods() if p["workload"] == "flaky-service")
        prev = self.http.post("/api/tail/previous", json={"namespace": NS, "pod": flaky["name"], "container": "app"})
        time.sleep(2)
        fatal = self.fetch_rows(workloads="flaky-service", levels="FATAL")
        self.results["instancia_anterior"] = {"http": prev.status_code, "respuesta": prev.json(),
                                              "estado_pod": flaky["status"], "reinicios": flaky["restarts"],
                                              "registros_FATAL": len(fatal)}
        self.log("Instancia anterior:", self.results["instancia_anterior"])

        # Trazabilidad
        # (En el demo, un pod de reemplazo reinicia su contador de traceId, así que
        # se elige el trace reciente que aparece en más servicios.)
        err = self.fetch_rows(workloads="orders-service", levels="ERROR")
        trace_report = {}
        for row in err[-10:]:
            traced = self.fetch_rows(trace=row["trace_id"])
            candidate = {"trace_id": row["trace_id"], "registros": len(traced),
                         "servicios": sorted({r["workload"] for r in traced})}
            if len(candidate["servicios"]) > len(trace_report.get("servicios", [])):
                trace_report = candidate
        self.results["trazabilidad"] = trace_report
        self.log("Trazabilidad:", trace_report)

        # Exactitud de formato/nivel/multilínea sobre todo lo recolectado
        self.results["exactitud"] = self.accuracy()
        self.log("Exactitud:", {k: v for k, v in self.results["exactitud"].items() if k != "por_workload"})

        # Carga
        if not a.skip_load:
            self.kubectl("delete", "deploy", "loadgen", "--ignore-not-found", check=False)
            subprocess.run(["kubectl", "--context", a.context, "apply", "-f", "deploy/demo/loadgen.yaml"],
                           capture_output=True, check=True)
            self.kubectl("rollout", "status", "deploy/loadgen", "--timeout=120s")
            self.start_all(only={"loadgen"})  # el generador espera START_DELAY antes de emitir
            dropped_before = self.tails()["dropped"]
            total_expected = 30000
            lat_result = {}
            lat_thread = threading.Thread(target=lambda: lat_result.update(
                self.measure_latency(a.load_seconds, params="workloads=loadgen", offset=offset)))
            lat_thread.start()
            end = time.time() + 240
            seqs = Counter()
            rows = []
            while time.time() < end:
                time.sleep(10)
                count = self.http.get("/api/logs", params={"workloads": "loadgen", "limit": 1}).json()["total"]
                if count > total_expected:  # +1 por la línea final "fin de la carga"
                    break
            lat_thread.join()
            rows = [r for r in self.fetch_rows(workloads="loadgen") if "seq=" in r["raw"]]
            pattern = KEY_PATTERNS["loadgen"]
            seqs.update(int(m.group(1)) for r in rows if (m := pattern.search(r["raw"])))
            missing = [s for s in range(1, total_expected + 1) if s not in seqs]
            span = None
            if len(rows) > 1:
                first = datetime.strptime(rows[0]["ts"][:26], "%Y-%m-%dT%H:%M:%S.%f")
                last = datetime.strptime(rows[-1]["ts"][:26], "%Y-%m-%dT%H:%M:%S.%f")
                span = (last - first).total_seconds()
            self.results["carga"] = {
                "lineas_generadas": total_expected,
                "tasa_generacion_lineas_s": round(len(rows) / span, 1) if span else None,
                "registros_guardados": len(rows),
                "perdidas": len(missing), "duplicadas": sum(c - 1 for c in seqs.values() if c > 1),
                "latencia": lat_result,
                "descartadas_por_cola_llena": self.tails()["dropped"] - dropped_before,
            }
            self.log("Carga:", self.results["carga"])
            self.kubectl("delete", "deploy", "loadgen", check=False)

        self.results["fin"] = datetime.now(timezone.utc).isoformat()
        return self.results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default="http://127.0.0.1:8765")
    parser.add_argument("--environment", required=True, help="Nombre del entorno en la herramienta")
    parser.add_argument("--context", required=True, help="Contexto de kubectl del mismo clúster")
    parser.add_argument("--node", required=True,
                        help="docker:<contenedor del nodo> (kind: <cluster>-control-plane; minikube: <perfil>)")
    parser.add_argument("--warmup", type=int, default=60)
    parser.add_argument("--latency-seconds", type=int, default=30)
    parser.add_argument("--outage-seconds", type=int, default=40)
    parser.add_argument("--load-seconds", type=int, default=90)
    parser.add_argument("--skip-outage", action="store_true")
    parser.add_argument("--skip-load", action="store_true")
    parser.add_argument("--out", help="Archivo JSON de resultados")
    args = parser.parse_args()
    results = Validator(args).run()
    text = json.dumps(results, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)
    print(text)


if __name__ == "__main__":
    main()
