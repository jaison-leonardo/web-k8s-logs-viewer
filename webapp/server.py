"""API + dashboard web para consultar logs de microservicios en Kubernetes."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import queue
import re
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from webapp import k8s
from webapp.collector import Collector
from webapp.config import Environment, Settings
from webapp.logformats import LEVELS
from webapp.storage import LogFilter, Storage

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


class Connection:
    """Entorno activo en la UI (los tails de otros entornos siguen corriendo)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.environment: Environment | None = None
        self.gateway: k8s.KubeGateway | None = None
        self.gateways: dict[str, k8s.KubeGateway] = {}
        self.error: str | None = None


# ------------------------------------------------------------------ modelos
class ConnectRequest(BaseModel):
    environment: str


class TailTarget(BaseModel):
    pod: str
    containers: list[str] | None = None


class TailStartRequest(BaseModel):
    namespace: str
    targets: list[TailTarget] = Field(default_factory=list)


class TailStopRequest(BaseModel):
    ids: list[str]


class PreviousRequest(BaseModel):
    namespace: str
    pod: str
    container: str


def _split(value: str | None) -> list[str]:
    return [v for v in (value or "").split(",") if v]


def build_filter(params) -> LogFilter:
    flt = LogFilter(
        environments=_split(params.get("environments")),
        namespaces=_split(params.get("namespaces")),
        workloads=_split(params.get("workloads")),
        pods=_split(params.get("pods")),
        containers=_split(params.get("containers")),
        levels=_split(params.get("levels")),
        formats=_split(params.get("formats")),
        text=params.get("q") or None,
        regex=params.get("regex") in ("1", "true", "on"),
        exclude=params.get("exclude") or None,
        trace_id=params.get("trace") or None,
        logger=params.get("logger") or None,
        since=params.get("since") or None,
        until=params.get("until") or None,
    )
    try:
        flt.validate()
    except re.error as exc:
        raise HTTPException(400, f"Expresión regular inválida: {exc}") from exc
    return flt


def environments_for(settings: Settings) -> list[Environment]:
    """Entornos declarados en config.yaml + contextos del kubeconfig no declarados."""
    envs = list(settings.environments)
    declared_contexts = {e.context for e in envs if not e.in_cluster and not e.kubeconfig}
    contexts, _ = k8s.list_contexts()
    for ctx in contexts:
        if ctx not in declared_contexts and ctx not in {e.name for e in envs}:
            envs.append(Environment(name=ctx, context=ctx))
    if not envs:
        envs.append(Environment(name="kubeconfig (contexto actual)"))
    return envs


def create_app(settings: Settings, storage: Storage | None = None, collector: Collector | None = None) -> FastAPI:
    storage = storage or Storage(settings.db_path, settings.retention_days, settings.max_rows)
    collector = collector or Collector(settings, storage)
    conn = Connection()

    @asynccontextmanager
    async def lifespan(_app):
        yield
        collector.stop_all()
        storage.close()

    app = FastAPI(title="Pods Log Viewer", lifespan=lifespan)
    app.state.settings = settings
    app.state.storage = storage
    app.state.collector = collector
    app.state.connection = conn
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    def require_gateway() -> tuple[str, k8s.KubeGateway]:
        with conn.lock:
            if conn.gateway is None or conn.environment is None:
                raise HTTPException(409, "No hay un entorno conectado. Selecciona uno en la parte superior.")
            return conn.environment.name, conn.gateway

    def kube_call(fn, *args):
        try:
            return fn(*args)
        except k8s.KubeError as exc:
            raise HTTPException(502, str(exc)) from exc

    def find_pod(gateway, namespace, name):
        if not k8s.is_valid_name(namespace) or not k8s.is_valid_name(name):
            raise HTTPException(400, f"Nombre inválido: {namespace}/{name}")
        pod = kube_call(gateway.get_pod, namespace, name)
        if pod is None:
            raise HTTPException(404, f"El pod {namespace}/{name} no existe.")
        return pod

    # ------------------------------------------------------------- páginas
    def static_version():
        # Cambia cada vez que se modifica un archivo del frontend: obliga al
        # navegador a descargar la versión nueva tras una actualización.
        return str(int(max(os.path.getmtime(os.path.join(STATIC_DIR, f)) for f in os.listdir(STATIC_DIR))))

    @app.get("/", response_class=HTMLResponse)
    def index():
        with open(os.path.join(STATIC_DIR, "index.html"), encoding="utf-8") as fh:
            html = fh.read().replace("__VERSION__", static_version())
        return HTMLResponse(html, headers={"Cache-Control": "no-cache"})

    # ------------------------------------------------------------- entornos
    @app.get("/api/environments")
    def api_environments():
        envs = environments_for(settings)
        _, current_ctx = k8s.list_contexts()
        default = next((e.name for e in envs if e.context == current_ctx), envs[0].name)
        return {
            "environments": [{"name": e.name, "context": e.context, "in_cluster": e.in_cluster} for e in envs],
            "default": default,
            "active": conn.environment.name if conn.environment else None,
            "error": conn.error,
        }

    @app.post("/api/connect")
    def api_connect(req: ConnectRequest):
        env = next((e for e in environments_for(settings) if e.name == req.environment), None)
        if env is None:
            raise HTTPException(404, f"Entorno desconocido: {req.environment}")
        try:
            gateway = conn.gateways.get(env.name) or k8s.KubeGateway(env)
            namespaces = gateway.list_namespaces()
        except k8s.KubeError as exc:
            with conn.lock:
                conn.error = str(exc)
            raise HTTPException(502, str(exc)) from exc
        with conn.lock:
            conn.environment, conn.gateway, conn.error = env, gateway, None
            conn.gateways[env.name] = gateway
        default_ns = gateway.default_namespace if gateway.default_namespace in namespaces else None
        return {"environment": env.name, "namespaces": namespaces, "default_namespace": default_ns}

    @app.get("/api/pods")
    def api_pods(namespace: str = Query(...)):
        environment, gateway = require_gateway()
        if not k8s.is_valid_name(namespace):
            raise HTTPException(400, "Namespace inválido.")
        pods = kube_call(gateway.list_pods, namespace)
        active = {(s["namespace"], s["pod"], s["container"]): s
                  for s in collector.statuses() if s["environment"] == environment and s["state"] != "stopped"}
        for pod in pods:
            for container in pod["containers"]:
                status = active.get((namespace, pod["name"], container["name"]))
                container["tail_state"] = status["state"] if status else None
            pod["default_containers"] = collector.default_containers(pod)
        return {"environment": environment, "namespace": namespace, "workloads": k8s.group_by_workload(pods)}

    # ---------------------------------------------------------------- tails
    @app.post("/api/tail/start")
    def api_tail_start(req: TailStartRequest):
        environment, gateway = require_gateway()
        started = []
        for target in req.targets:
            pod = find_pod(gateway, req.namespace, target.pod)
            started.extend(collector.start(gateway, environment, pod, target.containers))
        return {"started": started, "tails": collector.statuses()}

    @app.post("/api/tail/stop")
    def api_tail_stop(req: TailStopRequest):
        return {"stopped": collector.stop(req.ids), "tails": collector.statuses()}

    @app.get("/api/tail/status")
    def api_tail_status():
        return {"tails": collector.statuses(), "dropped": storage.dropped}

    @app.post("/api/tail/previous")
    def api_tail_previous(req: PreviousRequest):
        environment, gateway = require_gateway()
        pod = find_pod(gateway, req.namespace, req.pod)
        if req.container not in {c["name"] for c in pod["containers"]}:
            raise HTTPException(400, "Contenedor inválido.")
        try:
            count = collector.fetch_previous(gateway, environment, pod, req.container)
        except k8s.KubeError as exc:
            raise HTTPException(404, str(exc)) from exc
        except Exception as exc:
            raise HTTPException(502, k8s.describe_api_error(exc)) from exc
        return {"lines": count}

    # ---------------------------------------------------------------- logs
    @app.get("/api/meta")
    def api_meta():
        return {
            "levels": LEVELS,
            "formats": collector.registry.names(),
            "workloads": storage.distinct("workload"),
            "pods": storage.distinct("pod"),
            "containers": storage.distinct("container"),
            "namespaces": storage.distinct("namespace"),
            "environments": storage.distinct("environment"),
        }

    @app.get("/api/formats")
    def api_formats():
        return {"formats": collector.registry.names()}

    @app.get("/api/logs")
    def api_logs(request: Request, limit: int = 500, offset: int = 0, order: str = "desc"):
        flt = build_filter(request.query_params)
        limit = max(1, min(limit, 5000))
        rows = storage.query(flt, limit=limit, offset=max(0, offset), order=order)
        return {"rows": rows, "total": storage.count(flt), "limit": limit, "offset": offset}

    @app.get("/api/logs/export")
    def api_logs_export(request: Request, fmt: str = "txt"):
        flt = build_filter(request.query_params)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        fields = ["ts", "environment", "namespace", "workload", "pod", "container", "level",
                  "format", "logger", "thread", "trace_id", "message"]

        def gen_txt():
            for row in storage.iter_rows(flt):
                yield f"{row['ts'] or ''} [{row['pod']}/{row['container']}] {row['raw']}\n"

        def gen_jsonl():
            for row in storage.iter_rows(flt):
                yield json.dumps({k: row[k] for k in fields}, ensure_ascii=False) + "\n"

        def gen_csv():
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(fields)
            yield "﻿" + buf.getvalue()
            for row in storage.iter_rows(flt):
                buf.seek(0)
                buf.truncate()
                writer.writerow([row[k] for k in fields])
                yield buf.getvalue()

        generators = {"txt": (gen_txt, "text/plain"), "jsonl": (gen_jsonl, "application/x-ndjson"),
                      "csv": (gen_csv, "text/csv")}
        if fmt not in generators:
            raise HTTPException(400, "Formato de exportación no soportado (txt, jsonl, csv).")
        gen, media = generators[fmt]
        headers = {"Content-Disposition": f'attachment; filename="logs_{stamp}.{fmt}"'}
        return StreamingResponse(gen(), media_type=f"{media}; charset=utf-8", headers=headers)

    # ------------------------------------------------------------ websocket
    def origin_allowed(origin: str | None) -> bool:
        if not origin:
            return True  # clientes no navegador (no envían Origin)
        if origin in settings.allowed_origins:
            return True
        parsed = urlparse(origin)
        return parsed.hostname in ("127.0.0.1", "localhost", "::1") and parsed.port == settings.port

    @app.websocket("/ws/logs")
    async def ws_logs(websocket: WebSocket):
        if not origin_allowed(websocket.headers.get("origin")):
            await websocket.close(code=1008)
            return
        try:
            flt = build_filter(websocket.query_params)
        except HTTPException:
            await websocket.close(code=1003)
            return
        await websocket.accept()
        q = collector.subscribe()
        closed = asyncio.Event()

        async def watch_disconnect():
            try:
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        break
            except Exception:
                pass
            finally:
                closed.set()

        watcher = asyncio.create_task(watch_disconnect())
        try:
            while not closed.is_set():
                try:
                    item = await asyncio.to_thread(q.get, True, 0.5)
                except queue.Empty:
                    continue
                if flt.matches(item):
                    await websocket.send_json(item)
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            watcher.cancel()
            collector.unsubscribe(q)

    return app
