"""Pruebas del recolector y la API con un clúster simulado (sin Kubernetes real)."""

import threading
import time

import pytest
from fastapi.testclient import TestClient
from kubernetes.client.exceptions import ApiException

from webapp.collector import Collector, split_k8s_timestamp
from webapp.config import Environment, Settings
from webapp.server import create_app
from webapp.storage import LogFilter, Storage

POD = {
    "name": "orders-service-abc12-x1", "namespace": "demo", "workload": "orders-service",
    "workload_kind": "Deployment", "ready": "2/2", "status": "Running", "restarts": 0,
    "containers": [{"name": "app", "image": "x", "ready": True, "restarts": 0, "has_previous": True},
                   {"name": "istio-proxy", "image": "y", "ready": True, "restarts": 0, "has_previous": False}],
}


class FakeStream:
    def __init__(self, lines, block_after=False):
        self.lines = lines
        self.block_after = block_after
        self.closed = threading.Event()

    def stream(self, *_a, **_k):
        for line in self.lines:
            yield (line + "\n").encode()
        if self.block_after:
            self.closed.wait(5)

    def close(self):
        self.closed.set()


class FakeGateway:
    """Simula un pod que emite logs, se corta una vez y al reconectar reenvía la
    última línea (como hace since_seconds) para comprobar la deduplicación."""

    default_namespace = "demo"

    def __init__(self):
        self.env = Environment(name="fake")
        self.calls = []
        self.pod_exists = True

    def list_namespaces(self):
        return ["demo"]

    def list_pods(self, namespace):
        return [dict(POD)]

    def get_pod(self, namespace, name):
        return dict(POD) if (self.pod_exists and name == POD["name"]) else None

    def open_log_stream(self, namespace, pod, container, **kwargs):
        self.calls.append(kwargs)
        if kwargs.get("previous"):
            return FakeStream(["2026-01-10T09:00:00.000000000Z {\"level\":\"fatal\",\"msg\":\"murió\"}"])
        if len(self.calls) == 1:
            return FakeStream([
                "2026-01-10T10:00:00.000000000Z 2026-01-10 10:00:00.000 ERROR 1 --- [main] c.e.Svc : Falló",
                "2026-01-10T10:00:00.000000001Z java.lang.IllegalStateException: boom",
                "2026-01-10T10:00:00.000000002Z \tat c.e.Svc.run(Svc.java:1)",
                "2026-01-10T10:00:01.000000000Z 2026-01-10 10:00:01.000  INFO 1 --- [main] c.e.Svc : ok",
            ])
        if len(self.calls) == 2:
            raise ApiException(status=500, reason="boom")
        return FakeStream([
            "2026-01-10T10:00:01.000000000Z 2026-01-10 10:00:01.000  INFO 1 --- [main] c.e.Svc : ok",
            "2026-01-10T10:00:02.000000000Z 2026-01-10 10:00:02.000  WARN 1 --- [main] c.e.Svc : lento",
        ], block_after=True)


def wait_for(predicate, timeout=8):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def env(tmp_path):
    settings = Settings(db_path=str(tmp_path / "logs.db"), max_backoff_seconds=0.2, multiline_idle_seconds=0.2)
    storage = Storage(settings.db_path, start_threads=True)
    collector = Collector(settings, storage)
    yield settings, storage, collector
    collector.stop_all()
    storage.close()


def test_split_k8s_timestamp():
    assert split_k8s_timestamp("2026-01-10T10:00:00.5Z hola") == ("2026-01-10T10:00:00.500000000Z", "hola")
    assert split_k8s_timestamp("sin timestamp") == (None, "sin timestamp")


def test_collector_reconnects_dedups_and_groups(env):
    settings, storage, collector = env
    gateway = FakeGateway()
    started = collector.start(gateway, "fake", POD)
    assert started == ["fake/demo/orders-service-abc12-x1/app"]  # istio-proxy excluido por defecto

    assert wait_for(lambda: storage.count(LogFilter()) == 3)
    rows = storage.query(LogFilter(), order="asc")
    assert [r["level"] for r in rows] == ["ERROR", "INFO", "WARN"]
    assert rows[0]["message"].count("\n") == 2            # stack trace agrupado
    assert rows[0]["format"] == "spring_boot"
    assert gateway.calls[0]["tail_lines"] == settings.tail_lines
    assert gateway.calls[-1]["since_seconds"] is not None  # reanuda desde el último ts
    status = collector.statuses()[0]
    assert status["state"] == "following" and status["format"] == "spring_boot"


def test_collector_marks_deleted_pod_as_ended(env):
    _, storage, collector = env
    gateway = FakeGateway()

    def gone(*_a, **_k):
        raise ApiException(status=404, reason="Not Found")

    gateway.open_log_stream = gone
    collector.start(gateway, "fake", POD, ["app"])
    assert wait_for(lambda: collector.statuses()[0]["state"] == "ended")


def test_kubelet_error_text_is_not_stored_as_log(env):
    # Visto en minikube: si el contenedor anterior ya fue eliminado, kubelet
    # responde HTTP 200 con un texto de error en el cuerpo.
    _, storage, collector = env

    class GoneGateway(FakeGateway):
        def open_log_stream(self, namespace, pod, container, **kwargs):
            return FakeStream(["unable to retrieve container logs for containerd://a165b3a2"])

    from webapp.k8s import KubeError
    with pytest.raises(KubeError, match="ya no conserva"):
        collector.fetch_previous(GoneGateway(), "fake", POD, "app")
    time.sleep(0.6)
    assert storage.count(LogFilter()) == 0


def test_follows_replacement_pod_after_rollout(env):
    _, _, collector = env
    new_pod = dict(POD, name="orders-service-def34-y2", created="2026-01-10T11:00:00+00:00")
    other_workload = dict(POD, name="payments-1", workload="payments", created="2026-01-10T11:00:00+00:00")

    class RolloutGateway(FakeGateway):
        def get_pod(self, namespace, name):
            return new_pod if name == new_pod["name"] else None

        def list_pods(self, namespace):
            return [other_workload, new_pod]

        def open_log_stream(self, namespace, pod, container, **kwargs):
            if pod == POD["name"]:
                raise ApiException(status=404, reason="Not Found")
            return FakeStream(['2026-01-10T11:00:00.000000000Z {"level":"info","msg":"nuevo"}'], block_after=True)

    collector.start(RolloutGateway(), "fake", dict(POD, created="2026-01-10T10:00:00+00:00"), ["app"])
    assert wait_for(lambda: any(s["pod"] == new_pod["name"] and s["state"] == "following"
                                for s in collector.statuses()))
    statuses = {s["pod"]: s for s in collector.statuses()}
    assert statuses[POD["name"]]["state"] == "ended"
    assert "reemplazo" in statuses[POD["name"]]["message"]
    assert "payments-1" not in statuses  # no sigue pods de otros workloads


def test_api_flow(env, monkeypatch):
    settings, storage, collector = env
    gateway = FakeGateway()
    monkeypatch.setattr("webapp.server.k8s.KubeGateway", lambda env_: gateway)
    monkeypatch.setattr("webapp.server.k8s.list_contexts", lambda *a, **k: (["fake"], "fake"))
    app = create_app(settings, storage, collector)
    client = TestClient(app)

    assert client.get("/api/pods", params={"namespace": "demo"}).status_code == 409  # sin conectar
    envs = client.get("/api/environments").json()
    assert envs["default"] == "fake"
    conn = client.post("/api/connect", json={"environment": "fake"}).json()
    assert conn["namespaces"] == ["demo"] and conn["default_namespace"] == "demo"

    pods = client.get("/api/pods", params={"namespace": "demo"}).json()
    assert pods["workloads"][0]["workload"] == "orders-service"

    bad = client.post("/api/tail/start", json={"namespace": "demo", "targets": [{"pod": "--kubeconfig=x"}]})
    assert bad.status_code == 400
    missing = client.post("/api/tail/start", json={"namespace": "demo", "targets": [{"pod": "otro"}]})
    assert missing.status_code == 404

    ok = client.post("/api/tail/start", json={"namespace": "demo", "targets": [{"pod": POD["name"], "containers": ["app"]}]})
    assert ok.status_code == 200 and ok.json()["started"]
    assert wait_for(lambda: storage.count(LogFilter()) == 3)

    logs = client.get("/api/logs", params={"levels": "ERROR"}).json()
    assert logs["total"] == 1
    assert client.get("/api/logs", params={"q": "([", "regex": "1"}).status_code == 400

    csv = client.get("/api/logs/export", params={"fmt": "csv"})
    assert csv.status_code == 200 and "spring_boot" in csv.text

    prev = client.post("/api/tail/previous", json={"namespace": "demo", "pod": POD["name"], "container": "app"})
    assert prev.json()["lines"] == 1
    assert wait_for(lambda: storage.count(LogFilter(levels=["FATAL"])) == 1)

    meta = client.get("/api/meta").json()
    assert "spring_boot" in meta["formats"] and meta["workloads"] == ["orders-service"]

    status = client.get("/api/tail/status").json()["tails"]
    stopped = client.post("/api/tail/stop", json={"ids": [status[0]["id"]]}).json()
    assert stopped["stopped"] == [status[0]["id"]]


def test_index_busts_browser_cache(env):
    settings, storage, collector = env
    res = TestClient(create_app(settings, storage, collector)).get("/")
    assert res.status_code == 200
    assert "__VERSION__" not in res.text and "/static/app.js?v=" in res.text
    assert res.headers["cache-control"] == "no-cache"


def test_websocket_origin_and_live_filter(env, monkeypatch):
    settings, storage, collector = env
    app = create_app(settings, storage, collector)
    client = TestClient(app)

    with pytest.raises(Exception):
        with client.websocket_connect("/ws/logs", headers={"origin": "https://sitio-malicioso.example"}) as ws:
            ws.receive_json()

    with client.websocket_connect("/ws/logs?levels=ERROR",
                                  headers={"origin": f"http://127.0.0.1:{settings.port}"}) as ws:
        assert wait_for(lambda: collector.subscriber_count() == 1)
        base = dict(environment="e", namespace="n", workload="w", pod="p", container="c", format="plain",
                    logger=None, thread=None, trace_id=None)
        storage.enqueue({**base, "ts": "2026-01-10T10:00:00.000000000Z", "level": "INFO", "message": "x", "raw": "x"})
        storage.enqueue({**base, "ts": "2026-01-10T10:00:01.000000000Z", "level": "ERROR", "message": "y", "raw": "y"})
        item = ws.receive_json()
        assert item["message"] == "y"
    assert wait_for(lambda: collector.subscriber_count() == 0)
