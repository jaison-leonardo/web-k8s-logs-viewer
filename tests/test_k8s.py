from webapp import k8s


def _pod(name, owner_kind=None, owner_name=None, labels=None, statuses=None, containers=("app",), deleting=False):
    metadata = {"name": name, "namespace": "ns", "labels": labels or {}}
    if owner_kind:
        metadata["ownerReferences"] = [{"kind": owner_kind, "name": owner_name, "controller": True}]
    if deleting:
        metadata["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    return {
        "metadata": metadata,
        "spec": {"containers": [{"name": c, "image": f"img/{c}"} for c in containers]},
        "status": {"phase": "Running", "containerStatuses": statuses or []},
    }


def test_deployment_resolved_from_owner_and_pod_template_hash():
    pod = _pod("orders-service-7d4f9c8b6-x2kq9", "ReplicaSet", "orders-service-7d4f9c8b6",
               {"pod-template-hash": "7d4f9c8b6"})
    assert k8s.resolve_workload(pod) == ("orders-service", "Deployment")


def test_statefulset_is_not_mangled():
    # Con la regex del MVP este pod quedaba agrupado como "adfsender".
    pod = _pod("adfsender-deployment-0", "StatefulSet", "adfsender-deployment")
    assert k8s.resolve_workload(pod) == ("adfsender-deployment", "StatefulSet")


def test_cronjob_and_bare_pod():
    assert k8s.resolve_workload(_pod("backup-28934520-abcde", "Job", "backup-28934520")) == ("backup", "CronJob")
    assert k8s.resolve_workload(_pod("debug-shell")) == ("debug-shell", "Pod")


def test_status_ready_restarts_and_containers():
    statuses = [
        {"name": "app", "ready": False, "restartCount": 4,
         "state": {"waiting": {"reason": "CrashLoopBackOff"}}, "lastState": {"terminated": {"exitCode": 1}}},
        {"name": "istio-proxy", "ready": True, "restartCount": 0, "state": {"running": {}}},
    ]
    pod = _pod("p", statuses=statuses, containers=("app", "istio-proxy"))
    summary = k8s.pod_summary(pod)
    assert summary["status"] == "CrashLoopBackOff"
    assert summary["ready"] == "1/2"
    assert summary["restarts"] == 4
    assert summary["containers"][0]["has_previous"] is True
    assert k8s.pod_status(_pod("q", deleting=True)) == "Terminating"


def test_group_by_workload():
    pods = [k8s.pod_summary(_pod("b-1", "StatefulSet", "b")), k8s.pod_summary(_pod("b-0", "StatefulSet", "b")),
            k8s.pod_summary(_pod("a"))]
    groups = k8s.group_by_workload(pods)
    assert [g["workload"] for g in groups] == ["a", "b"]
    assert [p["name"] for p in groups[1]["pods"]] == ["b-0", "b-1"]


def test_name_validation_blocks_flag_injection():
    assert k8s.is_valid_name("orders-service-7d4f9c8b6-x2kq9")
    assert not k8s.is_valid_name("--kubeconfig=/tmp/x")
    assert not k8s.is_valid_name("Pod_Mayus")
    assert not k8s.is_valid_name("")


class FakeResponse:
    def __init__(self, chunks):
        self.chunks = chunks

    def stream(self, *_args, **_kwargs):
        yield from self.chunks


def test_iter_lines_handles_split_chunks_and_utf8():
    resp = FakeResponse([b"hola\nmun", "do ñ\r\n".encode(), b"fin"])
    assert list(k8s.iter_lines(resp)) == ["hola", "mundo ñ", "fin"]
