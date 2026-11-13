"""Acceso a Kubernetes independiente del proveedor.

Usa el cliente oficial de Kubernetes para Python con el kubeconfig estándar, por
lo que funciona igual con EKS, AKS, GKE, OpenShift, Rancher, kind, minikube o un
clúster on-premise: la autenticación propia de cada nube (aws/az/gcloud, OIDC,
certificados) la resuelve el plugin `exec` declarado en el kubeconfig. Dentro del
clúster puede usar el ServiceAccount del pod (in_cluster).

Las funciones puras de este módulo trabajan sobre el JSON del Pod (formato de la
API de Kubernetes) para poder probarlas sin clúster.
"""

from __future__ import annotations

import re

from kubernetes import client, config as kconfig
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException

from webapp.config import Environment

# DNS-1123 subdomain: lo que Kubernetes acepta como nombre de pod/namespace.
_NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$")
LIST_TIMEOUT = 20
STREAM_TIMEOUT = (10, 120)  # (conexión, lectura): tras 120 s sin datos se reconecta


class KubeError(RuntimeError):
    pass


def is_valid_name(name: str) -> bool:
    return bool(name) and bool(_NAME_RE.match(name))


def describe_api_error(exc: Exception) -> str:
    if isinstance(exc, ApiException):
        if exc.status in (401,):
            return "Sesión no autorizada o expirada: renueva las credenciales del proveedor (kubeconfig)."
        if exc.status == 403:
            return "Permisos insuficientes (RBAC) para esta operación."
        if exc.status == 404:
            return "Recurso no encontrado (el pod pudo haber sido eliminado)."
        return f"Error de la API de Kubernetes (HTTP {exc.status}): {exc.reason}"
    return f"No fue posible comunicarse con el clúster: {exc}"


def list_contexts(kubeconfig: str | None = None):
    """Contextos disponibles en el kubeconfig: ([nombres], contexto_actual)."""
    try:
        contexts, active = kconfig.list_kube_config_contexts(config_file=kubeconfig)
    except (ConfigException, OSError, TypeError):
        return [], None
    names = sorted(c["name"] for c in contexts or [])
    return names, (active or {}).get("name")


def _context_namespace(env: Environment) -> str | None:
    try:
        contexts, active = kconfig.list_kube_config_contexts(config_file=env.kubeconfig)
    except (ConfigException, OSError, TypeError):
        return None
    wanted = env.context or (active or {}).get("name")
    for ctx in contexts or []:
        if ctx["name"] == wanted:
            return (ctx.get("context") or {}).get("namespace")
    return None


class KubeGateway:
    """Conexión a un clúster (un entorno = un contexto de kubeconfig)."""

    def __init__(self, env: Environment):
        self.env = env
        try:
            if env.in_cluster:
                configuration = client.Configuration()
                kconfig.load_incluster_config(client_configuration=configuration)
                api_client = client.ApiClient(configuration)
                self.default_namespace = _read_incluster_namespace()
            else:
                api_client = kconfig.new_client_from_config(
                    config_file=env.kubeconfig, context=env.context, persist_config=False,
                )
                self.default_namespace = _context_namespace(env)
        except (ConfigException, OSError, TypeError) as exc:
            raise KubeError(f"No se pudo cargar la configuración de Kubernetes: {exc}") from exc
        self._api_client = api_client
        self.api = client.CoreV1Api(api_client)

    def _to_dict(self, obj):
        return self._api_client.sanitize_for_serialization(obj)

    def list_namespaces(self) -> list[str]:
        if self.env.namespaces:
            return list(self.env.namespaces)
        try:
            items = self.api.list_namespace(_request_timeout=LIST_TIMEOUT).items
            return sorted(ns.metadata.name for ns in items)
        except ApiException as exc:
            if exc.status == 403 and self.default_namespace:
                # Sin permiso para listar namespaces: usar el del contexto.
                return [self.default_namespace]
            raise KubeError(describe_api_error(exc)) from exc
        except Exception as exc:  # red, DNS, certificados, plugin exec...
            raise KubeError(describe_api_error(exc)) from exc

    def list_pods(self, namespace: str) -> list[dict]:
        try:
            items = self.api.list_namespaced_pod(namespace, _request_timeout=LIST_TIMEOUT).items
        except Exception as exc:
            raise KubeError(describe_api_error(exc)) from exc
        return [pod_summary(self._to_dict(p)) for p in items]

    def get_pod(self, namespace: str, name: str) -> dict | None:
        try:
            pod = self.api.read_namespaced_pod(name, namespace, _request_timeout=LIST_TIMEOUT)
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise KubeError(describe_api_error(exc)) from exc
        except Exception as exc:
            raise KubeError(describe_api_error(exc)) from exc
        return pod_summary(self._to_dict(pod))

    def open_log_stream(self, namespace, pod, container, *, follow=True, tail_lines=None,
                        since_seconds=None, previous=False):
        """Devuelve la respuesta HTTP en crudo (urllib3) para leer línea a línea.
        Puede lanzar ApiException; el llamador decide si reintenta."""
        kwargs = dict(container=container, follow=follow, timestamps=True, _preload_content=False,
                      _request_timeout=STREAM_TIMEOUT if follow else LIST_TIMEOUT)
        if tail_lines is not None:
            kwargs["tail_lines"] = int(tail_lines)
        if since_seconds is not None:
            kwargs["since_seconds"] = max(1, int(since_seconds))
        if previous:
            kwargs["previous"] = True
        return self.api.read_namespaced_pod_log(pod, namespace, **kwargs)


def _read_incluster_namespace():
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", encoding="utf-8") as fh:
            return fh.read().strip() or None
    except OSError:
        return None


def iter_lines(response, chunk_size=8192):
    """Itera las líneas (str, sin salto final) de una respuesta en streaming."""
    buffer = b""
    for chunk in response.stream(chunk_size, decode_content=True):
        if not chunk:
            continue
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line.rstrip(b"\r").decode("utf-8", errors="replace")
    if buffer:
        yield buffer.rstrip(b"\r").decode("utf-8", errors="replace")


# --------------------------------------------------------------------------
# Funciones puras sobre el JSON del Pod
# --------------------------------------------------------------------------

def resolve_workload(pod: dict) -> tuple[str, str]:
    """(nombre, tipo) del workload dueño del pod usando ownerReferences.

    ReplicaSet -> Deployment se resuelve quitando el label pod-template-hash del
    nombre del ReplicaSet (así lo construye el controlador de Deployments), sin
    llamadas extra a la API ni heurísticas sobre el nombre del pod.
    """
    metadata = pod.get("metadata", {}) or {}
    owners = metadata.get("ownerReferences") or []
    owner = next((o for o in owners if o.get("controller")), owners[0] if owners else None)
    if not owner:
        return metadata.get("name", ""), "Pod"
    kind, name = owner.get("kind", ""), owner.get("name", "")
    if kind == "ReplicaSet":
        pod_hash = (metadata.get("labels") or {}).get("pod-template-hash")
        if pod_hash and name.endswith(f"-{pod_hash}"):
            return name[: -len(pod_hash) - 1], "Deployment"
        return name, "ReplicaSet"
    if kind == "Job":
        # Jobs creados por un CronJob: <cronjob>-<timestamp numérico>
        match = re.match(r"^(.+)-\d{8,}$", name)
        if match:
            return match.group(1), "CronJob"
    return name, kind


def pod_status(pod: dict) -> str:
    status = pod.get("status", {}) or {}
    if (pod.get("metadata") or {}).get("deletionTimestamp"):
        return "Terminating"
    phase = status.get("phase", "Unknown")
    for cs in status.get("containerStatuses") or []:
        state = cs.get("state") or {}
        waiting = state.get("waiting")
        if waiting and waiting.get("reason"):
            return waiting["reason"]
        terminated = state.get("terminated")
        if terminated and terminated.get("reason") and phase != "Running":
            return terminated["reason"]
    return phase


def pod_ready(pod: dict) -> str:
    statuses = (pod.get("status") or {}).get("containerStatuses") or []
    if not statuses:
        return "0/0"
    ready = sum(1 for cs in statuses if cs.get("ready"))
    return f"{ready}/{len(statuses)}"


def pod_restarts(pod: dict) -> int:
    statuses = (pod.get("status") or {}).get("containerStatuses") or []
    return sum(int(cs.get("restartCount") or 0) for cs in statuses)


def pod_containers(pod: dict) -> list[dict]:
    spec = pod.get("spec") or {}
    statuses = {cs.get("name"): cs for cs in (pod.get("status") or {}).get("containerStatuses") or []}
    result = []
    for container in spec.get("containers") or []:
        name = container.get("name")
        cs = statuses.get(name, {})
        result.append({
            "name": name,
            "image": container.get("image"),
            "ready": bool(cs.get("ready")),
            "restarts": int(cs.get("restartCount") or 0),
            "has_previous": bool((cs.get("lastState") or {}).get("terminated")),
        })
    return result


def pod_summary(pod: dict) -> dict:
    metadata = pod.get("metadata", {}) or {}
    workload, kind = resolve_workload(pod)
    return {
        "name": metadata.get("name"),
        "namespace": metadata.get("namespace"),
        "created": metadata.get("creationTimestamp"),
        "workload": workload,
        "workload_kind": kind,
        "ready": pod_ready(pod),
        "status": pod_status(pod),
        "restarts": pod_restarts(pod),
        "containers": pod_containers(pod),
    }


def group_by_workload(pods: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str], list[dict]] = {}
    for pod in pods:
        groups.setdefault((pod["workload"], pod["workload_kind"]), []).append(pod)
    return [
        {"workload": name, "kind": kind, "pods": sorted(items, key=lambda p: p["name"])}
        for (name, kind), items in sorted(groups.items())
    ]
