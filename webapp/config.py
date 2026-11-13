"""Configuración del visor: archivo YAML (config.yaml) + overrides por variables PODS_*.

No hay valores atados a ningún proveedor ni clúster: el acceso se resuelve con el
kubeconfig estándar (contexto actual o el elegido en la UI). Los "entornos" del
archivo son solo atajos con nombre (contexto + namespaces sugeridos).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")

DEFAULT_EXCLUDED_CONTAINERS = ["istio-proxy", "istio-init", "linkerd-proxy", "linkerd-init"]


@dataclass
class Environment:
    name: str
    context: str | None = None        # contexto del kubeconfig (None = contexto actual)
    kubeconfig: str | None = None     # ruta (None = KUBECONFIG o ~/.kube/config)
    in_cluster: bool = False          # usar el ServiceAccount del pod (despliegue dentro del clúster)
    namespaces: list[str] = field(default_factory=list)  # vacío = listar los que permita RBAC


@dataclass
class FormatOverride:
    workload: str                     # regex sobre el nombre del workload
    format: str                       # nombre de formato (ver /api/formats) o "auto"
    container: str | None = None      # regex opcional sobre el contenedor


@dataclass
class Settings:
    host: str = "127.0.0.1"
    port: int = 8765
    allowed_origins: list[str] = field(default_factory=list)

    db_path: str = os.path.join(BASE_DIR, "data", "logs.db")
    retention_days: float = 7
    max_rows: int = 2_000_000

    tail_lines: int = 500
    exclude_containers: list[str] = field(default_factory=lambda: list(DEFAULT_EXCLUDED_CONTAINERS))
    max_backoff_seconds: float = 30
    multiline_idle_seconds: float = 0.5
    follow_rollouts: bool = True      # seguir automáticamente el pod que reemplaza a uno eliminado

    environments: list[Environment] = field(default_factory=list)
    format_overrides: list[FormatOverride] = field(default_factory=list)
    custom_formats: list[dict] = field(default_factory=list)


def _resolve_path(path: str | None) -> str | None:
    if not path:
        return None
    path = os.path.expandvars(os.path.expanduser(path))
    return path if os.path.isabs(path) else os.path.join(BASE_DIR, path)


def load_settings(path: str | None = None) -> Settings:
    path = path or os.environ.get("PODS_CONFIG") or DEFAULT_CONFIG_PATH
    data = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}

    server = data.get("server", {}) or {}
    storage = data.get("storage", {}) or {}
    collector = data.get("collector", {}) or {}
    formats = data.get("log_formats", {}) or {}

    settings = Settings()
    settings.host = os.environ.get("PODS_HOST", server.get("host", settings.host))
    settings.port = int(os.environ.get("PODS_PORT", server.get("port", settings.port)))
    settings.allowed_origins = list(server.get("allowed_origins") or [])

    settings.db_path = _resolve_path(os.environ.get("PODS_DB_PATH", storage.get("path"))) or settings.db_path
    settings.retention_days = float(storage.get("retention_days", settings.retention_days))
    settings.max_rows = int(storage.get("max_rows", settings.max_rows))

    settings.tail_lines = int(os.environ.get("PODS_LOG_TAIL", collector.get("tail_lines", settings.tail_lines)))
    if "exclude_containers" in collector:
        settings.exclude_containers = list(collector.get("exclude_containers") or [])
    settings.max_backoff_seconds = float(collector.get("max_backoff_seconds", settings.max_backoff_seconds))
    settings.multiline_idle_seconds = float(collector.get("multiline_idle_seconds", settings.multiline_idle_seconds))
    settings.follow_rollouts = bool(collector.get("follow_rollouts", settings.follow_rollouts))

    for env in data.get("environments", []) or []:
        settings.environments.append(Environment(
            name=str(env["name"]),
            context=env.get("context") or None,
            kubeconfig=_resolve_path(env.get("kubeconfig")),
            in_cluster=bool(env.get("in_cluster", False)),
            namespaces=list(env.get("namespaces") or []),
        ))

    for item in formats.get("overrides", []) or []:
        settings.format_overrides.append(FormatOverride(
            workload=item["workload"], format=item["format"], container=item.get("container"),
        ))
    settings.custom_formats = [
        {"name": str(c["name"]), "pattern": str(c["pattern"])} for c in formats.get("custom", []) or []
    ]
    return settings
