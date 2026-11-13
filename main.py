"""Punto de entrada: levanta el dashboard web para consultar logs de microservicios
en cualquier clúster Kubernetes accesible desde el kubeconfig.

El inicio de sesión del proveedor (aws sso login, az login, gcloud auth login, oc
login, etc.) se hace por fuera, como en el uso normal de kubectl: la herramienta
solo consume el kubeconfig resultante. El entorno, el namespace y los pods se
eligen desde la interfaz web.
"""

import argparse
import logging
import threading
import webbrowser

import uvicorn

from webapp.config import load_settings
from webapp.server import create_app


def parse_args():
    parser = argparse.ArgumentParser(description="Visor de logs de microservicios en Kubernetes")
    parser.add_argument("--config", help="Ruta a config.yaml (por defecto ./config.yaml o PODS_CONFIG)")
    parser.add_argument("--host", help="Interfaz de escucha (por defecto 127.0.0.1)")
    parser.add_argument("--port", type=int, help="Puerto (por defecto 8765)")
    parser.add_argument("--no-browser", action="store_true", help="No abrir el navegador al iniciar")
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = load_settings(args.config)
    if args.host:
        settings.host = args.host
    if args.port:
        settings.port = args.port

    if settings.host not in ("127.0.0.1", "localhost", "::1"):
        logging.warning("Escuchando en %s: la herramienta no tiene autenticación; "
                        "exponla solo en redes de confianza.", settings.host)

    app = create_app(settings)
    url = f"http://{'127.0.0.1' if settings.host in ('0.0.0.0', '::') else settings.host}:{settings.port}"
    print(f"\nDashboard disponible en: {url}\nPresiona Ctrl+C para detener.\n")
    if not args.no_browser:
        threading.Timer(1.0, webbrowser.open, args=(url,)).start()
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="warning")


if __name__ == "__main__":
    main()
