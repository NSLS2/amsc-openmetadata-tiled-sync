"""
Entry point for the NSLS-II American Science Cloud Metadata Relay

Usage
-----
Default (no YAML file):
    pixi run serve

Point at a specific YAML settings file:
    pixi run serve --settings settings.yaml
    pixi run serve --settings /path/to/production.yaml

Extra uvicorn options are passed through directly:
    pixi run serve --settings settings.yaml --port 9000 --no-reload
"""

import argparse
import os
import sys


responses = []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Start the NSLS-II AI FastAPI server.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--settings",
        metavar="FILE",
        default=None,
        help="Path to a YAML settings file (e.g. settings.yaml).",
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)."
    )
    parser.add_argument(
        "--port", type=int, default=8000, help="Bind port (default: 8000)."
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable auto-reload (recommended for production).",
    )

    args = parser.parse_args()

    if args.settings:
        path = os.path.abspath(args.settings)
        if not os.path.isfile(path):
            print(f"Error: settings file not found: {path}", file=sys.stderr)
            sys.exit(1)
        # Pass the resolved path to config.py via an environment variable so
        # that it is visible when pydantic-settings initialises Settings.
        os.environ["SETTINGS_FILE"] = path
        print(f"Loading settings from: {path}")

    import uvicorn

    from starlette.applications import Starlette
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route

    def review(request):
        return JSONResponse(responses)

    def publish(request):
        data = request.json()
        responses.append(data)
        return Response(status_code=204)

    routes = [
        Route("/publish", publish, methods=["POST"]),
        Route("/review", review, methods=["GET"]),
    ]
    app = Starlette(routes=routes)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        # reload=not args.no_reload,
        ssl_certfile=os.getenv("AMSC_CATALOG_RELAY_SSL_CERT"),
        ssl_keyfile=os.getenv("AMSC_CATALOG_RELAY_SSL_KEY"),
    )


if __name__ == "__main__":
    main()
