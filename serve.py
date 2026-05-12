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
import json
import traceback

import httpx


om_client = httpx.Client(base_url=os.environ["AMSC_OPENMETADATA_URL"])


def build_body(update, tiled_uri):
    metadata = update.metadata
    if update.structure_family == "container":
        body = { 
            "type": "artifactCollection",
            "name": path[-1],
            "description": metadata.get("description", json.dumps(metadata)),
            "display_name": metadata.get("display_name", metadata["uid"]),
            "location": f"{tiled_uri}/{path[-1]}",
            "parent_fqn": "bnl-lse-demo-storage.bnl-lse-demo-data-catalog.base",
        }
    else:
        body = { 
            "type": "artifact",
            "name": path[-1],
            "description": metadata.get("description", json.dumps(metadata)),
            "display_name": metadata.get("display_name", metadata["uid"]),
            "location": f"{tiled_uri}/{path[-1]}",
            "parent_fqn": "bnl-lse-demo-storage.bnl-lse-demo-data-catalog.base",
            "format": update.data_sources[0].mimetype,
            # "size":  # add this when assets know their size
        }
    return body


def upload(update, tiled_uri, client):
    try:
        body = build_body(update, tiled_uri)
        entity_type = body["type"]
        catalog_name = os.environ["AMSC_OPENMETADATA_CATALOG_NAME"]
        response = client.post(
            f"/catalog/{catalog_name}/{entity_type}",
            headers={
                "Authorization": f"Bearer {os.environ['AMSC_OPENMETADATA_TOKEN']}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=body,
        )
        response.raise_for_status()
    except Exception as exc:
        traceback.print_exc() 


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
    from starlette.background import BackgroundTask
    from starlette.responses import JSONResponse, Response
    from starlette.routing import Route

    async def review(request):
        return JSONResponse(responses)

    async def publish(request):
        data = await request.json()
        tiled_url = "https://tiled-staging.nsls2.bnl.gov/api/v1/metadata" + "".join(f"/{segment}" for segment in data["path"])
        task = BackgroundTask(upload, data, tiled_url, om_client)
        return Response(status_code=204, task=task)

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
