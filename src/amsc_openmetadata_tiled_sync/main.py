import os
import json
import traceback
import sys
from functools import partial

import httpx
from tiled.client import from_uri


def build_body(update, tiled_uri):
    metadata = update.metadata
    # FIXME: Metadata update events do not have structure_family, eventually this should be added
    structure_family = getattr(update, "structure_family", "")
    if structure_family == "container":
        body = {
            "type": "artifactCollection",
            "name": update.key,
            "description": metadata.get("description", json.dumps(metadata)),
            "display_name": metadata.get("display_name", metadata["xdi"]["Scan.uid"]),
            "location": f"{tiled_uri}/{update.key}",
            "parent_fqn": "bnl-lightshow-storage.bnl-lightshow-catalog.base",
        }
    else:
        body = {
            "type": "artifact",
            "name": update.key,
            "description": metadata.get("description", json.dumps(metadata)),
            "display_name": metadata.get("display_name", metadata["xdi"]["Scan.uid"]),
            "location": f"{tiled_uri}/{update.key}",
            "parent_fqn": "bnl-lightshow-storage.bnl-lightshow-catalog.base",
            # FIXME: Metadata update events do not have data_sources, eventually this should be added
            "format": getattr(getattr(update, "data_sources", [None])[0], "mimetype", "application/x-parquet"),
            # "size":  # add this when assets know their size
        }
    return body


def upload(update, tiled_uri, client, is_update=False):
    try:
        body = build_body(update, tiled_uri)
        entity_type = body["type"]
        catalog_name = os.environ["AMSC_OPENMETADATA_CATALOG_NAME"]
        if is_update:
            fqn = f"{body['parent_fqn']}.{body['name']}"
            response = client.put(
                f"/catalog/{fqn}",
                headers={
                    "Authorization": f"Bearer {os.environ['AMSC_OPENMETADATA_TOKEN']}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json=body,
            )
        else:
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
        print(response)
        print(response.json())
    except Exception as exc:
        traceback.print_exc()


def listen(tiled_uri):
    tiled_client = from_uri(tiled_uri)
    client = httpx.Client(base_url="https://api.american-science-cloud.org/api/current")
    sub = tiled_client.subscribe()
    create_callback = partial(
        upload, tiled_uri=tiled_uri, client=client, is_update=False
    )
    update_callback = partial(
        upload, tiled_uri=tiled_uri, client=client, is_update=True
    )
    # On Create
    sub.child_created.add_callback(create_callback)
    # On Update
    sub.child_metadata_updated.add_callback(update_callback)
    sub.start()  # block


def main(args=sys.argv):
    (uri,) = sys.argv[1:]
    listen(tiled_uri)
