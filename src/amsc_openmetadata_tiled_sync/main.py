import json
import traceback
from functools import partial

import httpx
from tiled.client import from_uri
from tiled.stream_messages import ChildCreated
from tiled.structures.core import StructureFamily

from .config import OpenMetadataSettings, Settings


def build_body(update: ChildCreated, tiled_uri: str):
    metadata = update.metadata
    if update.structure_family == StructureFamily.container:
        body = {
            "type": "artifactCollection",
            "name": update.key,
            "description": metadata.get("description", json.dumps(metadata)),
            "display_name": metadata.get("display_name", metadata["uid"]),
            "location": f"{tiled_uri}/{update.key}",
            "parent_fqn": "bnl-lightshow-storage.bnl-lightshow-catalog.base",
        }
    else:
        body = {
            "type": "artifact",
            "name": update.key,
            "description": metadata.get("description", json.dumps(metadata)),
            "display_name": metadata.get("display_name", metadata["uid"]),
            "location": f"{tiled_uri}/{update.key}",
            "parent_fqn": "bnl-lightshow-storage.bnl-lightshow-catalog.base",
            "format": update.data_sources[0].mimetype,
            # "size":  # add this when assets know their size
        }
    return body


def upload(
    update: ChildCreated,
    tiled_uri: str,
    client: httpx.Client,
    settings: OpenMetadataSettings,
):
    try:
        body = build_body(update, tiled_uri)
        entity_type = body["type"]
        response = client.post(
            f"/catalog/{settings.catalog_name}/{entity_type}",
            headers={
                "Authorization": f"Bearer {settings.token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            json=body,
        )
        response.raise_for_status()
    except Exception:
        traceback.print_exc()


def listen():
    settings = Settings()
    tiled_uri = settings.tiled.uri.unicode_string()
    tiled_client = from_uri(tiled_uri)

    # omd client
    omd_base_url = settings.amsc_openmetadata.base_url.unicode_string()
    client = httpx.Client(base_url=omd_base_url)

    sub = tiled_client.subscribe()
    callback = partial(
        upload,
        tiled_uri=tiled_uri,
        client=client,
        settings=settings.amsc_openmetadata,
    )
    sub.child_created.add_callback(callback)
    sub.start()  # block
