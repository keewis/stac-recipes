import pathlib
import re
from collections.abc import Generator

import pystac
import yaml

number_re = r"-?\d+(?:\.\d+)?"
bbox_re = re.compile(rf"bbox\((?P<parts>(?:{number_re}, ){{3,5}}{number_re})\)")


def parse_spatial_extent(spatial):
    if not isinstance(spatial, str):
        raise ValueError("spatial extent must be a string")

    if spatial == "global":
        return {"bbox": [[-180, -90, 180, 90]]}
    elif (match := bbox_re.match(spatial)) is not None:
        parts = match.group("parts").split(", ")
        return {"bbox": [parts]}
    else:
        raise ValueError(f"invalid format for the spatial extent: {spatial}")


def parse_temporal_extent(temporal):
    if isinstance(temporal, str):
        temporal = [temporal]

    intervals = [
        [part if part else None for part in interval.split("/")]
        for interval in temporal
    ]

    return {"interval": intervals}


def parse_extent(extent):
    if not isinstance(extent, dict):
        raise ValueError("need to specify the extent as a dict")
    if extent.keys() != {"spatial", "temporal"}:
        raise ValueError(
            "need to specify both spatial and temporal extent"
            f" (got a dict with keys {extent.keys()})"
        )

    transformers = {
        "spatial": parse_spatial_extent,
        "temporal": parse_temporal_extent,
    }
    prepared = {k: transformers[k](v) for k, v in extent.items()}

    return pystac.Extent.from_dict(prepared)


def spec_to_pystac(colspec: dict) -> pystac.Collection:
    transformers = {
        "extent": parse_extent,
    }

    processed = {k: transformers.get(k, lambda v: v)(v) for k, v in colspec.items()}
    links = processed.pop("links", [])
    assets = processed.pop("assets", {})

    col = pystac.Collection(**processed)
    for link in links:
        col.add_link(pystac.Link.from_dict(link))

    for name, asset in assets.items():
        col.add_asset(name, pystac.Asset.from_dict(asset))

    return col


def open_collections(path: str | pathlib.Path) -> Generator[pystac.Collection]:
    if isinstance(path, str):
        path = pathlib.Path(path)

    collections = yaml.safe_load(path.read_text())

    for col_spec in collections:
        col = spec_to_pystac(col_spec)

        yield col.id, col
