import pathlib
from collections.abc import Generator

import pystac
import yaml


def spec_to_pystac(colspec: dict) -> pystac.Collection:
    pass


def open_collections(path: pathlib.Path) -> Generator[pystac.Collection]:
    root = yaml.safe_load(path.read_text())

    for col_spec in root["collections"]:
        col = spec_to_pystac(col_spec)

        yield col.id, col
