from collections.abc import Sequence

import pystac
from pypgstac.db import PgstacDB
from pypgstac.load import Loader

ItemType = tuple[str, pystac.STACObject]


def store_to_pgstac(
    objs: ItemType | Sequence[ItemType], *, type_, method, client: PgstacDB
):
    if not isinstance(objs, list):
        objs = [objs]

    mappings = [
        obj.to_dict() if isinstance(obj, pystac.STACObject) else obj for _, obj in objs
    ]

    loader = Loader(client)

    if type_ == "collection":
        loader.load_collections(mappings, insert_mode=method)
    elif type_ == "item":
        loader.load_items(mappings, insert_mode=method)
    else:
        raise ValueError(f"invalid type: {type_}")
