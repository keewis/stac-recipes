from collections.abc import Sequence

import pystac
from pypgstac.db import DB
from pypgstac.load import Loader


def store_to_pgstac(objs: Sequence[pystac.STACObject], *, type, method, options):
    mappings = [obj.to_dict() for obj in objs]

    db = DB(**options)
    loader = Loader(db)

    if type == "collection":
        loader.load_collections(mappings, method=method)
    elif type == "item":
        loader.load_items(mappings, method=method)
    else:
        raise ValueError(f"invalid type: {type}")
