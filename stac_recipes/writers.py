import os.path

import pystac
from pystac.utils import make_absolute_href, make_relative_href


def maybe_normalize_hrefs(cat, href, normalize=True):
    cat.normalize_hrefs(href)

    return cat


def dehydrate_catalog(cat, dest_href, catalog_type):
    # TODO: is this actually what dehydrate means?
    if catalog_type == pystac.CatalogType.ABSOLUTE_PUBLISHED:
        catalog_self_link = True
    elif catalog_type != pystac.CatalogType.SELF_CONTAINED:
        # root catalog gets a self link
        catalog_self_link = True
    else:
        catalog_self_link = False

    if dest_href is not None:
        self_href = cat.get_self_href()
        rel_href = make_relative_href(self_href, self_href)
        href = make_absolute_href(rel_href, dest_href, start_is_dir=True)
    else:
        href = None

    yield cat, href, catalog_self_link

    for link in cat.get_child_links():
        if not link.is_resolved():
            continue

        child = link.target
        child.set_parent(cat.get_self_href())
        child.set_root(cat.get_root())

        if dest_href is not None:
            rel_href = make_relative_href(child.self_href, cat.self_href)
            child_href = make_absolute_href(rel_href, dest_href, start_is_dir=True)
        else:
            child_href = None

        yield from dehydrate_catalog(child, os.path.dirname(child_href), catalog_type)

    item_self_link = catalog_type in [pystac.CatalogType.ABSOLUTE_PUBLISHED]
    for link in cat.get_item_links():
        if not link.is_resolved():
            continue

        item = link.target
        # set the root / parent links
        item.set_parent(cat.get_self_href())
        item.set_collection(cat)
        item.set_root(cat.get_root())

        if dest_href is not None:
            rel_href = make_relative_href(item.self_href, cat.self_href)
            item_href = make_absolute_href(rel_href, dest_href, start_is_dir=True)
        else:
            item_href = None

        yield item, item_href, item_self_link


def dehydrate(catalog, dest_href, catalog_type):
    if catalog.get_root() is not catalog:
        raise ValueError("can only dehydrate using the root catalog")

    cat = catalog.clone()
    cat.type = catalog_type

    yield from dehydrate_catalog(cat, dest_href, catalog_type)


def store_as_json(data):
    obj, href, include_self_link = data

    stac_io = pystac.StacIO.default()

    try:
        obj.save_object(
            dest_href=href, include_self_link=include_self_link, stac_io=stac_io
        )
    except Exception as e:
        e.add_note(href)

        raise

    return href


def store_collections_as_pgstac(collections, method, postgresql_options):
    import pypgstac.db
    import pypgstac.load

    db = pypgstac.db.DB(**postgresql_options)
    loader = pypgstac.load.Loader(db)
    loader.load_collections([col.to_dict() for col in collections], method=method)


def store_items_as_pgstac(items, method, postgresql_options):
    import pypgstac.db
    import pypgstac.load

    db = pypgstac.db.DB(**postgresql_options)
    loader = pypgstac.load.Loader(db)

    loader.load_items([item.to_dict() for item in items], method=method)
