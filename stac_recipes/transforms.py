import os
from dataclasses import dataclass, field
from typing import Callable

import apache_beam as beam
import pystac
import xstac
from pystac.utils import make_absolute_href, make_relative_href
from tlz.functoolz import curry


def passthrough(item, ds):
    return item


def create_stac_item(indexed, template, postprocess, xstac_kwargs=None):
    if xstac_kwargs is None:
        xstac_kwargs = {}

    index, elem = indexed

    with elem as ds:
        ds_ = xstac.fix_attrs(ds)
        if callable(template):
            template = template(ds_)

        item = xstac.xarray_to_stac(
            ds_,
            template,
            **xstac_kwargs,
        )

        return postprocess(item, ds_)


@dataclass
class CreateStacItem(beam.PTransform):
    """create STAC items from files using xstac"""

    template: pystac.Item | Callable
    postprocess: Callable = passthrough
    xstac_kwargs: dict = field(default_factory=dict)

    def expand(self, pcoll):
        return pcoll | "Create STAC item" >> beam.Map(
            curry(
                create_stac_item,
                template=self.template,
                postprocess=self.postprocess,
                xstac_kwargs=self.xstac_kwargs,
            )
        )


@dataclass
class CombineAsCollection(beam.CombineFn):
    template: callable

    spatial_extent: list[float] | str = "from_items"
    temporal_extent: list[str] | str = "from_items"

    postprocess: callable = None

    def create_accumulator(self):
        return None

    def add_input(self, col, input):
        if col is None:
            # fresh accumulator
            col = self.template(col, input)

        col.add_item(input)

        return col

    def merge_accumulators(self, collections):
        merged = collections[0].clone()
        for col in collections[1:]:
            merged.add_items(col.get_items())

        return merged

    def extract_output(self, col):
        if self.spatial_extent == "from_items" or self.temporal_extent == "from_items":
            col.update_extent_from_items()

        if self.spatial_extent == "global":
            col.extent.spatial = pystac.SpatialExtent.from_dict(
                {"bbox": [-180, -90, 180, 90]}
            )
        elif isinstance(self.spatial_extent, dict):
            col.extent.spatial = pystac.SpatialExtent.from_dict(self.spatial_extent)
        elif self.spatial_extent != "from_items":
            raise ValueError(f"unknown spatial extent: {self.spatial_extent}")

        if isinstance(self.temporal_extent, dict):
            col.extent.temporal = pystac.TemporalExtent.from_dict(self.temporal_extent)
        elif self.temporal_extent != "from_items":
            raise ValueError(f"unknown temporal extent: {self.temporal_extent}")

        if self.postprocess is not None:
            return self.postprocess(col)

        return col


@dataclass
class CreateCollection(beam.PTransform):
    template: callable

    spatial_extent: list[float | int] | str = "from_items"
    temporal_extent: list[str] | str = "from_items"

    postprocess: callable = None

    def expand(self, pcoll):
        return (
            pcoll
            | "Group by collection name"
            >> beam.GroupBy(lambda it: it.properties["collection"])
            | "Combine to collection"
            >> beam.CombineValues(
                CombineAsCollection(
                    template=self.template,
                    postprocess=self.postprocess,
                    spatial_extent=self.spatial_extent,
                    temporal_extent=self.temporal_extent,
                )
            )
        )


@dataclass
class CreateRootCatalog(beam.CombineFn):
    template: callable

    def create_accumulator(self):
        return None

    def add_input(self, catalog, input):
        name, collection = input
        if catalog is None:
            catalog = self.template(collection)

        cat = catalog.clone()
        cat.add_child(collection)

        return cat

    def merge_accumulators(self, catalogs):
        merged = catalogs[0].clone()
        for cat in catalogs[1:]:
            merged.add_child(cat.clone())

        return merged

    def extract_output(self, catalog):
        return catalog


@dataclass
class CreateCatalog(beam.PTransform):
    template: callable

    def expand(self, pcoll):
        return pcoll | "Create a root catalog" >> beam.CombineGlobally(
            CreateRootCatalog(template=self.template)
        )


def maybe_normalize_hrefs(cat, href, normalize=True):
    cat.normalize_hrefs(href)

    return cat


def dehydrate_catalog(cat, dest_href, catalog_type):
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

    obj.save_object(
        dest_href=href, include_self_link=include_self_link, stac_io=stac_io
    )

    return href


@dataclass
class ToStaticJson(beam.PTransform):
    href: str
    normalize: bool = True
    catalog_type: pystac.CatalogType = pystac.CatalogType.RELATIVE_PUBLISHED

    def expand(self, pcoll):
        return (
            pcoll
            | "Normalize hrefs"
            >> beam.Map(
                curry(maybe_normalize_hrefs, href=self.href, normalize=self.normalize)
            )
            | "Dehydrate"
            >> beam.FlatMap(
                dehydrate, catalog_type=self.catalog_type, dest_href=self.href
            )
            | "Write to disk" >> beam.Map(store_as_json)
        )
