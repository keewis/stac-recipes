from dataclasses import dataclass, field
from typing import Callable

import apache_beam as beam
import pystac
import xstac
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
