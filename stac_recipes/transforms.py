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


def _extract_keywords(attrs, spec):
    if not isinstance(spec, str):
        return spec
    else:
        return attrs.get(spec.removeprefix("attr:"))


@dataclass
class CombineAsCollection(beam.CombineFn):
    id: str = "{attrs[id]}"
    title: str = "{attrs[title]}"
    description: str = "{attrs[description]}"

    keywords: list | str = field(default_factory=list)
    extra_fields: dict = field(default_factory=dict)
    providers: list = field(default_factory=list)

    spatial_extent: dict | str = "from_items"
    temporal_extent: dict | str = "from_items"

    license: str = "proprietary"

    postprocess: callable = None

    def create_accumulator(self):
        return pystac.Collection(
            id="",
            title=None,
            description=None,
            extent=pystac.Extent.from_dict(
                {
                    "spatial": {"bbox": [-180, -90, 180, 90]},
                    "temporal": {"interval": [["1900-01-01", "2100-01-01"]]},
                }
            ),
            keywords=[],
            extra_fields={},
            providers=[],
        )

    def add_input(self, accumulator, input):
        attrs = input.properties.pop("attrs", {})

        if accumulator.id == "":
            # fresh accumulator
            accumulator.id = self.id.format(attrs=attrs)
            accumulator.title = self.title.format(attrs=attrs)
            accumulator.description = self.description.format(attrs=attrs)

            accumulator.keywords = _extract_keywords(attrs, self.keywords)
            accumulator.extra_fields = self.extra_fields
            accumulator.providers = self.providers

            accumulator.license = self.license.format(attrs=attrs)

        accumulator.add_item(input)

        return accumulator

    def merge_accumulators(self, accumulators):
        merged = accumulators[0].clone()
        for cat in accumulators[1:]:
            merged.add_items(cat.get_items())

        if self.spatial_extent == "from_items" or self.temporal_extent == "from_items":
            merged.update_extent_from_items()

        if self.spatial_extent == "global":
            merged.extent.spatial = pystac.SpatialExtent.from_dict(
                {"bboxes": [-180, -90, 180, 90]}
            )
        elif isinstance(self.spatial_extent, dict):
            merged.extent.spatial = pystac.SpatialExtent.from_dict(self.spatial_extent)
        elif self.spatial_extent != "from_items":
            raise ValueError(f"unknown spatial extent: {self.spatial_extent}")

        if isinstance(self.temporal_extent, dict):
            merged.extent.temporal = pystac.TemporalExtent.from_dict(
                self.temporal_extent
            )
        elif self.temporal_extent != "from_items":
            raise ValueError(f"unknown temporal extent: {self.temporal_extent}")

        return merged

    def extract_output(self, accumulator):
        if self.postprocess is not None:
            return self.postprocess(accumulator)

        return accumulator


@dataclass
class CreateCollection(beam.PTransform):
    id: str = "{attrs[id]}"
    title: str = ""
    description: str = ""

    keywords: list | str = field(default_factory=list)
    extra_fields: dict = field(default_factory=dict)
    providers: list = field(default_factory=list)

    spatial_extent: dict | str = "from_items"
    temporal_extent: dict | str = "from_items"

    license: str = "proprietary"

    postprocess: callable = None

    combine_kwargs: dict = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self):
        self.combine_kwargs = {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "spatial_extent": self.spatial_extent,
            "temporal_extent": self.temporal_extent,
            "license": self.license,
            "postprocess": self.postprocess,
        }

    def expand(self, pcoll):
        return (
            pcoll
            | "Group by collection name"
            >> beam.GroupBy(lambda it: it.properties["collection"])
            | "Combine to collection"
            >> beam.CombineValues(CombineAsCollection(**self.combine_kwargs))
        )
