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

    index, ds = indexed

    if callable(template):
        template = template(ds)

    item = xstac.xarray_to_stac(
        xstac.fix_attrs(ds),
        template,
        **xstac_kwargs,
    )

    return postprocess(item, ds)


@dataclass
class Branch(beam.PTransform):
    transforms: list[beam.PTransform]

    def expand(self, pcoll):
        for transform in self.transforms:
            yield pcoll | transform


@dataclass
class Passthrough(beam.PTransform):
    def expand(self, pcoll):
        return pcoll


@dataclass
class CreateStacItem(beam.PTransform):
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
