# stac-recipes

Like [pangeo-forge-recipes](https://github.com/pangeo-forge/pangeo-forge-recipes), but for creating catalogs.

Typical usage:

```python
from stac_recipes.patterns import FilePattern, FileType
from stac_recipes.transforms import CreateStacTemplate, XarrayToStac, Branch, Passthrough
from pangeo_forge_recipes.transforms import OpenWithXarray

import apache_beam as beam
import pystac

import pandas as pd

def make_full_path(time):
    return f"..."


def generate_item_template(ds, path):
    item = pystac.Item(...)
    ...

    return item


time = pd.date_range("2022-01-01", periods=10, freq="d")
pattern = FilePattern(make_full_path, time)

recipe = (
    beam.Create(pattern.items())
    | Branch([
        OpenWithXarray(file_type=pattern.file_type),
        Passthrough().
    ])
    | beam.Flatten()
    | CreateStacTemplate(generate_template=generate_item_template)
    | CreateStacItem()
    | CreateCollection(generate_template=generate_collection)
    | WriteStac(path=...)
)

with beam.Pipeline() as p:
    p | recipe
```
