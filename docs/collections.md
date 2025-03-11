# Creating collections

Since reducing items to collections has a lot of overhead, we'll instead go with a separate strategy: statically
defining the collection in advance. This has the advantage that the database (or the static STAC catalog) can be written
in advance, making the writing of items more easily parallelizable – or rather, the parallelization is done by the
database.

The items can either be directly created as STAC collection objects, or read from a yaml file using the syntax described in this document:

```python
import pathlib
from stac_recipes.readers import open_collections

def create_collections(pipeline, ...):
    path = pathlib.Path(...)

    collections = open_collections(path)

    return (
        pipeline
        | beam.Create(collections)
        | ...
    )
```

## STAC collection yaml spec

JSON is not particularly human-readable, especially when it comes to fiddling with braces and brackets. To allow easier
modification by humans, the spec is defined as yaml.

Any file containing collections contains a list of collection objects at the root level:

```yaml
- collection1
- collection2
```

As a reminder, yaml allows multi-line strings:

```yaml
- field: >-
    field value
    as a string
    with multiple lines
- |-
  list item
  with multiple
  lines
```

The difference between `>` and `|` is that `>` concatenates all the lines, while `|` keeps the newlines. Additionally,
`>-` or `|-` remove the trailing newline, while `>`, `|` and `>+` and `|+` keep the trailing newline.

## Structure of the STAC collection object

STAC collection objects contain the following fields:

| name            | type     | description                                                                               |
| --------------- | -------- | ----------------------------------------------------------------------------------------- |
| id              | str      | unique ID of the collection                                                               |
| description     | str      | Description of the collection. Can contain fancy markdown. Typically a multi-line string. |
| extent          | dict     | Spatial and temporal [extent](#spatial-extent)                                            |
| title           | str      | title of the collection.                                                                  |
| stac_extensions | [str]    | list of [stac extensions](#stac-extensions) used by the collection object.                |
| license         | str      | Short license name (see the stac spec for the definition)                                 |
| keywords        | [str]    | list of keywords                                                                          |
| providers       | [object] | list of [providers](#providers)                                                           |
| links           | [object] | list of [links](#links)                                                                   |
| assets          | [object] | list of [assets](#assets)                                                                 |

Most of the keys are defined as in the [STAC collection spec](https://github.com/radiantearth/stac-spec/blob/master/collection-spec/collection-spec.md).

### Extent

The extent is a simplified version of its base in the STAC spec. It must contain the following keys:

| name     | type         | description                           |
| -------- | ------------ | ------------------------------------- |
| spatial  | str \| [str] | String describing the spatial extent. |
| temporal | str \| [str] | String describing the spatial extent. |

Where `spatial` can be the string `"global"`, a bbox string or a list of bbox strings. Each bbox string has the format

```
"bbox(minx, miny, maxx, maxy)"
"bbox(minx, miny, minh, maxx, maxy, maxh)"
```

(the whitespace after the `,` is required)

`temporal` can be either a single interval string or a list of interval strings. Each interval string has the format

```
"YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ"
"/YYYY-MM-DDTHH:MM:SSZ"
"YYYY-MM-DDTHH:MM:SSZ/"
```

### Providers

Providers translate the STAC spec's provider objects directly to yaml. The only difference is that `description` should use `>-` for multi-line strings.
