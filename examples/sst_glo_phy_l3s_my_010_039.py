"""recipe for creating a STAC catalog for the copernicus global high resolution odyssea sst multi-sensor l3 observations

Overview: https://data.marine.copernicus.eu/product/SST_GLO_PHY_L3S_MY_010_039/description
Data server at:
- s3 endpoint: https://s3.waw3-1.cloudferro.com
- url: mdl-native-06/native/SST_GLO_PHY_L3S_MY_010_039/cmems_obs-sst_glo_phy_my_l3s_P1D-m_202311

Dependencies:
- beam
- pangeo-forge-recipes
- stac-recipes
- fsspec
- s3fs
- pystac
- xstac
- h5netcdf
- shapely
"""

import itertools
import pathlib
import textwrap
from dataclasses import dataclass
from functools import partial

import apache_beam as beam
import pandas as pd
import pystac
import shapely
import shapely.wkt
from beam_pyspark_runner.pyspark_runner import PySparkRunner
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray
from rich.console import Console

from stac_recipes.transforms import CreateStacItem, ToStaticJson

console = Console()


def generate_url(root, time):
    suffix = "IFR-L3S_GHRSST-SSTfnd-ODYSSEA-GLO_MY_010_adjusted-v02.0-f01.0"

    return f"s3://{root}/{time.year}/{time.month:02d}/{time:%Y%m%d%H%M%S}-{suffix}.nc"


def generate_item_template(ds):
    url = ds.encoding["source"]

    date = pd.to_datetime(ds["time"].data[0])

    item_id = f"{ds.attrs['id']}-{date:%Y%m%d}"
    bbox = [
        float(ds.attrs["geospatial_lon_min"]),
        float(ds.attrs["geospatial_lat_min"]),
        float(ds.attrs["geospatial_lon_max"]),
        float(ds.attrs["geospatial_lat_max"]),
    ]

    geometry = shapely.geometry.mapping(
        shapely.wkt.loads(ds.attrs["geospatial_bounds"])
    )

    properties = {
        "start_datetime": ds.attrs["time_coverage_start"],
        "end_datetime": ds.attrs["time_coverage_start"],
    }

    item = pystac.Item(
        id=item_id, geometry=geometry, bbox=bbox, datetime=None, properties=properties
    )
    item.add_asset("data", pystac.Asset(href=url, media_type="application/netcdf"))

    return item


def postprocess_item(item, ds):
    return item


storage_kwargs = {"endpoint_url": "https://s3.waw3-1.cloudferro.com", "anon": True}
data_root = "mdl-native-06/native/SST_GLO_PHY_L3S_MY_010_039/cmems_obs-sst_glo_phy_my_l3s_P1D-m_202311"

target_root = pathlib.Path("catalogs")
out_root = target_root / "cmems_ifr_l3s_glo_odyssea"
out_root.mkdir(parents=True, exist_ok=True)
missing_dates = ["1988-07-12"]
time_values = pd.date_range("1982-01-01", "2022-12-31", freq="D").drop(
    labels=missing_dates
)
time = ConcatDim("time", time_values)
pattern = FilePattern(partial(generate_url, root=data_root), time, file_type="netcdf4")

collection = pystac.Collection(
    "IFR-L3S-GLO-ODYSSEA",
    title="Global High Resolution ODYSSEA Sea Surface Temperature Multi-sensor L3 Observations",
    description=textwrap.wrap(
        textwrap.dedent(
            """\
            This product provides daily foundation sea surface temperature from multiple satellite
            sources. The data are intercalibrated. This product consists in a fusion of sea
            surface temperature observations from multiple satellite sensors, daily, over a 0.05°
            resolution grid. It includes observations by polar orbiting from the ESA CCI / C3S
            archive. The L3S SST data are produced selecting only the highest quality input data
            from input L2P/L3P images within a strict temporal window (local nightime), to avoid
            diurnal cycle and cloud contamination. The observations of each sensor are
            intercalibrated prior to merging using a bias correction based on a multi-sensor
            median reference correcting the large-scale cross-sensor biases.
            """.rstrip()
        ),
        width=90,
    ),
    extent=pystac.Extent.from_dict(
        {
            "spatial": {"bbox": [[-180, -80, 180, 80]]},
            "temporal": {"interval": [["1982-01-01", "2022-12-31"]]},
        }
    ),
    providers=[
        pystac.Provider(
            name="CMEMS", roles=["host"], url="https://marine.copernicus.eu"
        ),
        pystac.Provider(
            name="Ifremer", roles=["producer"], url="https://doi.org/10.48670/mds-00329"
        ),
    ],
    keywords=["Oceans", "Ocean Temperature", "Sea Surface Temperature"],
    extra_fields={},
    license="proprietary",
)
collection.links.extend(
    [
        pystac.Link(
            rel="license",
            target="https://marine.copernicus.eu/user-corner/service-commitments-and-licence",
            title="CMEMS Service Commitments and Licence",
        ),
        pystac.Link(rel="cite-as", target="https://doi.org/10.48670/mds-00329"),
    ]
)


@dataclass
class AttachToCollection(beam.CombineFn):
    collection: pystac.Collection

    def create_accumulator(self):
        return []

    def add_input(self, col, input):
        return col + [input]

    def merge_accumulators(self, cols):
        return list(itertools.chain.from_iterable(cols))

    def extract_output(self, col):
        final_collection = self.collection.clone()
        final_collection.add_items(col)

        return final_collection


@dataclass
class AttachTo(beam.PTransform):
    collection: pystac.Collection

    def expand(self, pcoll):
        return pcoll | "Combine to collection" >> beam.CombineGlobally(
            AttachToCollection(collection=self.collection)
        )


recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(open_kwargs=storage_kwargs)
    | OpenWithXarray(file_type=pattern.file_type)
    | CreateStacItem(
        template=generate_item_template,
        postprocess=postprocess_item,
        xstac_kwargs={"reference_system": "epsg:4326"},
    )
    | AttachTo(collection=collection)
    | ToStaticJson(
        href=str(out_root), catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED
    )
)

console.log("recipe: successfully created recipe")
with beam.Pipeline(runner=PySparkRunner()) as p:
    p | recipe
console.log("recipe: successfully executed recipe")
