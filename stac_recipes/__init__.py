from importlib.metadata import version

try:
    __version__ = version("stac_recipes")
except Exception:
    __version__ = "9999"
