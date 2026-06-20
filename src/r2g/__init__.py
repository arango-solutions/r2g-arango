"""r2g: project relational and structured data sources as a graph in ArangoDB."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("r2g-arango")
except PackageNotFoundError:  # running from a source tree without installation
    __version__ = "0.0.0+unknown"
