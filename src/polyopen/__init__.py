"""
polyopen - A Python module for versatile file handling.

This package exposes `polyopen`, `PolyReader`, and `PolyWriter` for easy access.
"""

__version__ = "0.1.0"
__author__ = "Gabriele Fariello"
__email__ = ""  # Leaving blank or user can fill in

from .polyopen import (
    polyopen, PolyReader, PolyWriter,
    PolyopenError, UnsupportedArchiveError, ReadOnlyProtocolError, UnsupportedProtocolError
)


def __getattr__(name: str):
    if name == "FTPSessionWrapper":
        import sys
        return getattr(sys.modules["polyopen.polyopen"], "FTPSessionWrapper")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "polyopen", "PolyReader", "PolyWriter", "FTPSessionWrapper",
    "PolyopenError", "UnsupportedArchiveError", "ReadOnlyProtocolError", "UnsupportedProtocolError",
    "__version__", "__author__"
]
