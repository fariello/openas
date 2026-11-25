"""
polyopen - Unified stream-level file I/O across local files, remote protocols, and compression formats.

Exports `polyopen`, `PolyReader`, `PolyWriter`, custom exceptions (`PolyopenError`,
`UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`),
and (via lazy PEP 562 lookup for backward compatibility) `FTPSessionWrapper`.
"""

from .polyopen import (
    PolyReader,
    PolyWriter,
    PolyopenError,
    ReadOnlyProtocolError,
    UnsupportedArchiveError,
    UnsupportedProtocolError,
    __author__,
    __version__,
    polyopen,
)


def __getattr__(name: str):
    if name == "FTPSessionWrapper":
        import importlib
        _polyopen_mod = importlib.import_module(f"{__name__}.polyopen")
        return getattr(_polyopen_mod, "FTPSessionWrapper")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "polyopen",
    "PolyReader",
    "PolyWriter",
    "FTPSessionWrapper",
    "PolyopenError",
    "UnsupportedArchiveError",
    "ReadOnlyProtocolError",
    "UnsupportedProtocolError",
    "__version__",
    "__author__",
]
