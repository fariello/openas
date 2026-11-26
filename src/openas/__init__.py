"""
openas - Unified stream-level file I/O across local files, remote protocols, and compression formats.

Exports `openas`, `OpenAsReader`, `OpenAsWriter`, custom exceptions (`OpenAsError`,
`UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`),
and (via lazy PEP 562 lookup for backward compatibility) `FTPSessionWrapper`.
"""

from .openas import (
    OpenAsReader,
    OpenAsWriter,
    OpenAsError,
    ReadOnlyProtocolError,
    UnsupportedArchiveError,
    UnsupportedProtocolError,
    __author__,
    __version__,
    openas,
)


def __getattr__(name: str):
    if name == "FTPSessionWrapper":
        import importlib
        _openas_mod = importlib.import_module(f"{__name__}.openas")
        return getattr(_openas_mod, "FTPSessionWrapper")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "openas",
    "OpenAsReader",
    "OpenAsWriter",
    "FTPSessionWrapper",
    "OpenAsError",
    "UnsupportedArchiveError",
    "ReadOnlyProtocolError",
    "UnsupportedProtocolError",
    "__version__",
    "__author__",
]
