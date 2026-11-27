"""Unified stream-level file I/O across local files, remote protocols, and compression formats.

Quick Start
-----------
>>> from openas import openas
>>> with openas("data.gz", "w") as w:
...     w.write("hello\\n")
>>> with openas("data.gz", "r") as r:
...     for line in r:
...         print(line, end="")

Exports `openas`, `OpenAsReader`, `OpenAsWriter`, and custom exceptions (`OpenAsError`,
`UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`).
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


__all__ = [
    "openas",
    "OpenAsReader",
    "OpenAsWriter",
    "OpenAsError",
    "UnsupportedArchiveError",
    "ReadOnlyProtocolError",
    "UnsupportedProtocolError",
    "__version__",
    "__author__",
]
