# Public API Reference: `polyopen`

*[Return to Main README](../README.md)*

This document serves as the formal specification for developers programmatically integrating `polyopen` into Python workspaces. 

## The Core Wrapper

### `polyopen(filename: Union[str, os.PathLike], mode: str = 'r', backup: Optional[bool] = None, show_progress: bool = False)`
This function provides a drop-in replacement for the native Python `builtins.open()` context manager. It resolves remote protocols and stream compression formats automatically behind a uniform UTF-8 text interface.

#### Parameters
- **`filename`** (`Union[str, os.PathLike]`): The target path or URL (for example `tests/data.txt`, `pathlib.Path("data.gz")`, `https://example.com/log.zst`, `ftp://user:pass@host/data.txt`, or `ssh://user@host/syslog`). Query strings are ignored when detecting extensions, and extension matching is case-insensitive.
- **`mode`** (`str`): Target I/O mode:
  - Reading operations (`'r'`, `'rt'`, `'rb'`) open a `PolyReader`.
  - Writing / truncating operations (`'w'`, `'wt'`, `'wb'`) open a `PolyWriter(append=False)`.
  - Appending operations (`'a'`, `'at'`, `'ab'`) open a `PolyWriter(append=True)`.
- **`backup`** (`Optional[bool]`): Controls whether `polyopen` rotates an existing file to a numbered backup (`file.001.bu` through `file.999.bu`, then nanosecond timestamp fallback) before overwriting locally or over SSH/FTP. Defaults to `None`, which resolves to `True` in write modes and `False` in append modes.
  - *Guard Rails:* Explicitly passing `backup=True` with an append mode (`'a'`, `'at'`, `'ab'`) raises `ValueError`.
- **`show_progress`** (`bool`): Enables a console `tqdm` progress bar tracking exact raw bytes read (compressed bytes on compressed streams, with indeterminate `total=None` when stream size is unknown or missing `Content-Length`).
  - *Guard Rails:* Only supported on `PolyReader`. Passing `show_progress=True` to `polyopen(..., 'w'/'a')` or `PolyWriter(..., show_progress=True)` raises `NotImplementedError`.

#### Exceptions
All custom exceptions inherit from `PolyopenError`:

- `UnsupportedArchiveError`: Raised when the normalized path matches an unsupported archive format (`.zip`, `.tar`, `.tgz`, `.7z`, `.rar`, `.tar.gz`, `.tar.bz2`, `.tar.zst`, `.gz.tar`), including URLs with query strings.
- `ReadOnlyProtocolError`: Raised when attempting write or append operations on `http://` or `https://`.
- `UnsupportedProtocolError`: Raised when invoking unsupported cloud object store schemes (`s3://`, `gs://`, `az://`, `hdfs://`), recommending `smart_open` or dedicated SDKs such as `boto3`.
- Standard transport exceptions: missing remote SFTP files raise `FileNotFoundError` (never `SystemExit` or credential printing), and HTTP 4xx/5xx responses close the response and raise `requests.HTTPError`.

---

## Direct Handler Abstractions

Advanced callers bypassing `polyopen(...)` may use `PolyReader` and `PolyWriter` directly.

### `class PolyReader`
Reads local and remote (`ftp://`, `ssh://`, `sftp://`, `http://`, `https://`) UTF-8 text streams with transparent `.gz`, `.bz2`, `.xz`, and multi-frame `.zst` decompression.

#### Methods
- **`__init__(self, filename: Union[str, os.PathLike], show_progress: bool = False)`**: Normalizes `PathLike` inputs via `os.fsdecode()` and configures optional raw-byte progress tracking.
- **`open(self) -> 'PolyReader'`**: Establishes transport connections (FTP defaults to port 21; SSH/SFTP defaults to port 22 and forwards custom URL ports; HTTP validates status via `raise_for_status()` and parses `Content-Length` safely). Calling `open()` after `close()` reopens cleanly.
- **`read(self, size: int = -1) -> str`**, **`readline(self, size: int = -1) -> str`**, **`readlines(self, hint: int = -1) -> list`**, and **`__next__(self) -> str`**: Standard file-object read methods returning UTF-8 `str` and updating raw-byte progress without double-counting. Raise `ValueError` when called on a closed reader.
- **`close(self) -> None`**: Idempotently closes all acquired handles (stream wrappers, raw handles, SFTP file/client, SSH client, FTP host, HTTP response, and progress bar) in reverse ownership order and resets state attributes.

### `class PolyWriter`
Writes local and remote (`ftp://`, `ssh://`, `sftp://`) UTF-8 text streams with transparent `.gz`, `.bz2`, `.xz`, and `.zst` compression and collision-safe backup rotation.

#### Methods
- **`__init__(self, filename: Union[str, os.PathLike], show_progress: bool = False)`**: Normalizes `PathLike` inputs via `os.fsdecode()`; raises `NotImplementedError` if `show_progress=True`.
- **`open(self, append: bool = False, backup: bool = True) -> 'PolyWriter'`**: Performs atomic local backup rotation (`os.link`/`os.unlink` with `FileExistsError` retry and `os.rename` fallback where hard links are unsupported) or remote SFTP/FTP backup rotation capped at 999 sequential slots with nanosecond collision-safe fallback. FTP defaults to port 21; SSH/SFTP defaults to port 22 and forwards custom URL ports.
- **`write(self, data: str) -> None`** and **`flush(self) -> None`**: Write UTF-8 text and flush buffered output; raise `ValueError` when called on a closed writer.
- **`close(self) -> None`**: Idempotently closes all acquired handles in reverse ownership order and resets state attributes.
