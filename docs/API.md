# Public API Reference: `polyopen`

*[Return to Main README](../README.md)*

This document serves as the formal specification for developers programmatically integrating `polyopen` into Python workspaces. 

## The Core Wrapper

### `polyopen(filename: Union[str, os.PathLike], mode: str = 'r', backup: Optional[bool] = None, show_progress: bool = False, compresslevel: Optional[int] = None)`
This function provides a drop-in replacement for the native Python `builtins.open()` context manager. It resolves remote protocols and stream compression formats automatically behind a uniform UTF-8 text interface.

#### Parameters
- **`filename`** (`Union[str, os.PathLike]`): The target path or URL (for example `tests/data.txt`, `pathlib.Path("data.gz")`, `https://example.com/log.zst`, `ftp://user:pass@host/data.txt`, or `ssh://user@host/syslog`). Query strings are ignored when detecting extensions, and extension matching is case-insensitive.
- **`mode`** (`str`): Target I/O mode:
  - Reading operations (`'r'`, `'rt'`, `'rb'`) open a `PolyReader`.
  - Writing / truncating operations (`'w'`, `'wt'`, `'wb'`) open a `PolyWriter(append=False)`.
  - Appending operations (`'a'`, `'at'`, `'ab'`) open a `PolyWriter(append=True)`.
- **`backup`** (`Optional[bool]`): Controls whether `polyopen` rotates an existing file before overwriting locally or over SSH/FTP. Rotation checks `<path>.001.bu` first in O(1); if occupied, it runs a single directory listing (`os.scandir` locally, `listdir` on SFTP/FTP) to select the lowest free 3-digit slot (`file.001.bu` through `file.999.bu`), falling back to a collision-safe nanosecond timestamp (`time.time_ns()`) past 999. Defaults to `None`, which resolves to `True` in write modes and `False` in append modes.
  - *Guard Rails:* Explicitly passing `backup=True` with an append mode (`'a'`, `'at'`, `'ab'`) raises `ValueError`.
- **`show_progress`** (`bool`): Enables a console `tqdm` progress bar tracking exact raw bytes read (compressed bytes on compressed streams, batched in 64 KiB increments with exact flush on EOF/exit/`close()`, and indeterminate `total=None` when stream size is unknown or missing `Content-Length`).
  - *Guard Rails:* Only supported on `PolyReader`. Passing `show_progress=True` to `polyopen(..., 'w'/'a')` or `PolyWriter(..., show_progress=True)` raises `NotImplementedError`.
- **`compresslevel`** (`Optional[int]`): Optional compression level or preset forwarded to write and append modes (ignored in read mode). When `None` (default), each compression library uses its own default: `zstd` level 3, `lzma` preset 6, `bz2` level 9, and `gzip` level 9.

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
- **`open(self) -> 'PolyReader'`**: Establishes transport connections:
  - FTP defaults to port 21 and forwards custom URL ports.
  - SSH/SFTP defaults to port 22, forwards custom URL ports, caches `stat().st_size` once for `_get_file_size()`, and calls bounded `prefetch(file_size, max_concurrent_requests=64)` when size is known (falling back to `prefetch(file_size)` on Paramiko `<3.3`).
  - HTTP/HTTPS reuses a thread-local, cookie-free `requests.Session` (`http.cookiejar.DefaultCookiePolicy(allowed_domains=[])`), validates status via `raise_for_status()` (closing the response on error), parses `Content-Length` safely, sets `response.raw.decode_content = True` strictly on uncompressed HTTP, and streams via `io.TextIOWrapper(response.raw, encoding='utf-8')`.
  - Calling `open()` after `close()` reopens cleanly.
- **`read(self, size: int = -1) -> str`**, **`readline(self, size: int = -1) -> str`**, **`readlines(self, hint: int = -1) -> list`**, **`__iter__(self)`**, and **`__next__(self) -> str`**: Standard file-object read methods returning UTF-8 `str`. When `show_progress=False`, `__iter__()` returns the underlying `io.TextIOBase` C-level iterator directly. Raise `ValueError` when called on a closed reader.
- **`close(self) -> None`**: Deterministically and idempotently closes all acquired handles (`_fh`, `_raw_fh`, `_sftp_file`, `_sftp_client`, `_ssh_client`, `_ftp`, `_response`, and `_progress`) and resets state attributes, including on early iteration exit or when an individual handle close raises.

### `class PolyWriter`
Writes local and remote (`ftp://`, `ssh://`, `sftp://`) UTF-8 text streams with transparent `.gz`, `.bz2`, `.xz`, and `.zst` compression and collision-safe backup rotation.

#### Methods
- **`__init__(self, filename: Union[str, os.PathLike], show_progress: bool = False)`**: Normalizes `PathLike` inputs via `os.fsdecode()`; raises `NotImplementedError` if `show_progress=True`.
- **`open(self, append: bool = False, backup: bool = True, compresslevel: Optional[int] = None) -> 'PolyWriter'`**: Opens the target for writing or appending. Performs `.001.bu` O(1) fast-path backup rotation followed by a single directory listing (`os.scandir` locally with atomic `os.link`/`os.unlink` and `os.rename` fallback; single `listdir` on SFTP/FTP) capped at 999 sequential slots with collision-safe nanosecond timestamp fallback. Forwards optional `compresslevel` to `.zst`, `.gz`, `.bz2`, and `.xz` compressors (using library defaults when `None`: `zstd` level 3, `lzma` preset 6, `bz2` 9, `gzip` 9). FTP defaults to port 21; SSH/SFTP defaults to port 22, forwards custom URL ports, opens compressed targets in binary mode (`'wb'`/`'ab'`), and enables `set_pipelined(True)`.
- **`write(self, data: str) -> int`**, **`writelines(self, lines) -> None`**, and **`flush(self) -> None`**: Write UTF-8 text (or an iterable of strings) and flush buffered output; raise `ValueError` when called on a closed writer. When `write` is not overridden by a subclass, `open()` binds `self.write` directly to `self._fh.write` for minimal per-call overhead.
- **`close(self) -> None`**: Deterministically and idempotently closes all acquired handles (`_fh`, `_raw_fh`, `_sftp_file`, `_sftp_client`, `_ssh_client`, and `_ftp`) and resets state attributes, even if an individual handle fails to close.
