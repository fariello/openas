# Public API Reference: `polyopen`

*[Return to Main README](../README.md)*

Formal Python API reference for `polyopen` v0.1.0.

## Unified Entry Point

### `polyopen(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None) -> Union[PolyReader, PolyWriter]`

Opens a local or remote UTF-8 text stream with transparent compression and returns an opened `PolyReader` or `PolyWriter` context manager.

> **UTF-8 text only:** All modes operate on UTF-8 `str`. Modes `'rb'`, `'wb'`, and `'ab'` are accepted as compatibility aliases for text modes; they do not return or accept raw `bytes`.

#### Parameters

- **`filename`** (`Union[str, os.PathLike]`): Target path or URL (for example `tests/data.txt`, `pathlib.Path("data.gz")`, `https://example.com/log.zst`, `ftp://user@host/data.txt`, or `sftp://user@host/syslog.zst`). Query strings are ignored when detecting extensions, extension matching is case-insensitive, and FTP/SSH/SFTP credentials and paths are percent-decoded. `file://` URIs require an empty host or `localhost`.
- **`mode`** (`str`, default `'r'`):
  - Read (`'r'`, `'rt'`, `'rb'`): opens `PolyReader`.
  - Write / truncate (`'w'`, `'wt'`, `'wb'`): opens `PolyWriter(append=False)`.
  - Append (`'a'`, `'at'`, `'ab'`): opens `PolyWriter(append=True)`.
  - Any other mode raises `ValueError`.
- **`backup`** (`Optional[bool]`, default `None`): Rotates an existing file before overwriting locally or over SSH/SFTP/FTP. Checks `<path>.001.bu` first in O(1); if occupied, lists the parent directory once (`os.scandir` locally, single `listdir` on SFTP/FTP) to pick the lowest free slot (`.001.bu`..`.999.bu`), falling back to nanosecond timestamps (`time.time_ns()`) past 999. Resolves to `True` in write modes and `False` in append modes. Passing `backup=True` with append raises `ValueError`.
- **`show_progress`** (`bool`, default `False`): Enables a `tqdm` progress bar on `PolyReader` tracking exact bytes read (batched at 64 KiB with exact remainder flush on EOF, early exit, and `close()`, falling back to `total=None` when stream size is unavailable). Passing `show_progress=True` in write/append modes or to `PolyWriter` raises `NotImplementedError`.
- **`compresslevel`** (`Optional[int]`, default `None`): Optional compression level/preset forwarded to write/append codecs (ignored on read). Validated before backup rotation (booleans raise `TypeError`; out-of-range integers raise `ValueError`). Defaults when `None`: `zstd` level 3, `lzma` preset 6, `bz2` level 9, `gzip` level 9.
- **`timeout`** (`Optional[float]`, default `None`): Optional positive timeout in seconds forwarded to HTTP (`requests.Session.get`), SSH/SFTP (`timeout`, `banner_timeout`, `auth_timeout`), and FTP (`connect`). Non-positive or boolean values raise `ValueError`.

#### Exceptions

All custom exceptions inherit from `PolyopenError`:

- `UnsupportedArchiveError`: Raised when the normalized path matches a container archive extension (`.zip`, `.tar`, `.tgz`, `.rar`, `.7z`, `.tar.gz`, `.tar.bz2`, `.tar.xz`, `.tar.zst`, `.tar.lzma`, `.tbz`, `.tbz2`, `.txz`, `.tzst`, `.tlz`, `.gz.tar`, `.bz2.tar`, `.xz.tar`, `.zst.tar`), including URLs with query strings. URL credentials and query strings are redacted in the error message.
- `ReadOnlyProtocolError`: Raised when writing or appending to `http://` or `https://`.
- `UnsupportedProtocolError`: Raised for cloud object stores (`s3://`, `gs://`, `az://`, `hdfs://`), unknown `scheme://` URLs, non-localhost `file://` hosts, and FTP append mode.
- Standard exceptions: `FileNotFoundError` for missing files/SFTP targets, `IsADirectoryError` when attempting local backup rotation on an existing directory, `requests.HTTPError` for HTTP 4xx/5xx responses (response closed before re-raising), and `ValueError` for invalid modes, missing URL hostnames, `append=True` + `backup=True`, invalid `timeout`, or operations on closed handles.

---

## Direct Handler Classes

### `class PolyReader`

Reads local and remote (`ftp://`, `ssh://`, `sftp://`, `http://`, `https://`) UTF-8 text streams with transparent `.gz`, `.bz2`, `.xz`, and multi-frame `.zst` decompression.

#### Properties and Methods

- **`__init__(self, filename: Union[str, os.PathLike], show_progress: bool = False, timeout: Optional[float] = None)`**: Normalizes `PathLike` inputs via `os.fsdecode()` and validates `timeout`.
- **`closed` (`bool`)**, **`encoding` (`'utf-8'`)**, **`readable() -> True`**, **`writable() -> False`**, **`seekable() -> False`**, **`__repr__() -> str`** (with URL credentials/queries redacted).
- **`open(self) -> PolyReader`**: Opens the transport and codec stream:
  - FTP defaults to port 21, forwards custom ports and `timeout`, and wraps `ftputil.file.FTPFile` in an `io.BufferedIOBase` adapter.
  - SSH/SFTP defaults to port 22, verifies system host keys (`load_system_host_keys()` + `RejectPolicy`), caches `stat().st_size` once, and invokes `prefetch(file_size, max_concurrent_requests=64)` when size is known (Paramiko `<3.3` falls back to `prefetch(file_size)`). Note: Paramiko buffers prefetched chunks in RAM until read.
  - HTTP/HTTPS reuses a lock-initialized, thread-local, cookie-free `requests.Session`, sends `Accept-Encoding: identity` for compressed extensions, validates status via `raise_for_status()`, enables `response.raw.decode_content = True` strictly on uncompressed responses, and sets `total=None` if `Content-Encoding` is set on uncompressed responses.
  - Reopening after `close()` works cleanly.
- **`read(size=-1) -> str`**, **`readline(size=-1) -> str`**, **`readlines(hint=-1) -> list`**, **`__iter__()`**, **`__next__() -> str`**: Standard text read operations. Raise `ValueError` on a closed reader.
- **`close() -> None`**: Deterministically and idempotently closes all acquired handles and resets state, even if one handle raises.

### `class PolyWriter`

Writes local and remote (`ftp://`, `ssh://`, `sftp://`) UTF-8 text streams with transparent `.gz`, `.bz2`, `.xz`, and `.zst` compression and backup rotation.

> **Context-manager options note:** `with PolyWriter(path) as w:` calls `open()` with defaults (`append=False, backup=True, compresslevel=None`). To pass custom options with a `with` statement, chain `with PolyWriter(path).open(append=..., backup=..., compresslevel=...) as w:` or use `polyopen(...)`.

#### Properties and Methods

- **`__init__(self, filename: Union[str, os.PathLike], show_progress: bool = False, timeout: Optional[float] = None)`**: Normalizes `PathLike` inputs; raises `NotImplementedError` if `show_progress=True`; validates `timeout`.
- **`closed` (`bool`)**, **`encoding` (`'utf-8'`)**, **`readable() -> False`**, **`writable() -> True`**, **`seekable() -> False`**, **`__repr__() -> str`** (with URL credentials/queries redacted).
- **`open(self, append: bool = False, backup: bool = True, compresslevel: Optional[int] = None) -> PolyWriter`**: Validates `compresslevel` and imports required codecs before rotating backups or opening the target. Local backups reject existing directories (`IsADirectoryError`) and use atomic `os.link`/`os.unlink`. SFTP opens binary mode (`'wb'`/`'ab'`) with `set_pipelined(True)` and wraps uncompressed writes in UTF-8 `io.TextIOWrapper` so `write()` returns `int`; SFTP backup retries rename collisions. FTP rejects `append=True` with `UnsupportedProtocolError`.
- **`write(data: str) -> int`**, **`writelines(lines) -> None`**, **`flush() -> None`**: Write UTF-8 text and flush buffered output; raise `ValueError` when closed. When `write` is not overridden by a subclass, `open()` binds `self.write` directly to `self._fh.write`.
- **`close() -> None`**: Deterministically and idempotently closes all acquired handles and resets state.

---

## Internal Compatibility Export

- **`FTPSessionWrapper`**: Internal `ftplib.FTP` subclass used as the `ftputil` session factory. It remains lazily accessible via PEP 562 module `__getattr__` for backward compatibility in v0.1.0, is not intended for direct end-user use, and is scheduled for removal from `__all__` in v0.2.
