# Public API Reference: `openas`

*[Return to Main README](../README.md)*

Formal Python API reference for `openas` v0.1.0.

## Unified Entry Point

### `openas(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None) -> Union[OpenAsReader, OpenAsWriter]`

Opens a local or remote stream with transparent compression in UTF-8 text (`str`) or binary (`bytes`) mode and returns an opened `OpenAsReader` or `OpenAsWriter` context manager.

#### Parameters

- **`filename`** (`Union[str, os.PathLike]`): Target path or URL (for example `tests/data.txt`, `pathlib.Path("data.gz")`, `https://example.com/log.zst`, `ftp://user@host/path/to/data.txt`, or `sftp://user@host/path/to/syslog.zst`). Targets are treated as URLs only when they start with `scheme://`; single-colon local filenames such as `file:report.txt` remain local paths. Query strings are ignored when detecting extensions, extension matching is case-insensitive, FTP/SSH/SFTP credentials and paths are percent-decoded, and network URL paths are percent-decoded before extension and archive checks (so `https://example.com/data%2Egz` is read as `.gz` and `https://example.com/archive%2Ezip` raises `UnsupportedArchiveError`). `file://` URIs require an empty host or `localhost`.
- **`mode`** (`str`, default `'r'`):
  - Text read (`'r'`, `'rt'`): opens `OpenAsReader(..., binary=False)` returning UTF-8 `str`.
  - Binary read (`'rb'`): opens `OpenAsReader(..., binary=True)` returning raw `bytes`.
  - Text write / truncate (`'w'`, `'wt'`): opens `OpenAsWriter(..., binary=False).open(append=False, ...)` accepting UTF-8 `str`.
  - Binary write / truncate (`'wb'`): opens `OpenAsWriter(..., binary=True).open(append=False, ...)` accepting `bytes`-like objects.
  - Text append (`'a'`, `'at'`): opens `OpenAsWriter(..., binary=False).open(append=True, ...)` accepting UTF-8 `str`.
  - Binary append (`'ab'`): opens `OpenAsWriter(..., binary=True).open(append=True, ...)` accepting `bytes`-like objects.
  - Any other mode raises `ValueError`.
  - **Text newline semantics (`newline=None`):** All text modes use Python's default `newline=None` across both local and remote streams. Reads apply universal newline translation (converting `\r\n`, `\r`, and `\n` to `\n`), and writes translate `\n` to `os.linesep` (`\n` on POSIX, `\r\n` on Windows). Use binary modes (`'rb'`, `'wb'`, `'ab'`) when you need exact wire bytes or guaranteed LF (`\n`) preservation across platforms.
- **`backup`** (`Optional[bool]`, default `None`): Moves an existing target to a sequential `.bu` backup before overwriting locally or over SSH/SFTP/FTP. Checks `<path>.001.bu` first in O(1); if occupied, lists the parent directory once (`os.scandir` locally, single `listdir` on SFTP/FTP) to pick the lowest free slot (`.001.bu`..`.999.bu`), falling back to nanosecond timestamps (`time.time_ns()`) past 999. Resolves to `True` in write modes and `False` in append modes. Passing `backup=True` with append raises `ValueError`.
- **`show_progress`** (`bool`, default `False`): Enables a `tqdm` progress bar on `OpenAsReader` tracking exact raw bytes read (batched at 64 KiB with exact remainder flush on EOF, early exit, and `close()`, falling back to `total=None` when stream size is unavailable). For uncompressed local text files with CRLF (`\r\n`) or CR (`\r`) line endings, universal newline translation strips `\r` bytes from yielded strings; at natural EOF, `OpenAsReader` reconciles the progress bar to `total` (`_reconcile_crlf_eof_progress()`) so the bar reaches 100%. Passing `show_progress=True` in write/append modes or to `OpenAsWriter` raises `NotImplementedError`.
- **`compresslevel`** (`Optional[int]`, default `None`): Optional compression level or preset forwarded to write/append codecs (ignored on read). Validated by `_validate_write_options()` before creating any `.bu` backup (`bool` or non-`int` values raise `TypeError`; out-of-range integers raise `ValueError`). Accepted ranges and defaults when `None`:
  - `.gz`: integers `0` through `9` (default `9`).
  - `.bz2`: integers `1` through `9` (default `9`).
  - `.xz`: integers `0` through `9`, optionally bitwise-OR'd with `lzma.PRESET_EXTREME` (default `6`).
  - `.zst`: integers up to `zstandard.MAX_COMPRESSION_LEVEL` (`22`, including negative fast levels), validated via `zstandard.ZstdCompressor(level=compresslevel)` (default `3`).
  - Plain (uncompressed) files: type-checked as `int` (rejecting `bool` and non-integers with `TypeError`), then ignored.
- **`timeout`** (`Optional[float]`, default `None`): Optional positive timeout in seconds forwarded to HTTP (`requests.Session.get`), SSH/SFTP (`timeout`, `banner_timeout`, `auth_timeout`), and FTP (`connect`). Non-positive or boolean values raise `ValueError`.

#### Remote URL Paths, Authentication, and Environment Variables

- **SSH / SFTP (`ssh://`, `sftp://`):** `urllib.parse.urlparse(url).path` (percent-decoded via `urllib.parse.unquote()`) is passed directly as an absolute remote path (for example `sftp://user@host/path/to/syslog.zst` opens `/path/to/syslog.zst`). Tilde (`~/`) home-directory expansion is not performed. Paramiko connects directly via `SSHClient.connect()` and does not read `~/.ssh/config` host aliases or options. When the URL omits a username, Paramiko defaults to the local OS user and authenticates via the SSH agent or default `~/.ssh/id_*` keys.
- **FTP (`ftp://`):** The percent-decoded `urlparse(url).path` is passed directly to `ftputil.FTPHost.open()`. When the URL omits credentials, `ftplib.FTP.login(None, None)` logs in anonymously.
- **Environment variables:** `openas` defines no custom environment variables. HTTP/HTTPS requests reuse `requests.Session()` with default `trust_env=True`, so HTTP/HTTPS honors standard `requests` environment settings (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, and `~/.netrc` credentials).

#### Exceptions

All custom exceptions inherit from `OpenAsError`:

- `UnsupportedArchiveError`: Raised when the normalized path matches a container archive extension (`.zip`, `.tar`, `.tgz`, `.rar`, `.7z`, `.tar.gz`, `.tar.bz2`, `.tar.xz`, `.tar.zst`, `.tar.lzma`, `.tbz`, `.tbz2`, `.txz`, `.tzst`, `.tlz`, `.gz.tar`, `.bz2.tar`, `.xz.tar`, `.zst.tar`), including URLs with query strings. URL credentials and query strings are redacted in the error message.
- `ReadOnlyProtocolError`: Raised when writing or appending to `http://` or `https://`.
- `UnsupportedProtocolError`: Raised for cloud object stores (`s3://`, `gs://`, `az://`, `hdfs://`), unknown `scheme://` URLs, non-localhost `file://` hosts, and FTP append mode.
- Standard exceptions: `FileNotFoundError` for missing files/SFTP targets, `IsADirectoryError` when attempting a local, SFTP, or FTP `.bu` backup on an existing directory, `EOFError` when reading truncated `.zst` streams across local, HTTP, FTP, and SFTP transports (detected by `_ZstdFrameValidatingStream` before frame completion), `TypeError` when writing `bytes` to a text writer, writing `str` to a binary writer, or passing a non-`int` or `bool` `compresslevel`, `requests.HTTPError` for HTTP 4xx/5xx responses (response closed before re-raising), and `ValueError` for invalid modes, missing URL hostnames, `append=True` + `backup=True`, invalid `timeout`, out-of-range `compresslevel`, or operations on closed handles.

---

## Direct Handler Classes

### `class OpenAsReader`

Reads local and remote (`ftp://`, `ssh://`, `sftp://`, `http://`, `https://`) streams line-by-line or in chunks with transparent `.gz`, `.bz2`, `.xz`, and multi-frame `.zst` decompression. Operates on UTF-8 `str` by default, or raw `bytes` when `binary=True`.

#### Properties and Methods

- **`__init__(self, filename: Union[str, os.PathLike], show_progress: bool = False, timeout: Optional[float] = None, binary: bool = False)`**: Normalizes `PathLike` inputs via `os.fsdecode()` and validates `timeout`.
- **`binary` (`bool`)**, **`closed` (`bool`)**, **`encoding` (`'utf-8'` in text mode, `None` in binary mode)**, **`readable() -> True`**, **`writable() -> False`**, **`seekable() -> False`**, **`__repr__() -> str`** (includes `mode` and redacts URL credentials/queries).
- **`open(self) -> OpenAsReader`**: Opens the transport and codec stream:
  - FTP defaults to port 21, forwards custom ports and `timeout`, and wraps `ftputil.file.FTPFile` in an `io.BufferedIOBase` adapter.
  - SSH/SFTP defaults to port 22, verifies system host keys (`load_system_host_keys()` + `RejectPolicy`), caches `stat().st_size` once, and wraps `SFTPFile` in a bounded 4 MiB sliding-window `readv()` stream (`MAX_REQUEST_SIZE` sub-chunks, `max_concurrent_prefetch_requests=64`, Paramiko `<3.3` fallback) when `readv` is available and size is positive, guaranteeing O(1) memory on multi-GB files. When `readv` is absent or size is unknown or non-positive, it falls back directly to sequential `sftp_file.read()`.
  - HTTP/HTTPS reuses a lock-initialized, thread-local, cookie-free `requests.Session`, sends `Accept-Encoding: identity` for compressed extensions, validates status via `raise_for_status()`, enables `response.raw.decode_content = True` strictly on uncompressed responses, and sets `total=None` if `Content-Encoding` is set on uncompressed responses.
  - Binary compressed readers (`.gz`, `.bz2`, `.xz`) and plain binary network readers are wrapped in `io.BufferedReader(..., buffer_size=65536)` for fast C-level line scanning across chunk boundaries; `.zst` readers wrap the raw binary stream in `_ZstdFrameValidatingStream` (zero-copy RFC 8878 frame validation raising `EOFError` on truncated streams) before `ZstdDecompressor.stream_reader(..., read_across_frames=True, read_size=131072)` and `io.BufferedReader(..., buffer_size=131072)` in both text and binary modes.
  - Reopening after `close()` works cleanly.
- **`read(size=-1) -> Union[str, bytes]`**, **`readline(size=-1) -> Union[str, bytes]`**, **`readlines(hint=-1) -> list`**, **`__iter__()`**, **`__next__() -> Union[str, bytes]`**: Standard stream read operations (`str` when `binary=False`, `bytes` when `binary=True`). Raise `ValueError` on a closed reader. `readlines(hint=-1)` loads all remaining lines into a list when `hint` is `None` or `<= 0`, or stops once cumulative line length reaches or exceeds `hint` when `hint > 0` (delegating directly to C-level `self._fh.readlines()` for non-progress and `_CountingByteStream` streams). On multi-GB files, iterate line-by-line (`for line in reader:`) or read bounded chunks (`reader.read(65536)`) instead of calling unbounded `readlines()` or `read()` to stay in O(1) memory.
- **`close() -> None`**: Deterministically and idempotently closes all acquired handles and resets state, even if one handle raises.

### `class OpenAsWriter`

Writes local and remote (`ftp://`, `ssh://`, `sftp://`) streams with transparent `.gz`, `.bz2`, `.xz`, and `.zst` compression and optional sequential `.bu` backups. Operates on UTF-8 `str` by default, or raw `bytes`-like objects when `binary=True`.

> **Context-manager options note:** `with OpenAsWriter(path) as w:` calls `open()` with defaults (`append=False, backup=True, compresslevel=None`). To pass custom options with a `with` statement, chain `with OpenAsWriter(path).open(append=..., backup=..., compresslevel=...) as w:` or use `openas(...)`.

#### Properties and Methods

- **`__init__(self, filename: Union[str, os.PathLike], show_progress: bool = False, timeout: Optional[float] = None, binary: bool = False)`**: Normalizes `PathLike` inputs; raises `NotImplementedError` if `show_progress=True`; validates `timeout`.
- **`binary` (`bool`)**, **`closed` (`bool`)**, **`encoding` (`'utf-8'` in text mode, `None` in binary mode)**, **`readable() -> False`**, **`writable() -> True`**, **`seekable() -> False`**, **`__repr__() -> str`** (includes `mode` and redacts URL credentials/queries).
- **`open(self, append: bool = False, backup: bool = True, compresslevel: Optional[int] = None) -> OpenAsWriter`**: Validates `compresslevel` and imports required codecs before moving an existing file to a `.bu` backup or opening the target. Local, SFTP, and FTP `.bu` backups reject existing directories with `IsADirectoryError` without renaming them. Local backups use atomic `os.link`/`os.unlink`. SFTP opens binary mode (`'wb'`/`'ab'`) with `set_pipelined(True)` and wraps uncompressed writes in `_BinaryStreamAdapter` (plus `io.TextIOWrapper` in text mode) so `write()` returns `int`; SFTP backup retries rename collisions. FTP rejects `append=True` with `UnsupportedProtocolError`.
- **`write(data) -> int`**, **`writelines(lines) -> None`**, **`flush() -> None`**: Write data and flush buffered output; raise `ValueError` when closed. In text mode (`binary=False`), `write(data)` accepts UTF-8 `str` and returns the number of characters written, and `writelines(lines)` accepts an iterable of `str`. In binary mode (`binary=True`), `write(data)` accepts `bytes`-like objects and returns the number of bytes written, and `writelines(lines)` accepts an iterable of `bytes`-like objects (calling `self._fh.write(line)` per item so `ZstdCompressionWriter` matches stdlib binary writers). Neither `writelines()` mode inserts line separators automatically. When `write` is not overridden by a subclass, `open()` binds `self.write` directly to `self._fh.write`.
- **`close() -> None`**: Deterministically and idempotently closes all acquired handles and resets state.

---

## Internal Compatibility Export

- **`FTPSessionWrapper`**: Internal `ftplib.FTP` subclass used as the `ftputil` session factory. It remains lazily accessible via PEP 562 module `__getattr__` for backward compatibility in v0.1.0, is not intended for direct end-user use, and is scheduled for removal from `__all__` in v0.2.
