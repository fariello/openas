# Functional Specification: `polyopen`

*[Return to Main README](README.md)*

**Version:** 0.1.0
**Last Updated:** 2026-11-25

## 1. Purpose, Audience, and Scope

`polyopen` is a lightweight Python library and CLI for streaming plain and compressed files (`.gz`, `.bz2`, `.xz`, `.zst`) line-by-line or in chunks across local files and remote protocols (`http://`, `https://`, `ftp://`, `ssh://`, `sftp://`), in both UTF-8 text (`str`) and binary (`bytes`) modes, with optional sequential `.bu` overwrite backups.

- **Target audience:** Python developers, data engineers, and operators streaming local or remote compressed files (such as logs, CSV, JSONL, or binary records) without pulling in cloud object-store SDKs.
- **Explicit non-goals (v0.1.0):**
  - No cloud object-store support (`s3://`, `gs://`, `az://`, `hdfs://`).
  - No container archive manipulation (`.zip`, `.tar`, `.tar.*`, `.rar`, `.7z`).
  - No random-access `seek()`/`tell()` operations or `r+`/`w+`/`x` modes.
  - No asynchronous (`async`/`await`) I/O.
  - No HTTP/HTTPS write or append support.
  - No FTP append support (`ftputil` does not support FTP append).
  - No concurrent multi-target upload fan-out in the CLI.

## 2. Core Concepts

- **Unified Streaming Wrapper:** `polyopen()` routes text and binary read, write, and append modes to `PolyReader` or `PolyWriter`. Optional standard library and third-party modules are imported lazily on first use.
- **Line-by-Line Transparent Compression:** Uses Python's `gzip`, `bz2`, `lzma`, `zstandard`, `io.TextIOWrapper` (text mode), and `io.BufferedReader` (binary `.zst` and network reads) to stream decompressed and compressed data across chunk boundaries without buffering entire files in memory.
- **Sequential `.bu` Overwrite Backups:** On overwrite (`backup=True`, default for write modes), `polyopen` validates `compresslevel` and imports required codecs first, then checks `<path>.001.bu` in O(1); if occupied, it lists the parent directory once (`os.scandir` locally, single `listdir` on SFTP/FTP) to select the lowest free 3-digit slot up to `.999.bu`, falling back to nanosecond timestamps (`time.time_ns()`) past 999. Existing `.bu` files are never deleted or pruned automatically.

## 3. Supported Protocols and Codecs

### 3.1 Transports

- **Local (`file://` or plain path):** Read, write, and append in text and binary modes. `file://` URIs require an empty host or `localhost` (remote `file://` hosts raise `UnsupportedProtocolError`) and decode percent-encoded paths via `url2pathname`.
- **FTP (`ftp://`):** Read and write in text and binary modes via `ftputil` (`FTPSessionWrapper`, default port 21, custom ports and optional `timeout` forwarded, credentials and paths percent-decoded). Append mode (`'a'`, `'at'`, `'ab'`) raises `UnsupportedProtocolError` before connecting.
- **SSH / SFTP (`ssh://`, `sftp://`):** Read, write, and append in text and binary modes via `paramiko` (`load_system_host_keys()`, default `RejectPolicy`, default port 22, custom ports and optional `timeout` forwarded, credentials and paths percent-decoded). Reads cache `stat().st_size` once and call `prefetch(file_size, max_concurrent_requests=64)` when size is known (falling back to `prefetch(file_size)` on Paramiko `<3.3`). Writes open binary SFTP handles with `set_pipelined(True)` and wrap uncompressed streams in `_BinaryStreamAdapter` (plus `io.TextIOWrapper` in text mode).
- **HTTP / HTTPS (`http://`, `https://`):** Read-only streaming in text and binary modes via lock-initialized, thread-local, cookie-free `requests.Session` reuse (`DefaultCookiePolicy(allowed_domains=[])`) with optional `timeout` forwarding. Compressed extensions send `Accept-Encoding: identity`; uncompressed responses set `response.raw.decode_content = True`. Write/append raises `ReadOnlyProtocolError`.
- **Rejected schemes:** Cloud schemes (`s3://`, `gs://`, `az://`, `hdfs://`) and unrecognized multi-letter `scheme://` URLs raise `UnsupportedProtocolError`.

### 3.2 Compression Formats

All supported write/append codecs accept optional `compresslevel: Optional[int] = None` (boolean values raise `TypeError`; out-of-range integers raise `ValueError` before any backup is created):

1. `.bz2` (`bz2`; range 1..9; default 9)
2. `.gz` (`gzip`; range 0..9; default 9)
3. `.xz` (`lzma`; preset 0..9, optionally OR'd with `lzma.PRESET_EXTREME`; default 6)
4. `.zst` (`zstandard`; default level 3; multi-frame reads via `read_across_frames=True`)

**Container Archives Rejected:** `.zip`, `.tar`, `.tgz`, `.rar`, `.7z`, `.tar.gz`, `.tar.bz2`, `.tar.xz`, `.tar.zst`, `.tar.lzma`, `.tbz`, `.tbz2`, `.txz`, `.tzst`, `.tlz`, `.gz.tar`, `.bz2.tar`, `.xz.tar`, and `.zst.tar` raise `UnsupportedArchiveError`.

## 4. Public API

### `polyopen(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None) -> Union[PolyReader, PolyWriter]`

- **`filename`** (`Union[str, os.PathLike]`): Normalized via `os.fsdecode()`. Extension matching is case-insensitive and ignores URL query strings.
- **`mode`** (`str`):
  - `'r'`, `'rt'`: `PolyReader(..., binary=False)` returning UTF-8 `str`.
  - `'rb'`: `PolyReader(..., binary=True)` returning raw `bytes`.
  - `'w'`, `'wt'`: `PolyWriter(..., binary=False).open(append=False, ...)` accepting UTF-8 `str` (rejecting `bytes` with `TypeError`).
  - `'wb'`: `PolyWriter(..., binary=True).open(append=False, ...)` accepting `bytes`-like objects (rejecting `str` with `TypeError`).
  - `'a'`, `'at'`: `PolyWriter(..., binary=False).open(append=True, ...)` accepting UTF-8 `str`.
  - `'ab'`: `PolyWriter(..., binary=True).open(append=True, ...)` accepting `bytes`-like objects.
  - Any other mode raises `ValueError`.
- **`backup`** (`Optional[bool]`): Defaults to `None` (`True` on write, `False` on append). Passing `backup=True` with append raises `ValueError`.
- **`show_progress`** (`bool`): Enables a `tqdm` progress bar on `PolyReader` tracking raw bytes read (batched at 64 KiB, exact flush on EOF/exit/`close()`, `total=None` when stream size is unknown or `Content-Encoding` transforms plain HTTP). Passing `show_progress=True` to `PolyWriter` or write/append modes raises `NotImplementedError`.
- **`compresslevel`** (`Optional[int]`): Forwarded to write/append codecs; ignored on read.
- **`timeout`** (`Optional[float]`): Optional positive timeout in seconds forwarded to HTTP (`requests.Session.get`), SSH/SFTP (`timeout`, `banner_timeout`, `auth_timeout`), and FTP (`connect`). Non-positive or boolean values raise `ValueError`.

Both `PolyReader` and `PolyWriter` expose `binary`, `closed`, `readable()`, `writable()`, `seekable()`, `encoding` (`'utf-8'` in text mode, `None` in binary mode), credential-redacted `__repr__()`, context management, and deterministic idempotent `close()`. Operations on closed handles raise `ValueError`.

## 5. Command-Line Interface

Available via `polyopen`, `python -m polyopen`, and `python -m polyopen.polyopen` (operates in UTF-8 text mode):

- `--read` (`-r`): Requires at least one positional file; reads sequentially and prints line count, UTF-8 byte size (`B` through `YB`), throughput, and elapsed time.
- `--write` (`-w`): Pre-validates destination guards, reads source once inside `contextlib.ExitStack`, broadcasts lines synchronously to all destinations (moving existing destinations to sequential `.bu` backups), prints redacted status/summary lines, and closes all handles on error.
- `--input-file` (`-i`): Source file for `--write` mode (all positional arguments become destinations). Rejected via `parser.error` if combined with `--read` or without at least one destination.
- `--version`: Prints `polyopen <version>` and exits 0.

## 6. Edge Cases and Boundary Conditions

1. **Append and Backup Conflict:** `polyopen(path, 'a', backup=True)`, `polyopen(path, 'ab', backup=True)`, and `PolyWriter(path).open(append=True, backup=True)` raise `ValueError`.
2. **Local Backup Safety:** Existing directories raise `IsADirectoryError` rather than being renamed to `.001.bu`. Broken symlinks are detected via `os.path.lexists` and never silently overwritten on rename fallback. Local sequential backups are intended for non-concurrent writers; `os.link`/`os.unlink` prevents overwrite of existing `.bu` slots on POSIX filesystems supporting hard links.
3. **Remote Backup Semantics:** SFTP retries `rename()` collisions up to 5 times across free slots. FTP rename atomicity depends on the remote FTP server; additionally, `ftputil.file.FTPFile.close()` ignores FTP `426`/`450`/`451` transfer-abort responses.
4. **Pre-Validation Ordering:** `compresslevel` validation and optional codec imports execute before any `.bu` backup move so invalid arguments do not displace existing files.
5. **Multi-Frame Zstandard and Large Lines:** Multi-frame concatenated `.zst` streams (`read_across_frames=True`) and lines exceeding 128 KiB work consistently in both text and binary modes across local, FTP, SFTP, and HTTP transports.

## 7. Security and Resource Boundaries

1. **SSH Host-Key Verification:** `paramiko.SSHClient()` calls `load_system_host_keys()` and preserves Paramiko's default `RejectPolicy` (unknown host keys are rejected rather than auto-accepted to prevent Man-in-the-Middle attacks).
2. **Credential Redaction:** URL passwords and query parameters are masked by `_redact_url()` in CLI stdout, `__repr__()`, and `UnsupportedArchiveError` messages. Inline URL passwords in shell commands remain visible to OS process tables; prefer SSH keys (`sftp://user@host/path`).
3. **Archive Corruption Prevention:** Container archive extensions fail fast before opening streams for reading or writing.
4. **Unbounded SFTP Prefetch and Zstandard Window Caveats (v0.1.0):**
   - When SFTP file size is known, `PolyReader` calls `prefetch(file_size, max_concurrent_requests=64)`. Paramiko stores completed chunks in RAM until consumed, so slow consumers or early-exit reads on multi-GB SFTP files can buffer significant memory.
   - `ZstdDecompressor` uses `max_window_size=2**31` (2 GiB) to support `--long=31` archives. Treat untrusted `.zst` inputs with caution because a crafted header can request a large decompression window.
5. **Cleartext FTP:** `ftp://` uses standard `ftplib.FTP` (no TLS); use `sftp://` or `https://` for encrypted transfers.
6. **Network Timeouts:** `timeout` defaults to `None` for compatibility with slow bulk transfers; pass an explicit `timeout` in automated services to bound stalled connections.
