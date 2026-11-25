# System Architecture

*[Return to Main README](README.md)*

`polyopen` exposes a uniform UTF-8 `io.TextIOWrapper`-backed streaming interface over local files, remote network transports, and transparent stream compressors while keeping optional dependencies lazily imported.

## Stream Layering

```text
Caller (polyopen() / PolyReader / PolyWriter / CLI)
  |
  v
Target Resolution & Guards (_resolve_target, _validate_write_options)
  |-- rejects container archives (.zip, .tar.*, .7z, .rar, ...)
  |-- rejects cloud schemes (s3://, gs://, az://, hdfs://) and unknown scheme://
  |-- rejects HTTP/HTTPS writes and FTP append
  |-- validates compresslevel & imports codec before backup rotation
  v
Transport Layer (_wrap_local, _wrap_http, _wrap_ftp, _wrap_ssh)
  |-- Local: plain file / atomic os.link+os.unlink backup rotation
  |-- HTTP/HTTPS: thread-local cookie-free requests.Session + _HttpRawStream
  |-- FTP: ftputil.FTPHost + FTPSessionWrapper + _BinaryStreamAdapter
  |-- SSH/SFTP: paramiko.SSHClient (system host keys) + SFTPFile + _BinaryStreamAdapter
  v
Optional Raw-Byte Counter (_CountingByteStream, when show_progress=True)
  v
Codec Layer (.gz, .bz2, .xz, .zst multi-frame, or plain UTF-8)
  v
UTF-8 Text Interface (io.TextIOWrapper / io.TextIOBase)
```

## Core Components

### 1. Routing Facade (`polyopen()`)

`polyopen()` maps modes to `PolyReader` (`'r'`, `'rt'`, `'rb'`) or `PolyWriter` (`'w'`, `'wt'`, `'wb'` with `append=False`; `'a'`, `'at'`, `'ab'` with `append=True`), forwarding `backup`, `show_progress`, optional `compresslevel`, and optional `timeout`. All modes operate on UTF-8 text (`str`).

Optional standard library modules (`argparse`, `atexit`, `bz2`, `ftplib`, `gzip`, `lzma`, `urllib.parse`, `urllib.request`) and third-party packages (`paramiko`, `ftputil`, `requests`, `zstandard`, `tqdm`) are imported lazily on first use. `FTPSessionWrapper` is exposed lazily via PEP 562 module `__getattr__`.

### 2. Target Resolution and Safety Guards

`_resolve_target()` skips URL parsing for plain local paths without `:` and parses URLs on demand:

- `file://` URIs are accepted only with an empty host or `localhost` and converted via `urllib.request.url2pathname` (decoding percent-encoded paths).
- Cloud object store schemes (`s3://`, `gs://`, `az://`, `hdfs://`) and unrecognized multi-letter `scheme://` URLs raise `UnsupportedProtocolError`.
- Supported remote URLs (`http`, `https`, `ftp`, `ssh`, `sftp`) require a non-empty hostname; FTP and SSH/SFTP credentials and paths are percent-decoded via `urllib.parse.unquote`.
- Extension checks match case-insensitively on the URL path (ignoring query strings) and reject container archives (`UNSUPPORTED_ARCHIVES`) with `UnsupportedArchiveError`.
- `_redact_url()` masks URL passwords and query parameters before printing in CLI output, `__repr__()`, or archive error messages.

### 3. Transport Wrappers

- **Local (`_wrap_local`, `_backup_local_file`):** Validates `compresslevel` and imports optional codecs before touching disk. Backup rotation checks `<path>.001.bu` first in O(1); if occupied, it scans the parent directory once via `os.scandir` to find the lowest free 3-digit ASCII slot up to `.999.bu`, then falls back to nanosecond timestamps (`time.time_ns()`). Existing directories raise `IsADirectoryError` rather than being renamed. Moves use atomic `os.link()` + `os.unlink()`, falling back to `os.rename()` (only when the destination does not exist per `os.path.lexists`) on filesystems that do not support hard links.
- **SSH / SFTP (`_wrap_ssh`, `_ssh_backup_if`):** Uses `paramiko.SSHClient()` with `load_system_host_keys()` and Paramiko's default `RejectPolicy`. Assigns `self._ssh_client` before `connect()` so host-key or authentication failures close the transport thread cleanly. Reads record `stat().st_size` once in `_sftp_file_size` and invoke `prefetch(file_size, max_concurrent_requests=64)` when size is known (falling back to `prefetch(file_size)` on Paramiko `<3.3`). Writes open SFTP files in binary mode (`'wb'`/`'ab'`), enable `set_pipelined(True)`, wrap uncompressed streams in `io.TextIOWrapper(_BinaryStreamAdapter(...), encoding='utf-8')` so `write()` consistently returns `int`, and retry backup `rename()` collisions.
- **FTP (`_wrap_ftp`, `FTPSessionWrapper`):** Uses `ftputil.FTPHost` with `FTPSessionWrapper` (default port 21, custom ports and optional `timeout` forwarded, socket closed if `login()` fails). Wraps `ftputil.file.FTPFile` in `_BinaryStreamAdapter(io.BufferedIOBase)` so `io.TextIOWrapper`, `bz2.open`, and `lzma.open` work reliably on real FTP handles. FTP append mode is rejected up front with `UnsupportedProtocolError`.
- **HTTP / HTTPS (`_wrap_http`, `_get_http_session`):** Reuses a lock-initialized, thread-local `requests.Session` with cookie persistence disabled (`DefaultCookiePolicy(allowed_domains=[])`) and `atexit` cleanup. Sends `Accept-Encoding: identity` on compressed extensions so servers do not double-encode compressed bodies. On uncompressed responses, enables `response.raw.decode_content = True`, streams through `_HttpRawStream(response.raw)`, and sets `_http_file_size = None` when `Content-Encoding` is present so progress bars do not compare compressed `Content-Length` against decompressed bytes.

### 4. Hot Paths, Progress Accounting, Handle Teardown, and CLI

- **Iteration and Write Hot Paths:** When `show_progress=False`, `PolyReader.__iter__()` returns the underlying `io.TextIOBase` C-level iterator directly. When `PolyWriter.write` is not overridden by a subclass, `PolyWriter.open()` binds `self.write` directly to `self._fh.write` (removed on `close()`).
- **Progress Accounting:** Compressed and remote streams wrap their binary handle in `_CountingByteStream`; `_iter_progress()` batches `tqdm` updates in 64 KiB increments, stays synchronized with `self._progress_pending`/`self._progress_raw_bytes` across `yield`, and flushes exact remainders on EOF, early exit, or `close()`. Uncompressed local reads count UTF-8 byte length (`len(line.encode('utf-8'))`). When file size is unavailable or `stat()`/`getsize()` fails, `tqdm` uses `total=None`.
- **Deterministic Teardown:** `close()` snapshots and clears all handle attributes before invoking `_close_handles()`, ensuring idempotent cleanup even when one handle fails to close. Mid-`open()` cleanup catches `BaseException` so interrupts (`KeyboardInterrupt`) do not leak sockets or threads.
- **CLI (`main()`, `src/polyopen/__main__.py`):** Exposed via `polyopen`, `python -m polyopen`, and `python -m polyopen.polyopen`. `--write` pre-validates destination guards before opening streams, reads the source once inside `contextlib.ExitStack`, and fans lines out synchronously to all destinations.

## Test and Benchmark Layout

- **Unit tests (`tests/unit/`):**
  - `test_local_io.py`: Local codecs, chunking, backups, progress accounting, `file://` URIs, lifecycle, lazy imports, and file-like attributes.
  - `test_routing.py`: Mocked FTP/SFTP/HTTP routing, real `FTPFile`-contract tests, `FTPSessionWrapper`, percent-decoding, timeouts, and an ephemeral local `ThreadingHTTPServer` for cookie-free and gzip HTTP streaming.
  - `test_guards.py`: Archive guards, protocol guards, invalid modes, append/backup conflicts, and text/binary alias contracts.
- **Regression tests (`tests/regression/`):**
  - `test_golden.py`: Reads and round-trip verifies `tests/fixtures/gold.txt`, `gold.txt.gz`, `gold.txt.bz2`, `gold.txt.xz`, and `gold.txt.zst` against independent stdlib and `zstandard` decoders.
  - `test_cli.py`: CLI flags, single-pass broadcast, error exits, credential redaction, and checkout `src/` import verification.
- **Benchmarks (`benchmarks/bench_polyopen.py`):** Standalone informational suite measuring import latency, 200k-line read/write throughput, compression peak RSS, local HTTP throughput, 500-backup rotation latency, and 3-target CLI broadcast.
