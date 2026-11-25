# System Architecture

*[Return to Main README](README.md)*

`polyopen` exposes a uniform line-oriented and chunk-oriented streaming interface in both UTF-8 text (`str`) and binary (`bytes`) modes over local files, remote network transports, and transparent stream compressors while keeping optional dependencies lazily imported.

## Stream Layering

```text
Caller (polyopen() / PolyReader / PolyWriter / CLI)
  |
  v
Target Resolution & Guards (_resolve_target, _validate_write_options)
  |-- rejects container archives (.zip, .tar.*, .7z, .rar, ...)
  |-- rejects cloud schemes (s3://, gs://, az://, hdfs://) and unknown scheme://
  |-- rejects HTTP/HTTPS writes and FTP append
  |-- validates compresslevel & imports codec before moving existing files to .bu backups
  v
Transport Layer (_wrap_local, _wrap_http, _wrap_ftp, _wrap_ssh)
  |-- Local: plain file / atomic os.link+os.unlink sequential .bu backups
  |-- HTTP/HTTPS: thread-local cookie-free requests.Session + _HttpRawStream
  |-- FTP: ftputil.FTPHost + FTPSessionWrapper + _BinaryStreamAdapter
  |-- SSH/SFTP: paramiko.SSHClient (system host keys) + _BoundedSFTPReadStream (4 MiB readv windows) / SFTPFile + _BinaryStreamAdapter
  v
Optional Raw-Byte Counter (_CountingByteStream: 64 KiB batched tqdm.update when show_progress=True)
  v
Codec Layer (.gz, .bz2, .xz, .zst multi-frame with 128 KiB read_size, or uncompressed)
  v
Mode Layer:
  |-- Text mode (default): io.TextIOWrapper / io.TextIOBase (UTF-8 str; .zst buffered at 128 KiB)
  |-- Binary mode (rb/wb/ab, binary=True): io.BufferedReader (64 KiB / 128 KiB for .zst) / binary codec / _BinaryStreamAdapter (raw bytes)
```

## Core Components

### 1. Routing Facade (`polyopen()`)

`polyopen()` maps modes to `PolyReader` (`'r'`/`'rt'` with `binary=False`; `'rb'` with `binary=True`) or `PolyWriter` (`'w'`/`'wt'` with `append=False, binary=False`; `'wb'` with `append=False, binary=True`; `'a'`/`'at'` with `append=True, binary=False`; `'ab'` with `append=True, binary=True`), forwarding `backup`, `show_progress`, optional `compresslevel`, and optional `timeout`.

Optional standard library modules (`argparse`, `atexit`, `bz2`, `ftplib`, `gzip`, `lzma`, `urllib.parse`, `urllib.request`) and third-party packages (`paramiko`, `ftputil`, `requests`, `zstandard`, `tqdm`) are imported lazily on first use. `FTPSessionWrapper` is exposed lazily via PEP 562 module `__getattr__`.

### 2. Target Resolution and Safety Guards

`_resolve_target()` skips URL parsing for plain local paths without `:` and parses URLs on demand:

- `file://` URIs are accepted only with an empty host or `localhost` and converted via `urllib.request.url2pathname` (decoding percent-encoded paths).
- Cloud object store schemes (`s3://`, `gs://`, `az://`, `hdfs://`) and unrecognized multi-letter `scheme://` URLs raise `UnsupportedProtocolError`.
- Supported remote URLs (`http`, `https`, `ftp`, `ssh`, `sftp`) require a non-empty hostname; FTP and SSH/SFTP credentials and paths are percent-decoded via `urllib.parse.unquote`.
- Extension checks match case-insensitively on the URL path (ignoring query strings) and reject container archives (`UNSUPPORTED_ARCHIVES`) with `UnsupportedArchiveError`.
- `_redact_url()` masks URL passwords and query parameters before printing in CLI output, `__repr__()`, or archive error messages.

### 3. Transport Wrappers

- **Local (`_wrap_local`, `_backup_local_file`):** Validates `compresslevel` and imports optional codecs before touching disk. Sequential `.bu` backup handling checks `<path>.001.bu` first in O(1); if occupied, it scans the parent directory once via `os.scandir` to find the lowest free 3-digit ASCII slot up to `.999.bu`, then falls back to nanosecond timestamps (`time.time_ns()`). Existing directories raise `IsADirectoryError` rather than being renamed. Moves use atomic `os.link()` + `os.unlink()`, falling back to `os.rename()` (only when the destination does not exist per `os.path.lexists`) on filesystems that do not support hard links.
- **SSH / SFTP (`_wrap_ssh`, `_BoundedSFTPReadStream`, `_ssh_backup_if`):** Uses `paramiko.SSHClient()` with `load_system_host_keys()` and Paramiko's default `RejectPolicy`. Assigns `self._ssh_client` before `connect()` so host-key or authentication failures close the transport thread cleanly. Reads record `stat().st_size` once in `_sftp_file_size`. When `file_size > 0` and `sftp_file.readv` is available, `_wrap_ssh()` wraps `SFTPFile` in `_BoundedSFTPReadStream` (stored on `self._raw_fh` for deterministic teardown under codecs like `GzipFile` that do not close wrapped `fileobj` handles), requesting 4 MiB sliding windows split into `MAX_REQUEST_SIZE` sub-chunks via `readv(sub_chunks, max_concurrent_prefetch_requests=64)` (with Paramiko `<3.3` `TypeError` fallback and early-EOF stop on truncated files). When `readv` is absent or `file_size` is unknown or non-positive, reads fall back to sequential `sftp_file.read()` (wrapped in `_BinaryStreamAdapter` + `io.BufferedReader` for plain binary mode). Writes open SFTP files in binary mode (`'wb'`/`'ab'`), enable `set_pipelined(True)`, wrap uncompressed streams in `_BinaryStreamAdapter` (and `io.TextIOWrapper` in text mode) so `write()` consistently returns `int`, and retry backup `rename()` collisions.
- **FTP (`_wrap_ftp`, `FTPSessionWrapper`):** Uses `ftputil.FTPHost` with `FTPSessionWrapper` (default port 21, custom ports and optional `timeout` forwarded, socket closed if `login()` fails). Wraps `ftputil.file.FTPFile` in `_BinaryStreamAdapter(io.BufferedIOBase)` so `io.TextIOWrapper`, `io.BufferedReader`, `bz2.open`, `lzma.open`, and direct binary writes work reliably on real FTP handles. FTP append mode is rejected up front with `UnsupportedProtocolError`.
- **HTTP / HTTPS (`_wrap_http`, `_get_http_session`):** Reuses a lock-initialized, thread-local `requests.Session` with cookie persistence disabled (`DefaultCookiePolicy(allowed_domains=[])`) and `atexit` cleanup. Sends `Accept-Encoding: identity` on compressed extensions so servers do not double-encode compressed bodies. On uncompressed responses, enables `response.raw.decode_content = True`, streams through `_HttpRawStream(response.raw)` into `io.TextIOWrapper` (text) or `io.BufferedReader` (binary), and sets `_http_file_size = None` when `Content-Encoding` is present so progress bars do not compare compressed `Content-Length` against decompressed bytes.

### 4. Hot Paths, Progress Accounting, Handle Teardown, and CLI

- **Iteration, `readlines()`, and Write Hot Paths:** `PolyReader.__iter__()` returns the underlying C-level iterator (`io.TextIOBase` in text mode, or `io.BufferedReader` in binary mode) directly when `show_progress=False` or when `_CountingByteStream` owns progress updates (all compressed and remote streams). Binary `.gz`, `.bz2`, `.xz`, and plain network readers wrap their handle in `io.BufferedReader(..., buffer_size=65536)` for C-level newline scanning; `.zst` readers use `stream_reader(..., read_across_frames=True, read_size=131072)` wrapped in `io.BufferedReader(..., buffer_size=131072)` in both text and binary modes. `PolyReader.readlines(hint=-1)` delegates directly to C-level `self._fh.readlines()` / `self._fh.readlines(hint)` for non-progress and counted streams (using `list(self._iter_progress())` for unbounded plain local progress reads). When `PolyWriter.write` is not overridden by a subclass, `PolyWriter.open()` binds `self.write` directly to `self._fh.write` (removed on `close()`). In binary mode, `PolyWriter.writelines()` iterates `self._fh.write(line)` so `ZstdCompressionWriter` behaves identically to stdlib binary writers.
- **Progress Accounting:** Compressed and remote streams wrap their binary handle in `_CountingByteStream`, which is the sole progress owner for counted streams: `PolyReader.open()` attaches `self._progress.update` after initializing `tqdm(initial=initial_bytes)` so `open()` header bytes are never double-counted, and `_CountingByteStream` batches `tqdm.update()` in 64 KiB increments while flushing exact remainders at EOF (before `close()`), on `readline()`/`read()`/`readlines()`, and in `close()`. Uncompressed local reads use `_iter_progress()` to count UTF-8 byte length in text mode (`len(line.encode('utf-8'))`, with CRLF EOF reconciliation) and raw byte length in binary mode (`len(line)`). When file size is unavailable or `stat()`/`getsize()` fails, `tqdm` uses `total=None`.
- **Deterministic Teardown:** `close()` flushes pending progress, detaches `_CountingByteStream` callbacks, snapshots and clears all handle attributes before invoking `_close_handles()` (which deduplicates raw handles already closed by `_BinaryStreamAdapter` or `_BoundedSFTPReadStream`), ensuring idempotent cleanup even when one handle fails to close. Mid-`open()` cleanup catches `BaseException` so interrupts (`KeyboardInterrupt`) do not leak sockets or threads.
- **CLI (`main()`, `src/polyopen/__main__.py`):** Exposed via `polyopen`, `python -m polyopen`, and `python -m polyopen.polyopen`. Both `--read` and `--write` stream 64 KiB binary chunks (`PolyReader(..., binary=True)` / `PolyWriter(..., binary=True)`), preserving exact bytes (including CRLF and non-UTF-8 payloads) and counting `b'\n'`-delimited lines. `--write` pre-validates destination guards before opening streams, reads the source once inside `contextlib.ExitStack`, and fans chunks out synchronously to all destinations.

## Test and Benchmark Layout

- **Unit tests (`tests/unit/`):**
  - `test_local_io.py`: Local codecs (text and binary modes), multi-chunk line streaming, sequential `.bu` backups, progress accounting, `file://` URIs, lifecycle, lazy imports, and file-like attributes.
  - `test_routing.py`: Mocked FTP/SFTP/HTTP routing (text and binary modes), real `FTPFile`-contract tests, `FTPSessionWrapper`, percent-decoding, timeouts, and an ephemeral local `ThreadingHTTPServer` for cookie-free and gzip HTTP streaming.
  - `test_guards.py`: Archive guards, protocol guards, invalid modes, append/backup conflicts, and text vs binary mode contracts.
- **Regression tests (`tests/regression/`):**
  - `test_golden.py`: Reads and round-trip verifies `tests/fixtures/gold.txt`, `gold.txt.gz`, `gold.txt.bz2`, `gold.txt.xz`, and `gold.txt.zst` in both text and binary modes against independent stdlib and `zstandard` decoders.
  - `test_cli.py`: CLI flags, single-pass broadcast, error exits, credential redaction, and checkout `src/` import verification.
- **Benchmarks (`benchmarks/bench_polyopen.py`):** Standalone informational suite measuring nine workloads: import latency, 200k-line text read throughput, 200k-line write throughput, compression peak RSS, local HTTP throughput, 500-backup allocation latency, 3-target CLI broadcast, 200k-line binary (`'rb'`) read throughput, and 200k-line `PolyReader.readlines()` throughput.
