# System Architecture

*[Return to Main README](README.md)*

The core philosophy of `polyopen` is mapping heterogeneous stream protocol objects and non-contiguous native algorithm dictionaries perfectly uniformly behind a single standard CPython `io.TextIOWrapper` barrier dynamically. 

This strict separation ensures that complex internal file iterations natively remain immune to multi-level architectural memory boundaries seamlessly.

## Topology Core Components

### 1. The Interceptor Routing Engine (`polyopen()`)
Provides a drop-in factory facade over `PolyReader` and `PolyWriter`:
- Maps `'r'`/`'rt'`/`'rb'` to `PolyReader`, `'w'`/`'wt'`/`'wb'` to `PolyWriter(append=False)`, and `'a'`/`'at'`/`'ab'` to `PolyWriter(append=True)`.
- Forwards `backup`, `show_progress`, and optional `compresslevel: Optional[int] = None` (ignored in read mode) to the underlying handler.
- Defers optional standard library (`argparse`, `bz2`, `ftplib`, `gzip`, `lzma`, `urllib.parse`) and transport dependencies (`paramiko`, `ftputil`, `requests`, `zstandard`, `tqdm`) to first use, including PEP 562 lazy resolution for `FTPSessionWrapper`.

### 2. URL Protocol Extractor
Uses `urllib.parse.urlparse` on demand (skipped for plain local paths without `:`):
- Detects explicit scheme prefixes (`ftp://`, `ssh://`, `sftp://`, `http://`, `https://`, `file://`).
- Converts `file://` URIs to local OS paths via `urllib.request.url2pathname`.
- Explicitly rejects cloud object store schemes (`s3://`, `gs://`, `az://`, `hdfs://`) by raising `UnsupportedProtocolError`.

### 3. Context De-Mux Handlers (`_wrap_*`)
Determines backend communication semantics and isolates transport handle lifecycles cleanly:
- **`_wrap_ssh` & `_wrap_ftp`**: Support both reading and writing (default port 22 for SSH/SFTP and 21 for FTP, forwarding custom URL ports). On write, optional `compresslevel: Optional[int] = None` forwards `level` (`zstandard`), `preset` (`lzma`), or `compresslevel` (`gzip`, `bz2`), omitting `None` so each library uses its own default (`zstd` level 3, `lzma` preset 6, `bz2` 9, `gzip` 9). Compressed SSH writes open the SFTP file in binary mode (`'wb'`/`'ab'`) and call `set_pipelined(True)`. SFTP reads record `stat().st_size` once in `_sftp_file_size` (avoiding duplicate round trips in `_get_file_size()`) and invoke bounded `prefetch(file_size, max_concurrent_requests=64)` when file size is known (falling back to `prefetch(file_size)` on Paramiko `<3.3` `TypeError`). Backup rotation checks `<path>.001.bu` first in O(1); if occupied, it performs a single directory listing (`os.scandir` locally, `listdir` on SFTP/FTP with POSIX parent splitting) to pick the lowest free 3-digit ASCII slot up to `.999.bu`, falling back to collision-safe nanosecond timestamps (`time.time_ns()`). Local backups use atomic `os.link()` + `os.unlink()` with `os.rename()` fallback when hard links are unavailable. Missing SFTP files propagate `FileNotFoundError` cleanly without printing credentials or calling `exit(1)`. `close()` is idempotent and closes `_fh`, `_raw_fh`, `_sftp_file`, `_sftp_client`, `_ssh_client`, `_ftp`, `_response`, and `_progress` via `_close_handles()` even when an individual handle raises.
- **`_wrap_http`**: Uses a thread-local `requests.Session` from `_get_http_session()` with cookie persistence disabled (`http.cookiejar.DefaultCookiePolicy(allowed_domains=[])`). Validates status with `response.raise_for_status()` (closing the response on error), parses `Content-Length` safely to `Optional[int]`, and matches extensions case-insensitively on the URL path without query strings. Uncompressed HTTP responses set `response.raw.decode_content = True` and stream via `io.TextIOWrapper(_HttpRawStream(response.raw), encoding='utf-8')`, while compressed responses pass raw bytes directly to the matching decompressor without toggling `decode_content`. Closing the reader deterministically calls `response.close()`, including on early iteration exit.
- **Hot-Path Iteration, Progress Accounting, and CLI Fan-Out**: When `show_progress=False`, `PolyReader.__iter__()` returns the underlying `io.TextIOBase` C-level iterator directly. When `show_progress=True`, compressed streams wrap their binary handle in `_CountingByteStream`, batching `tqdm` updates in 64 KiB increments with exact remainder flushes on EOF, early exit, and `close()`, and falling back to `total=None` when stream size is unknown. `PolyWriter.open()` binds `self.write` directly to `self._fh.write` when `write` is not overridden by a subclass, and exposes `PolyWriter.writelines()`. The CLI `--write` command uses `contextlib.ExitStack` to read the source once and fan out each line synchronously to all destination writers, closing every handle on error.

### 4. Mathematical Chunk Boundaries & Compression (`io.TextIOWrapper`)
Directly extracts dictionary frame sizes globally enforcing contiguous buffer extraction formats reliably:
- Defers dictionary buffer maps explicitly to standard internal packages natively tracking algorithms independently (`gzip`, `bz2`, `lzma`).
- Evaluates purely autonomous C-binding interfaces dynamically intercepting the `zstandard` module mapping chunk output frames seamlessly identically conforming to CPython bounds dynamically (e.g. standardizes variable byte output block tracking arrays enforcing native `128KB` chunk alignment implicitly).

## Local Development Topology

The localized development and testing frameworks are strictly mapped executing continuous validation loops verifying system boundary architectures correctly cleanly structurally.

### Hybrid Evaluation Mocks
To maintain a blazingly fast iteration scope safely averting massive computational CI overhead constraints locally:
- Exact algorithm ingestion validation paths uniformly track memory constraints verifying boundary overlaps perfectly against heavily isolated native files identically (`tests/unit/test_local_io.py`).
- SFTP and remote HTTP dependencies uniquely evaluate Mocked byte objects statically determining strictly algorithmic logic intercepts successfully tracking code branches correctly entirely independently avoiding establishing live architectural nodes entirely (`tests/unit/test_routing.py`).

### Golden Master Subsystem
Internal logic strictly relies upon isolated 1KB backend archive references internally housed validating internal memory execution targets fundamentally ensuring backwards binary decoding loops permanently remain identical seamlessly (`tests/fixtures/gold.zst`, `tests/regression/test_golden.py`).
