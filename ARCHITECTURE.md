# System Architecture

*[Return to Main README](README.md)*

The core philosophy of `polyopen` is mapping heterogeneous stream protocol objects and non-contiguous native algorithm dictionaries perfectly uniformly behind a single standard CPython `io.TextIOWrapper` barrier dynamically. 

This strict separation ensures that complex internal file iterations natively remain immune to multi-level architectural memory boundaries seamlessly.

## Topology Core Components

### 1. The Interceptor Routing Engine (`polyopen()`)
Structurally handles contextual API injection routes explicitly evaluated alongside Python's internal namespace cleanly:
- Operates primarily mapping logic evaluating `'w'` vs `'r'` string modifiers.
- Translates global parameters identically down to localized classes.
- Inherently acts as a pure factory facade pattern obfuscating manual class implementation loops fundamentally.

### 2. URL Protocol Extractor
Relying intrinsically upon the standard standard library `urllib.parse` engine natively:
- Accurately tracks explicit `://` demarcations evaluating payload destinations.
- Strips maligned internal `file:///C:/` OS paths reverting logic successfully back onto normalized OS standard paths implicitly mapping standard local IO constraints seamlessly.
- Explicitly rejects cloud object store schemes (`s3://`, `gs://`, `az://`, `hdfs://`) by raising `UnsupportedProtocolError`.

### 3. Context De-Mux Handlers (`_wrap_*`)
Determines backend communication semantics and isolates transport handle lifecycles cleanly:
- **`_wrap_ssh` & `_wrap_ftp`**: Support both reading and writing (with default ports 22 for SSH/SFTP and 21 for FTP, forwarding custom URL ports). Backups are capped at 999 sequential `.NNN.bu` slots with nanosecond collision-safe fallback on SFTP and FTP, while local backups use atomic `os.link()` + `os.unlink()` with `os.rename()` fallback when hard links are unavailable. Missing SFTP files propagate `FileNotFoundError` cleanly without printing credentials or calling `exit(1)`.
- **`_wrap_http`**: Streams responses via `requests.get(stream=True)`, validates status with `response.raise_for_status()` (closing the response on error), parses `Content-Length` safely to `Optional[int]`, and matches extensions case-insensitively on the URL path without query strings.
- **Raw-Byte Progress Accounting (`_CountingByteStream`)**: When `show_progress=True`, compressed streams wrap their underlying binary handle in `_CountingByteStream` so `tqdm` tracks exact raw compressed bytes read and falls back to `total=None` when stream size is unknown.

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
