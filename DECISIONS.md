# Design and Architectural Decisions

*[Return to Main README](README.md)*

This document records the rationale and trade-offs behind load-bearing design choices in `polyopen` so contributors and reviewers can understand why current invariants exist before changing them.

## ADR-001: Distinct UTF-8 text and binary `bytes` modes (2026-11-25)

- **Context:** `polyopen` was built primarily to stream compressed files (logs, CSV, JSONL, and binary records) line-by-line or in chunks across local and remote locations. Earlier versions accepted `'rb'`, `'wb'`, and `'ab'` as aliases for UTF-8 text mode, which contradicted Python's `builtins.open()` semantics and prevented handling non-UTF-8 binary streams.
- **Decision:** Match standard Python mode semantics before the initial v0.1.0 release: `'r'`/`'rt'`, `'w'`/`'wt'`, and `'a'`/`'at'` (and `binary=False` on `PolyReader`/`PolyWriter`) operate on UTF-8 `str` (`encoding == 'utf-8'`), while `'rb'`, `'wb'`, and `'ab'` (and `binary=True`) operate on raw `bytes` (`encoding is None`). Wrap binary `.gz`, `.bz2`, `.xz`, and plain binary network readers in 64 KiB `io.BufferedReader` (`buffer_size=65536`) for C-level line scanning across chunk boundaries, and wrap `.zst` stream readers (`read_size=131072`) in 128 KiB `io.BufferedReader` (`buffer_size=131072`) in both text and binary modes (which also supplies `readline()` over `ZstdDecompressionReader`, whose raw `readline()` raises `io.UnsupportedOperation`). Implement binary `PolyWriter.writelines()` by calling `write()` per item so `ZstdCompressionWriter` matches stdlib binary writers.
- **Consequences:** Callers get predictable `builtins.open()` behavior in both text and binary modes without breaking default text usage (`'r'`, `'w'`, `'a'`).

## ADR-002: Reject cloud object stores and container archives explicitly (2026-11-21)

- **Context:** Cloud object stores (`s3://`, `gs://`, `az://`, `hdfs://`) require heavy SDKs (`boto3`, GCS/Azure libraries), and container archives (`.zip`, `.tar`, `.tar.*`, `.rar`, `.7z`) embed internal directory headers that stream writes corrupt.
- **Decision:** Fail fast with `UnsupportedProtocolError` (recommending `smart_open` or cloud SDKs) and `UnsupportedArchiveError` (recommending `zipfile`/`tarfile`). Unknown multi-letter `scheme://` URLs also raise `UnsupportedProtocolError` instead of falling through to local file opening.
- **Consequences:** Keeps `polyopen` small and prevents silent archive corruption or confusing local `FileNotFoundError` messages on URL typos.

## ADR-003: Lazy imports with batteries-included install dependencies (2026-11-21, reaffirmed 2026-11-25)

- **Context:** Importing `paramiko`, `requests`, `ftputil`, `zstandard`, `tqdm`, and optional stdlib codecs eagerly slows CLI startup and local-file scripts. Conversely, splitting dependencies into optional extras breaks `pip install polyopen` for users who expect documented remote and `.zst` features immediately.
- **Decision:** Keep all five third-party packages in `[project.dependencies]` for v0.1.0, while deferring every optional import until first use (including PEP 562 lazy resolution for `FTPSessionWrapper`).
- **Consequences:** Fast `import polyopen` latency (verified by subprocess tests) with zero-flag installation. Optional extras (`[ssh]`, `[ftp]`, etc.) may be evaluated for v0.2.

## ADR-004: Codec defaults via `compresslevel=None` (2026-11-21)

- **Context:** Earlier code hardcoded extreme compression levels (`zstd` level 22, `lzma` preset 9), which consumed high memory and CPU on writes.
- **Decision:** Accept `compresslevel: Optional[int] = None` and omit the keyword argument when `None` so each library uses its own balanced default (`zstd` 3, `lzma` 6, `bz2` 9, `gzip` 9). Validate `compresslevel` type/range and import codecs before creating any `.bu` backup so invalid inputs never displace existing files.
- **Consequences:** Fast, low-memory default writes with explicit caller tuning when higher compression is needed.

## ADR-005: Fast-path, bounded, atomic local sequential `.bu` backups (2026-11-21, hardened 2026-11-25)

- **Context:** Probing `.001.bu`, `.002.bu`, ... sequentially becomes O(N) on directories with hundreds of backups and causes high RTT overhead over SFTP/FTP. Naive `os.rename` can silently overwrite existing backups or rename an existing directory on Linux `EPERM`.
- **Decision:** Check `<path>.001.bu` first in O(1); if occupied, scan the parent directory once (`os.scandir` locally, `listdir` remotely), select the lowest free 3-digit ASCII slot up to `.999.bu`, and fall back to nanosecond timestamps (`time.time_ns()`). Locally, reject existing directories (`IsADirectoryError`), use `os.link()` + `os.unlink()`, and check `os.path.lexists` before any rename fallback. Remotely, both SFTP (`PolyWriter._ssh_backup_if` via `stat.S_ISDIR`) and FTP (`PolyWriter._wrap_ftp` via `isdir`) reject existing remote directories with `IsADirectoryError` before calling `rename()`, and SFTP retries `rename()` collisions across free slots. Existing `.bu` files are never pruned or rotated out.
- **Consequences:** O(1) common case and single-scan scaling up to 999 backups without risking directory renames or broken-symlink overwrites. Local sequential backups remain designed for non-concurrent writers; FTP rename atomicity depends on the remote server.

## ADR-006: SSH host-key verification and bounded SFTP streaming/pipelining (2026-11-21, updated 2026-11-25)

- **Context:** Remote SFTP throughput degrades without pipelined reads and writes, while `AutoAddPolicy()` would expose SSH connections to Man-in-the-Middle attacks. Paramiko's `SFTPFile.prefetch(file_size, max_concurrent_requests=64)` allocates `file_size // 32768` request tuples up front and caps only in-flight requests (`_prefetch_extents`), not buffered unread chunks (`_prefetch_data`), causing out-of-memory (OOM) failures on multi-gigabyte SFTP streams whenever a consumer is slower than the network or stops early.
- **Decision:** Always call `client.load_system_host_keys()` and preserve Paramiko's default `RejectPolicy`. Enable `set_pipelined(True)` on SFTP writes (always binary SFTP handles wrapped in `_BinaryStreamAdapter`, plus `io.TextIOWrapper` in text mode; Paramiko drains `_reqs` in O(1) RAM). For SFTP reads, cache `stat().st_size` once and wrap `SFTPFile` in `_BoundedSFTPReadStream` whenever `file_size > 0` and `readv` is available: each active window requests at most 4 MiB split into `MAX_REQUEST_SIZE` (default 32 KiB) sub-chunks via `sftp_file.readv(sub_chunks, max_concurrent_prefetch_requests=64)` (falling back to single-argument `readv(sub_chunks)` on Paramiko `<3.3` `TypeError`). When `readv` is absent or `file_size` is unknown or non-positive, fall back directly to sequential `sftp_file.read()`. Store `_BoundedSFTPReadStream` on `self._raw_fh` so `close()` always stops active `readv` generators and closes the underlying `SFTPFile` deterministically.
- **Consequences:** High pipelined SFTP throughput with strict host-key verification, O(1) `open()` startup time, and O(1) memory (`<= 4 MiB` active prefetch window per stream) on multi-gigabyte SFTP files.

## ADR-007: Thread-local cookie-free HTTP session and `Accept-Encoding` handling (2026-11-21, hardened 2026-11-25)

- **Context:** Reusing a `requests.Session` enables TCP/TLS keep-alive, but sharing cookies across unrelated URLs leaks state across requests, and default `Accept-Encoding: gzip, deflate` can cause servers to gzip-wrap `.bz2`/`.xz`/`.zst` payloads.
- **Decision:** Maintain one lock-initialized, thread-local `requests.Session` per thread with `DefaultCookiePolicy(allowed_domains=[])` and `atexit` cleanup. Send `Accept-Encoding: identity` on compressed extensions, enable `response.raw.decode_content = True` strictly on uncompressed responses, and set `total=None` on uncompressed responses carrying `Content-Encoding`.
- **Consequences:** Safe connection pooling without cross-origin cookie bleed or double-decompression corruption.

## ADR-008: `ftputil` adapter and FTP append exclusion (2026-11-25)

- **Context:** `ftputil.file.FTPFile` delegates a restricted attribute set and raises `AttributeError` for `io.BufferedIOBase` methods (`readable`, `writable`, `seekable`, `read1`, `readinto`), and `ftputil.FTPHost.open(..., 'ab')` unconditionally raises `FTPIOError("append mode not supported")`. Additionally, `ftputil` 5.2 emits an import-time `DeprecationWarning` regarding its planned 6.0 default path encoding switch from Latin-1 to UTF-8, and `FTPFile.close()` ignores `426`/`450`/`451` responses.
- **Decision:** Wrap `FTPFile` handles in `_BinaryStreamAdapter(io.BufferedIOBase)` so all read/write codecs work on real FTP handles, and reject FTP append up front with `UnsupportedProtocolError` before connecting. Keep `ftputil` for v0.1.0 rather than rewriting FTP on raw `ftplib`.
- **Consequences:** Fixes real-world FTP crashes cleanly while avoiding risky pre-v0.1.0 transport rewrites. Raw `ftplib` / `FTPS` support is deferred to v0.2.

## ADR-009: Zstandard 2 GiB decompression window (2026-11-21, documented 2026-11-25)

- **Context:** Zstandard archives compressed with `--long=31` require `max_window_size=2**31` to decompress, but untrusted `.zst` inputs with a large window header can allocate up to 2 GiB of RAM.
- **Decision:** Keep `max_window_size=2**31` in v0.1.0 to preserve compatibility with `--long=31` archives, document the untrusted-input caveat in `FUNCTIONAL_SPEC.md`, and defer making `max_window_size` configurable to v0.2.
- **Consequences:** Existing large-window archives keep working; callers handling untrusted `.zst` streams are explicitly informed of the memory boundary.

## ADR-010: Minimum Python 3.9 to match PEP 639 build backend (2026-11-25)

- **Context:** `pyproject.toml` uses PEP 639 `license` and `license-files` metadata requiring `setuptools>=77.0.0`, which dropped Python 3.8 support in `75.4.0`. Python 3.8 is also end-of-life.
- **Decision:** Set `requires-python = ">=3.9"` in `pyproject.toml` and test across Python 3.9 through 3.13 in CI.
- **Consequences:** Packaging metadata matches the actual build backend and supported Python versions.
