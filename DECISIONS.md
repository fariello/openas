# Design and Architectural Decisions

*[Return to Main README](README.md)*

This document records the rationale and trade-offs behind load-bearing design choices in `polyopen` so contributors and reviewers can understand why current invariants exist before changing them.

## ADR-001: UTF-8 text-only interface with `'rb'`/`'wb'`/`'ab'` aliases (2026-11-21, reaffirmed 2026-11-25)

- **Context:** `polyopen` was built for line-oriented text stream processing (`.txt`, `.csv`, logs) across compressed and remote transports. Historical callers sometimes passed `'rb'`, `'wb'`, or `'ab'` expecting `polyopen` to handle binary codec framing transparently while still iterating strings.
- **Decision:** All modes operate strictly on UTF-8 `str` through `io.TextIOWrapper`. In v0.1.0, `'rb'`, `'wb'`, and `'ab'` remain accepted aliases for UTF-8 text modes rather than switching to `bytes` or raising `ValueError`.
- **Consequences:** Preserves compatibility for v0.1.0 callers, but differs from `builtins.open()` binary semantics. Docs and docstrings warn about this explicitly. True raw `bytes` mode or rejecting binary mode strings is deferred to v0.2.

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
- **Decision:** Accept `compresslevel: Optional[int] = None` and omit the keyword argument when `None` so each library uses its own balanced default (`zstd` 3, `lzma` 6, `bz2` 9, `gzip` 9). Validate `compresslevel` type/range and import codecs before backup rotation so invalid inputs never displace existing files.
- **Consequences:** Fast, low-memory default writes with explicit caller tuning when higher compression is needed.

## ADR-005: Fast-path, bounded, atomic local backup rotation (2026-11-21, hardened 2026-11-25)

- **Context:** Probing `.001.bu`, `.002.bu`, ... sequentially becomes O(N) on directories with hundreds of backups and causes high RTT overhead over SFTP/FTP. Naive `os.rename` can silently overwrite existing backups or rename an existing directory on Linux `EPERM`.
- **Decision:** Check `<path>.001.bu` first in O(1); if occupied, scan the parent directory once (`os.scandir` locally, `listdir` remotely), select the lowest free 3-digit ASCII slot up to `.999.bu`, and fall back to nanosecond timestamps (`time.time_ns()`). Locally, reject existing directories (`IsADirectoryError`), use `os.link()` + `os.unlink()`, and check `os.path.lexists` before any rename fallback. Remotely, SFTP retries `rename()` collisions across free slots.
- **Consequences:** O(1) common case and single-scan scaling up to 999 backups without risking directory renames or broken-symlink overwrites. Local rotation remains designed for non-concurrent writers; FTP rename atomicity depends on the remote server.

## ADR-006: SSH host-key verification and SFTP prefetch/pipelining (2026-11-21, documented 2026-11-25)

- **Context:** Remote SFTP throughput degrades without pipelining and prefetch, while `AutoAddPolicy()` would expose SSH connections to Man-in-the-Middle attacks.
- **Decision:** Always call `client.load_system_host_keys()` and preserve Paramiko's default `RejectPolicy`. Enable `set_pipelined(True)` on SFTP writes (always binary mode wrapped in UTF-8 `io.TextIOWrapper`), cache `stat().st_size` once, and call `prefetch(file_size, max_concurrent_requests=64)` when file size is known (Paramiko `<3.3` falls back to `prefetch(file_size)`).
- **Consequences:** High SFTP throughput with strict host-key verification. Known trade-off: Paramiko stores prefetched chunks in RAM until consumed; an opt-in/bounded prefetch parameter is deferred to v0.2.

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
