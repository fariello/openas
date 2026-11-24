# Functional Specification: `polyopen`

*[Return to Main README](README.md)*

**Version:** 0.1.0
**Last Updated:** 2026-11-24

## 1. Purpose and Scope
`polyopen` is a lightweight, zero-configuration Python library designed to natively unify read, write, and append operations across multiple remote protocols and stream-based data compression algorithms. It acts as an augmented drop-in replacement for CPython's standard `builtins.open()` context manager, resolving complex external transport layers securely out-of-the-box.

**Non-Goals:** `polyopen` is deliberately not designed for cloud infrastructure management (e.g., AWS S3 multipart streaming) or complex Posix archival system manipulation (`.tar`, `.zip`). 

## 2. Core Concepts and Terminology
- **Unified Wrapper:** The `polyopen()` entry point infers behavior from the requested mode and delegates to `PolyReader` or `PolyWriter`. Optional standard library and transport modules are imported lazily on first use.
- **Transparent Decompression and Compression:** Using standard library codecs (`gzip`, `bz2`, `lzma`), `zstandard`, and `io.TextIOWrapper`, compressed streams are read and written lazily as UTF-8 text.
- **Dynamic Backup Rotation:** On overwrite, `polyopen` checks `<path>.001.bu` first in O(1); if that slot is occupied, it lists the parent directory once (`os.scandir` locally, single `listdir` on SFTP/FTP) to pick the lowest free 3-digit slot up to `.999.bu`, falling back to collision-safe nanosecond timestamps past 999.

## 3. Supported Protocols & Data Models
Because `polyopen` is explicitly configured for contiguous stream parsing, it asserts rigorous internal guards defining file boundaries. 

### 3.1 Network Transports
- `file://` (Implicit): 100% feature compliance.
- `ftp://`: Read, write, and append via `ftputil` (default port 21, custom ports forwarded), with `.001.bu` fast-path and single-listing backup rotation.
- `ssh://` or `sftp://`: Read, write, and append via `paramiko` (`load_system_host_keys()`, default port 22, custom ports forwarded), with bounded read prefetch (`max_concurrent_requests=64`, falling back to `prefetch(file_size)` on Paramiko `<3.3`), cached `stat().st_size`, binary-mode compressed writes with `set_pipelined(True)`, and deterministic idempotent handle closure.
- `http://` / `https://`: **Read-Only** streaming via thread-local, cookie-free `requests.Session` reuse (`DefaultCookiePolicy(allowed_domains=[])`) and `io.TextIOWrapper(response.raw, encoding='utf-8')` streaming (`response.raw.decode_content = True` strictly on uncompressed responses), with deterministic `response.close()` on completion, error, or early exit.

**Refusals:** `s3://`, `gs://`, `hdfs://`, `az://` raise `UnsupportedProtocolError`, directing callers to `smart_open` or dedicated cloud SDKs.

### 3.2 Compression Models
Natively supported stream formats (all accept optional `compresslevel: Optional[int] = None` on write/append; when `None`, each library uses its own default):
1. `bz2` (Standard Python `bz2` binding; default `compresslevel` 9)
2. `gz` (Standard Python `gzip` binding; default `compresslevel` 9)
3. `xz` (Standard Python `lzma` binding; default `preset` 6)
4. `zst` (`zstandard` module; default `level` 3; multi-frame reads with `read_across_frames=True`)

**Archival Containers (e.g., `.zip`, `.tar`, `.rar`) are explicitly rejected** during I/O instantiation because `readline()`/`write()` streams corrupt their embedded binary file tables.

## 4. Public API 
### `polyopen(filename: Union[str, os.PathLike], mode: str = 'r', backup: Optional[bool] = None, show_progress: bool = False, compresslevel: Optional[int] = None)`
- **`filename`**: Accepts `str` or `os.PathLike` (normalized via `os.fsdecode()`). Extension matching is case-insensitive and ignores URL query strings.
- **`mode`**: Accepts `'r'`, `'rt'`, `'rb'` (`PolyReader`), `'w'`, `'wt'`, `'wb'` (`PolyWriter(append=False)`), and `'a'`, `'at'`, `'ab'` (`PolyWriter(append=True)`). All streams decode and encode UTF-8 text explicitly.
- **`backup`**: Defaults to `None` (`True` on write modes, `False` on append modes). Passing `backup=True` with an append mode raises `ValueError`.
- **`show_progress`**: Injects a `tqdm` progress bar tracking exact raw bytes read (compressed bytes on compressed streams, batched in 64 KiB increments with exact flush on EOF/exit/close, and indeterminate `total=None` when stream size is unknown). Supported only on `PolyReader`; passing `show_progress=True` to `PolyWriter` or write/append modes raises `NotImplementedError`.
- **`compresslevel`**: Optional integer compression level or preset forwarded to `.zst`, `.gz`, `.bz2`, and `.xz` writers (ignored in read mode). When `None`, each compression library uses its own default (`zstd` level 3, `lzma` preset 6, `bz2` 9, `gzip` 9).

`PolyReader` exposes standard `read()`, `readline()`, `readlines()`, `__iter__()`, `__next__()`, `open()`, and idempotent `close()` methods (reopening after `close()` works cleanly; calling read methods after `close()` raises `ValueError`). `PolyWriter` exposes `write()`, `writelines()`, `flush()`, `open(append=False, backup=True, compresslevel=None)`, and idempotent `close()` methods (calling `write()`, `writelines()`, or `flush()` after `close()` raises `ValueError`).

## 5. Command-Line Interface (CLI) Behavior
The package exposes a CLI via `python -m polyopen.polyopen`:
- `--read` (`-r`): Reads all positional `files` and prints line count, UTF-8 byte size (`B` through `YB`), throughput, and elapsed time.
- `--write` (`-w`): Performs a single-pass synchronous broadcast using `contextlib.ExitStack`, reading the source file once, writing identical lines to all destination files, printing `Writing ...` and `FINISHED Writing. ...` lines per destination, and closing all handles cleanly on error.
- `--input-file` (`-i`): Optional source path for `--write` mode (all positional arguments become destinations). Rejected with `parser.error` if used with `--read` or without at least one destination file.
*Exit Semantics:* Returns `0` on success, or exits non-zero via `parser.error` when arguments clash or required targets are missing.

## 6. Edge Cases & Boundary Conditions
1. **Append & Backup Collision:** Passing `polyopen(file, 'a', backup=True)` or `PolyWriter(file).open(append=True, backup=True)` raises `ValueError("Cannot have both append and backup set to True.")`.
2. **Fast-Path, Single-Listing, Bounded Backup Rotation:** Backup rotation checks `<path>.001.bu` first in O(1); if occupied, it runs a single parent directory scan (`os.scandir` locally, `listdir` on SFTP/FTP) matching exact 3-digit ASCII `.NNN.bu` slots up to 999, then falls back to nanosecond timestamps (`time.time_ns()`) with collision retry. Local backups use atomic `os.link()` followed by `os.unlink()` (retrying the next free slot on `FileExistsError` and falling back to `os.rename()` only when hard links are unsupported).
3. **Chunk Shearing and Multi-Frame Zstandard:** `zstandard` internal payloads are 128KB by default. Massive lines and multi-frame concatenated `.zst` streams (`read_across_frames=True`) are parsed consistently across local, FTP, SFTP, and HTTP transports via `io.TextIOWrapper`.

## 7. Security and Safety Considerations
1. **Remote Shell Code Execution Risk:** SSH client endpoints enforce `load_system_host_keys()`, actively refusing blind Hostkey injections by default on fresh environments to thwart Man-In-The-Middle network redirections.
2. **Archival Destruction Safety:** Archival rejection is hardcoded in memory via static array evaluations ensuring a user can never permanently destroy localized central directory maps of remote zip files via blindly appending data frames.
3. **Cloud Credentials Bleed Avoidance:** Rejection of `boto3` logic natively secures developers against accidentally mapping AWS environment access tokens directly through untracked repository executions.
