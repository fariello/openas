# Functional Specification: `polyopen`

*[Return to Main README](README.md)*

**Version:** 0.1.0
**Last Updated:** 2026-04-07

## 1. Purpose and Scope
`polyopen` is a lightweight, zero-configuration Python library designed to natively unify read, write, and append operations across multiple remote protocols and stream-based data compression algorithms. It acts as an augmented drop-in replacement for CPython's standard `builtins.open()` context manager, resolving complex external transport layers securely out-of-the-box.

**Non-Goals:** `polyopen` is deliberately not designed for cloud infrastructure management (e.g., AWS S3 multipart streaming) or complex Posix archival system manipulation (`.tar`, `.zip`). 

## 2. Core Concepts and Terminology
- **Unified Wrapper:** The `polyopen()` entry point intelligently infers behavior, automatically delegating execution to the proper internal handler (e.g., `PolyReader` or `PolyWriter`) based on the invocation mode.
- **Transparent Decompression:** Utilizing standard library extensions (`_compression` and `io.TextIOWrapper`), stream algorithms like `.zst` or `.gz` are evaluated entirely lazily and piped back into standard UTF-8 string fragments automatically.
- **Dynamic Backup Rotation:** A unique local/network resiliency feature that auto-detects destination overwrite collisions, caching historical overwrites iteratively with sequential `.001.bu` numeric tags rather than blindly destroying data.

## 3. Supported Protocols & Data Models
Because `polyopen` is explicitly configured for contiguous stream parsing, it asserts rigorous internal guards defining file boundaries. 

### 3.1 Network Transports
- `file://` (Implicit): 100% feature compliance.
- `ftp://`: Fully supported (relies on `ftputil`).
- `ssh://` or `sftp://`: Fully supported (relies on `paramiko` mapping context configuration via `~/.ssh/known_hosts` natively). 
- `http://` / `https://`: Stream fetching supported natively (relies on `requests`). **Read-Only**.

**Refusals:** `s3://`, `gs://`, `hdfs://`, `az://` will actively trigger a hard crash alerting users to instead leverage dedicated `smart_open` binaries.

### 3.2 Compression Models
Natively supported stream architectures:
1. `bz2` (Standard Python binding)
2. `gz` (Standard Python binding)
3. `xz` (Standard Python lzma binding)
4. `zst` (Requires `zstandard` module)

**Archival Containers (e.g., `.zip`, `.tar`, `.rar`) are explicitly rejected** during I/O instantiation because `readline()`/`write()` streams catastrophically corrupt their embedded binary file-tables.

## 4. Public API 
### `polyopen(filename: Union[str, os.PathLike], mode: str = 'r', backup: Optional[bool] = None, show_progress: bool = False)`
- **`filename`**: Accepts `str` or `os.PathLike` (normalized via `os.fsdecode()`). Extension matching is case-insensitive and ignores URL query strings.
- **`mode`**: Accepts `'r'`, `'rt'`, `'rb'` (`PolyReader`), `'w'`, `'wt'`, `'wb'` (`PolyWriter(append=False)`), and `'a'`, `'at'`, `'ab'` (`PolyWriter(append=True)`). All streams decode and encode UTF-8 text explicitly.
- **`backup`**: Defaults to `None` (`True` on write modes, `False` on append modes). Passing `backup=True` with an append mode raises `ValueError`.
- **`show_progress`**: Injects a `tqdm` progress bar tracking exact raw bytes read (compressed bytes on compressed streams, and indeterminate `total=None` when stream size is unknown). Supported only on `PolyReader`; passing `show_progress=True` to `PolyWriter` or write/append modes raises `NotImplementedError`.

`PolyReader` exposes standard `read()`, `readline()`, `readlines()`, `__iter__()`, `__next__()`, `open()`, and idempotent `close()` methods (reopening after `close()` works cleanly; calling read methods after `close()` raises `ValueError`). `PolyWriter` exposes `write()`, `flush()`, `open()`, and idempotent `close()` methods (calling `write()` or `flush()` after `close()` raises `ValueError`).

## 5. Command-Line Interface (CLI) Behavior
The package exposes a CLI via `python -m polyopen.polyopen`:
- `--read` (`-r`): Reads all positional `files` and prints line count, UTF-8 byte size (`B` through `YB`), throughput, and elapsed time.
- `--write` (`-w`): Streams lines from the source file to one or more destination files.
- `--input-file` (`-i`): Optional source path for `--write` mode (all positional arguments become destinations). Rejected with `parser.error` if used with `--read` or without at least one destination file.
*Exit Semantics:* Returns `0` on success, or exits non-zero via `parser.error` when arguments clash or required targets are missing.

## 6. Edge Cases & Boundary Conditions
1. **Append & Backup Collision:** Passing `polyopen(file, 'a', backup=True)` or `PolyWriter(file).open(append=True, backup=True)` raises `ValueError("Cannot have both append and backup set to True.")`.
2. **Bounded Backup Rotation and Atomic Local Moves:** Backup numbering is capped at 999 sequential `.NNN.bu` slots across local, SFTP, and FTP destinations; beyond 999 backups, `polyopen` falls back to nanosecond timestamps (`time.time_ns()`) with collision retry. Local backups use atomic `os.link()` followed by `os.unlink()` (retrying the next free slot on `FileExistsError` and falling back to `os.rename()` only when hard links are unsupported).
3. **Chunk Shearing and Multi-Frame Zstandard:** `zstandard` internal payloads are 128KB by default. Massive lines and multi-frame concatenated `.zst` streams (`read_across_frames=True`) are parsed consistently across local, FTP, SFTP, and HTTP transports via `io.TextIOWrapper`.

## 7. Security and Safety Considerations
1. **Remote Shell Code Execution Risk:** SSH client endpoints enforce `load_system_host_keys()`, actively refusing blind Hostkey injections by default on fresh environments to thwart Man-In-The-Middle network redirections.
2. **Archival Destruction Safety:** Archival rejection is hardcoded in memory via static array evaluations ensuring a user can never permanently destroy localized central directory maps of remote zip files via blindly appending data frames.
3. **Cloud Credentials Bleed Avoidance:** Rejection of `boto3` logic natively secures developers against accidentally mapping AWS environment access tokens directly through untracked repository executions.
