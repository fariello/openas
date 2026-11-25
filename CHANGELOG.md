# Changelog

All notable changes to `polyopen` are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-25

Initial standalone open-source release under the Apache License, Version 2.0.

### Added

- Unified `polyopen(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None)` entry point plus `PolyReader` and `PolyWriter` context managers.
- Transparent UTF-8 text compression and decompression for `.gz`, `.bz2`, `.xz`, and multi-frame `.zst`, with configurable `compresslevel` forwarding and balanced library defaults when `None`.
- Transport support for local files (`file://` and implicit paths), `http://`/`https://` (read-only via thread-local, cookie-free `requests.Session` streaming), `ftp://` (read and write via `ftputil` and `_BinaryStreamAdapter`), and `ssh://`/`sftp://` (read, write, and append via `paramiko` with system host-key verification, SFTP read prefetch, and write pipelining).
- Fast, collision-safe `.bu` backup rotation (`.001.bu` O(1) fast-path, single directory listing up to `.999.bu`, nanosecond timestamp fallback, atomic local `os.link`/`os.unlink`, directory protection, and SFTP collision retry).
- `PolyReader` `tqdm` progress bars tracking exact bytes read with 64 KiB batched updates and `total=None` fallback when stream size is unavailable.
- Standard file-like attributes (`closed`, `readable()`, `writable()`, `seekable()`, `encoding`, credential-redacted `__repr__()`) and `PolyWriter.writelines()` / `flush()`.
- Fail-fast exception hierarchy (`PolyopenError`, `UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`) covering container archives, cloud object stores, HTTP writes, FTP append, non-localhost `file://` URIs, and unrecognized URL schemes.
- Built-in CLI accessible via `polyopen`, `python -m polyopen`, and `python -m polyopen.polyopen` with `--read`, single-pass multi-target `--write`, `--input-file`/`-i`, `--version`, and automatic URL credential/query redaction.
- PEP 562 lazy import architecture, standalone benchmark suite (`benchmarks/bench_polyopen.py`), comprehensive unit/regression/golden test suite, and cold-start project documentation (`ARCHITECTURE.md`, `FUNCTIONAL_SPEC.md`, `DECISIONS.md`, `GUIDING_PRINCIPLES.md`, `docs/API.md`, `docs/CLI.md`, `CONTRIBUTING.md`, `CITATION.cff`, `LICENSE`, `NOTICE`).
