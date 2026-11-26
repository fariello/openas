# Changelog

All notable changes to `openas` are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-25

Initial standalone open-source release under the Apache License, Version 2.0.

### Added

- Unified `openas(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None)` entry point plus `OpenAsReader` and `OpenAsWriter` context managers supporting both UTF-8 text (`'r'`/`'rt'`, `'w'`/`'wt'`, `'a'`/`'at'`) and binary `bytes` (`'rb'`, `'wb'`, `'ab'`, `binary=True`) modes.
- Line-by-line and chunked transparent compression and decompression for plain files, `.gz`, `.bz2`, `.xz`, and multi-frame `.zst` (including C-buffered binary reads via 64 KiB `io.BufferedReader` for binary `.gz`/`.bz2`/`.xz`/plain-network readers and 128 KiB `io.BufferedReader` for `.zst` in both text and binary modes), with configurable `compresslevel` forwarding and balanced library defaults when `None`.
- Transport support for local files (`file://` and implicit paths), `http://`/`https://` (read-only via thread-local, cookie-free `requests.Session` streaming), `ftp://` (read and write via `ftputil` and `_BinaryStreamAdapter`), and `ssh://`/`sftp://` (read, write, and append via `paramiko` with system host-key verification, bounded 4 MiB sliding-window SFTP `readv()` streaming via `_BoundedSFTPReadStream`, and write pipelining).
- Optional, collision-safe sequential `.bu` overwrite backups (`.001.bu` O(1) fast-path, single directory listing up to `.999.bu`, nanosecond timestamp fallback, atomic local `os.link`/`os.unlink`, local/SFTP/FTP directory `IsADirectoryError` protection, and SFTP collision retry).
- Stream-driven `OpenAsReader` `tqdm` progress bars (`_CountingByteStream`) tracking exact raw bytes read with 64 KiB batched updates, exact EOF and `close()` remainder flushing, CRLF local text EOF reconciliation, and `total=None` fallback when stream size is unavailable.
- Standard file-like attributes (`binary`, `closed`, `readable()`, `writable()`, `seekable()`, `encoding`, credential-redacted `__repr__()`) and `OpenAsWriter.writelines()` / `flush()`.
- Fail-fast exception hierarchy (`OpenAsError`, `UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`) covering container archives, cloud object stores, HTTP writes, FTP append, non-localhost `file://` URIs, and unrecognized URL schemes.
- Built-in CLI (`main()`) accessible via `openas`, `python -m openas`, and `python -m openas.openas` with 64 KiB binary-chunk streaming for `--read` and single-pass multi-target `--write` (preserving exact bytes including CRLF and non-UTF-8 payloads), `--input-file`/`-i`, `--version`, and automatic URL credential/query redaction.
- PEP 562 lazy import architecture, standalone benchmark suite (`benchmarks/bench_openas.py`), comprehensive unit/regression/golden test suite, and cold-start project documentation (`ARCHITECTURE.md`, `FUNCTIONAL_SPEC.md`, `DECISIONS.md`, `GUIDING_PRINCIPLES.md`, `docs/API.md`, `docs/CLI.md`, `CONTRIBUTING.md`, `CITATION.cff`, `LICENSE`, `NOTICE`).
