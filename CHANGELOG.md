# Changelog

All notable changes to `openas` are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-27

Initial standalone open-source release under the Apache License, Version 2.0.

### Added

- Unified `openas(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None, encoding=None, errors=None, newline=None, headers=None, key_filename=None)` entry point plus `OpenAsReader` and `OpenAsWriter` context managers supporting configurable text modes (`'r'`/`'rt'`, `'w'`/`'wt'`, `'a'`/`'at'`, UTF-8 by default with `builtins.open()`-compatible `encoding`, `errors`, and `newline` options) and binary `bytes` (`'rb'`, `'wb'`, `'ab'`, `binary=True`) modes.
- Line-by-line and chunked transparent compression and decompression for plain files, `.gz`, `.bz2`, `.xz`, and multi-frame `.zst` (including C-buffered binary reads via 64 KiB `io.BufferedReader` for binary `.gz`/`.bz2`/`.xz`/plain-network readers and 128 KiB `io.BufferedReader` for `.zst` in both text and binary modes), with configurable `compresslevel` forwarding and balanced library defaults when `None`.
- Zero-copy RFC 8878 truncated `.zst` stream detection (`_ZstdFrameValidatingStream`) raising `EOFError` on truncated Zstandard streams across local, HTTP, FTP, and SFTP reads while preserving O(1) memory.
- Transport support for local files (`file://` and implicit paths), `http://`/`https://` (read-only via thread-local, cookie-free `requests.Session` streaming with optional validated per-request `headers`), `ftp://` (read and write via `ftputil` and `_BinaryStreamAdapter`), and `ssh://`/`sftp://` (read, write, and append via `paramiko` with system host-key verification, optional `key_filename` SSH private key forwarding, bounded 4 MiB sliding-window SFTP `readv()` streaming via `_BoundedSFTPReadStream`, and write pipelining).
- Optional, collision-safe sequential `.bu` overwrite backups (`.001.bu` O(1) fast-path, single directory listing up to `.999.bu`, nanosecond timestamp fallback, atomic local `os.link`/`os.unlink`, local/SFTP/FTP directory `IsADirectoryError` protection, and SFTP collision retry).
- Stream-driven `tqdm` raw-byte progress bars for both reading (`OpenAsReader` via `_CountingByteStream`, 64 KiB batched updates, exact EOF and `close()` remainder flushing, CRLF local text EOF reconciliation, and `total=None` fallback) and writing/appending (`OpenAsWriter` via `_CountingWriteStream`, `total=None`, capturing open-time header bytes in `initial` and flushing buffered payload and compressed trailer bytes on `flush()` and `close()`).
- Standard file-like attributes (`binary`, `closed`, `readable()`, `writable()`, `seekable()`, `encoding`, credential-redacted `__repr__()`) plus `OpenAsReader.flush()` / binary `read1()` (enabling direct `io.TextIOWrapper(openas(path, "rb"))` composition with zero `ResourceWarning`) and `OpenAsWriter.writelines()` / `flush()`.
- Fail-fast exception hierarchy (`OpenAsError`, `UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`) covering container archives, cloud object stores, HTTP writes, FTP append, non-localhost `file://` URIs, and unrecognized URL schemes, with clean top-level `openas.__all__` exports excluding internal `FTPSessionWrapper`.
- Built-in CLI (`main()` wrapped by `_cli_main()`) accessible via `openas`, `python -m openas`, and `python -m openas.openas` with 64 KiB binary-chunk streaming for `--read` and single-pass multi-target `--write` (preserving exact bytes including CRLF and non-UTF-8 payloads), `--input-file`/`-i`, `--no-backup`, `--compresslevel`/`-c`, `--timeout`, `--progress`, `--version`, pre-validated destination deduplication and source-equals-destination rejection, concise redacted exit-1 operational error reporting without tracebacks, and automatic URL credential/query redaction.
- PEP 562 lazy import architecture, standalone benchmark suite (`benchmarks/bench_openas.py`), comprehensive unit/regression/golden test suite, and cold-start project documentation (`ARCHITECTURE.md`, `FUNCTIONAL_SPEC.md`, `DECISIONS.md`, `GUIDING_PRINCIPLES.md`, `docs/API.md`, `docs/CLI.md`, `CONTRIBUTING.md`, `CITATION.cff`, `LICENSE`, `NOTICE`).

[0.1.0]: https://github.com/fariello/openas/releases/tag/v0.1.0
