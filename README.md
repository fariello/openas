# polyopen

A lightweight Python module for unified UTF-8 text stream I/O across local files, remote protocols (`http://`, `https://`, `ftp://`, `ssh://`, `sftp://`), and transparent compression (`.gz`, `.bz2`, `.xz`, `.zst`), with automatic `.bu` backup rotation on overwrite.

## Background and Philosophy

`polyopen` originated around 2007 as an internal utility for reading and writing compressed text streams across local and remote filesystems behind one Pythonic interface, and has now been extracted into a standalone Apache-2.0 package.

**Why use `polyopen` instead of `smart_open`?**
`smart_open` is an excellent library designed for cloud object stores (`s3://`, `gs://`, Azure) and cloud SDKs. `polyopen` serves a smaller, focused scope:

- **Fast import times and zero cloud SDKs:** Optional standard-library codecs (`gzip`, `bz2`, `lzma`, `ftplib`, `argparse`, `urllib.parse`) and transport libraries (`paramiko`, `ftputil`, `requests`, `zstandard`, `tqdm`) are imported lazily on first use. Note that all five runtime dependencies are installed by default so every documented protocol and codec works out of the box without extra install flags.
- **Built-in backup rotation:** Overwriting an existing file locally or over SSH/SFTP/FTP automatically rotates the prior file to `<path>.001.bu` (and sequential `.002.bu`..`.999.bu` slots with a collision-safe nanosecond fallback), reducing accidental data loss.

See [GUIDING_PRINCIPLES.md](GUIDING_PRINCIPLES.md) and [DECISIONS.md](DECISIONS.md) for the project's design boundaries and architectural trade-offs.

## Features

- **Transparent Compression:** Reads and writes `.gz`, `.bz2`, `.xz`, and `.zst` UTF-8 text streams on the fly, with optional `compresslevel: Optional[int] = None` forwarding (when omitted or `None`, each codec uses its library default: `zstd` level 3, `lzma` preset 6, `bz2` level 9, `gzip` level 9).
- **Multiple Protocols:** Reads and writes over local files (`file://` or plain paths), `ssh://`/`sftp://`, and `ftp://` (read and write), and streams read-only `http://`/`https://` responses via thread-local, cookie-free `requests.Session` reuse. Supports an optional `timeout` parameter across all network transports.
- **Fast, Collision-Safe Backups:** Checks `<path>.001.bu` first in O(1) before falling back to a single directory listing (`os.scandir` locally, single `listdir` on SFTP/FTP), capping sequential backups at `.999.bu` with a nanosecond timestamp fallback. Local backups use atomic `os.link`/`os.unlink` and reject directories (`IsADirectoryError`).
- **SFTP Pipelining and Clean Shutdown:** Uses bounded SFTP read prefetch (`max_concurrent_requests=64`), SFTP write pipelining (`set_pipelined(True)`), and deterministic, idempotent handle closure across all transports.
- **Standard File-Like Interface:** Supports `with` context management, `read()`, `readline()`, `readlines()`, iteration, `write()`, `writelines()`, `flush()`, `closed`, `readable()`, `writable()`, `seekable()`, and `encoding`.

## Installation

Requires Python 3.9 or newer:

```bash
pip install polyopen
```

*Runtime dependencies installed automatically:* `paramiko`, `zstandard`, `requests`, `tqdm`, and `ftputil` (imported lazily on first use).

## Quick Start

Use the unified `polyopen()` function for local or remote compressed UTF-8 text streams.

> **Important (UTF-8 text only):** `polyopen` always decodes and encodes UTF-8 text (`str`). The binary mode strings `'rb'`, `'wb'`, and `'ab'` are accepted as compatibility aliases for text modes; they do **not** return or accept raw `bytes`.

### Basic Usage

```python
from polyopen import polyopen

# 1. Write a local compressed file (optional compresslevel tunes speed vs ratio)
with polyopen("output.bz2", "w", compresslevel=6) as f:
    f.write("Hello, World!\n")

# 2. Append without producing backup files
with polyopen("output.bz2", "a") as f:
    f.writelines(["Appended line 1.\n", "Appended line 2.\n"])

# 3. Read it back
with polyopen("output.bz2", "r") as f:
    for line in f:
        print(line.strip())
```

## Capabilities and Support Matrix

`polyopen` handles uniform text streams and explicitly rejects container archives to prevent corruption.

### Supported Stream Formats and Protocols

| Format / Protocol | Read | Write | Append | Notes |
| :--- | :---: | :---: | :---: | :--- |
| Plain Text (`.txt`, `.csv`, etc.) | Yes | Yes | Yes | UTF-8 text streams |
| Zstandard (`.zst`) | Yes | Yes | Yes | Multi-frame reads; default write level 3 (tunable via `compresslevel`) |
| GZIP (`.gz`) | Yes | Yes | Yes | Standard Python `gzip`; default write level 9 (tunable via `compresslevel`) |
| BZIP2 (`.bz2`) | Yes | Yes | Yes | Standard Python `bz2`; default write level 9 (tunable via `compresslevel`) |
| LZMA (`.xz`) | Yes | Yes | Yes | Standard Python `lzma`; default write preset 6 (tunable via `compresslevel`) |
| Local File (`file://` or implicit) | Yes | Yes | Yes | Atomic `os.link`/`os.unlink` backup rotation with `.001.bu` fast-path |
| SSH / SFTP (`ssh://`, `sftp://`) | Yes | Yes | Yes | System host-key verification, SFTP prefetch/pipelining, backups |
| FTP (`ftp://`) | Yes | Yes | No | Read and write with backups; append raises `UnsupportedProtocolError` |
| HTTP / HTTPS (`http://`, `https://`) | Yes | No | No | Thread-local cookie-free session reuse; write/append raises `ReadOnlyProtocolError` |

### Unsupported Archive Formats

Container archives embed directory tables and multi-file headers rather than a single text stream. Passing any of these extensions raises `UnsupportedArchiveError`:

| Container Extensions | Supported | Reason | Alternative |
| :--- | :---: | :--- | :--- |
| `.zip` | No | Stream writes overwrite the Central Directory table | Python `zipfile` |
| `.tar`, `.tar.gz`, `.tgz`, `.tar.bz2`, `.tbz`, `.tbz2`, `.tar.xz`, `.txz`, `.tar.zst`, `.tzst`, `.tar.lzma`, `.tlz`, `.gz.tar`, `.bz2.tar`, `.xz.tar`, `.zst.tar` | No | Contains POSIX tar archive headers | Python `tarfile` |
| `.rar`, `.7z` | No | Multi-file container formats, not text streams | Dedicated archive libraries |

### Remote Streaming Examples

Pass remote URLs directly; `polyopen` detects `.gz`, `.bz2`, `.xz`, and `.zst` from the URL path (ignoring query strings):

```python
from polyopen import polyopen

# Write over SSH/SFTP using SSH agent / ~/.ssh keys (preferred over inline passwords)
with polyopen("sftp://user@hostname/data.csv.zst", "w", backup=True, timeout=30) as f:
    f.write("id,name\n1,alice\n")

# Read a remote compressed HTTP stream
with polyopen("https://example.com/data.txt.gz", "r", timeout=30) as f:
    for line in f:
        print(line.strip())
```

> **Credential safety:** Prefer SSH agent or `~/.ssh` key authentication (`sftp://user@host/path`) over inline passwords (`user:pass@host`). Avoid putting plaintext passwords on command lines where they can appear in shell history or process listings. `polyopen` redacts URL passwords and query strings in CLI output, `__repr__()`, and archive error messages.

## Command-Line Interface

Run the built-in CLI via `polyopen` (installed console script) or `python -m polyopen`:

```bash
# Show help and version
python -m polyopen --help
python -m polyopen --version

# Read compressed files and report line/byte throughput
python -m polyopen --read path/to/large.zst

# Single-pass broadcast (reads source once and writes to one or more destinations)
python -m polyopen --write https://example.com/data.csv.gz local_copy.txt sftp://user@host/dest.csv.zst
```

## Development and Benchmarks

Set up a local development environment (Python 3.9+):

```bash
git clone https://github.com/fariello/polyopen.git
cd polyopen
pip install -e ".[dev]"
python -m pytest -v
```

Run the optional standalone performance benchmark suite (all temporary files stay inside `tempfile.TemporaryDirectory()`):

```bash
python benchmarks/bench_polyopen.py
```

## Documentation

- [Public API Reference](docs/API.md): Signatures, parameters, exceptions, and handle lifecycle semantics.
- [CLI Guide](docs/CLI.md): `--read`, `--write`, `--input-file` / `-i`, `--version`, and broadcast behavior.
- [Architecture Overview](ARCHITECTURE.md): Stream layering, transport wrappers, backup rotation, and test structure.
- [Functional Specification](FUNCTIONAL_SPEC.md): Behavioral contracts, edge cases, non-goals, and security boundaries.
- [Design Decisions](DECISIONS.md): Rationale and trade-offs behind load-bearing invariants.
- [Guiding Principles](GUIDING_PRINCIPLES.md): Core engineering boundaries of `polyopen`.
- [Changelog](CHANGELOG.md): Release history.
- [Contributing](CONTRIBUTING.md): Development workflow and testing expectations.

## Known Limitations

- **UTF-8 Text Only:** All modes read and write UTF-8 `str` (no raw `bytes` mode, no `seek()`/random access, no async I/O).
- **Cloud Object Store Exclusions:** Cloud schemes (`s3://`, `gs://`, `az://`, `hdfs://`) raise `UnsupportedProtocolError`. Use `smart_open` or dedicated cloud SDKs instead.
- **Write Progress Tracking (v0.1.0):** `show_progress=True` is implemented for `PolyReader` only; passing `show_progress=True` to `PolyWriter` or write/append modes raises `NotImplementedError`.
- **FTP Append Not Supported:** `ftp://` supports read and write only; append mode raises `UnsupportedProtocolError` because `ftputil` does not support FTP append.
- **SFTP Read Prefetch Memory:** When file size is known, `PolyReader` calls Paramiko's `prefetch(file_size, max_concurrent_requests=64)`. Paramiko buffers prefetched chunks in RAM until consumed, so slow consumers or early-exit reads on multi-GB SFTP files can accumulate memory.
- **Synchronous CLI Fan-Out:** Multi-target `--write` reads the source once via `contextlib.ExitStack` and writes synchronously to each destination. Use the Python API with threads or processes for concurrent remote uploads.

## License and Citation

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

If you use `polyopen` in academic work, technical reports, or publications, please cite it using [CITATION.cff](CITATION.cff).
