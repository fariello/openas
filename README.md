# polyopen

A lightweight Python library and CLI for streaming plain and compressed files (`.gz`, `.bz2`, `.xz`, `.zst`) line-by-line or in chunks across local paths and remote protocols (`http://`, `https://`, `ftp://`, `ssh://`, `sftp://`), in both UTF-8 text (`str`) and binary (`bytes`) modes.

## Background and Philosophy

`polyopen` started around 2007 as an internal utility to read and write compressed files (such as logs, CSVs, and JSONL) line-by-line without decompressing full files into memory or writing separate boilerplate for each codec and transport. It has now been extracted into a standalone Apache-2.0 package.

**How `polyopen` compares to `smart_open` and `fsspec`:**
`smart_open` and `fsspec` also support streaming compressed files line-by-line and excel at cloud object stores (`s3://`, `gs://`, Azure) or full virtual filesystem APIs. `polyopen` focuses on a smaller, self-contained scope:

- **Unified line-by-line compressed streaming across local and POSIX/network transports:** Reads and writes plain, `.gz`, `.bz2`, `.xz`, and multi-frame `.zst` streams line-by-line in both UTF-8 text (`str`) and raw binary (`bytes`) modes, without requiring manual `io.TextIOWrapper` or `io.BufferedReader` glue (including for `zstandard`, whose raw `stream_reader` does not support `readline()` directly).
- **No cloud SDKs and fast startup:** Explicitly excludes cloud object stores (`s3://`, `gs://`, `az://`, `hdfs://`) so it never pulls in cloud SDKs (`boto3`, GCS, Azure). Optional standard-library codecs (`gzip`, `bz2`, `lzma`, `ftplib`, `argparse`, `urllib.parse`) and runtime transport libraries (`paramiko`, `ftputil`, `requests`, `zstandard`, `tqdm`) are imported lazily on first use, while all five runtime dependencies install by default so every documented protocol and codec works immediately.
- **Built-in transfer tuning, progress, and overwrite safety:** Includes bounded 4 MiB sliding-window SFTP read streaming and write pipelining, thread-local cookie-free HTTP session reuse, raw-byte `tqdm` progress bars (`show_progress=True`), optional sequential `.NNN.bu` backups on overwrite, and a single-pass 64 KiB binary-chunk multi-destination CLI (`--read` / `--write`).

See [GUIDING_PRINCIPLES.md](GUIDING_PRINCIPLES.md) and [DECISIONS.md](DECISIONS.md) for design boundaries and architectural trade-offs.

## Features

- **Line-by-Line Streaming and Transparent Compression:** Streams plain, `.gz`, `.bz2`, `.xz`, and multi-frame `.zst` files on the fly in UTF-8 text (`'r'`/`'rt'`, `'w'`/`'wt'`, `'a'`/`'at'`) or raw binary (`'rb'`, `'wb'`, `'ab'`) mode. Accepts optional `compresslevel: Optional[int] = None` (defaulting to each library's own setting: `zstd` level 3, `lzma` preset 6, `bz2` level 9, `gzip` level 9).
- **Local and Remote Protocols:** Reads and writes over local files (`file://` or plain paths), `ssh://`/`sftp://` (read, write, and append), and `ftp://` (read and write), and streams read-only `http://`/`https://` responses via thread-local, cookie-free `requests.Session` reuse. Supports an optional `timeout` parameter across all network transports.
- **SFTP Pipelining, Progress Bars, and Clean Shutdown:** Uses bounded 4 MiB sliding-window SFTP `readv()` streaming (`max_concurrent_prefetch_requests=64`, O(1) memory on multi-GB files), SFTP write pipelining (`set_pipelined(True)`), optional `tqdm` progress bars tracking exact raw bytes read (`show_progress=True`), and deterministic, idempotent handle closure across all transports.
- **Standard File-Like Interface:** Supports `with` context management, `read()`, `readline()`, `readlines()`, line iteration (`for line in f:`), `write()`, `writelines()`, `flush()`, `closed`, `readable()`, `writable()`, `seekable()`, and `encoding` (`'utf-8'` in text mode, `None` in binary mode).
- **Optional Sequential `.bu` Overwrite Backups:** When overwriting an existing file (`backup=True`, default for write modes; pass `backup=False` to disable), moves the existing file to `<path>.001.bu` (checking `.001.bu` first in O(1), then scanning once up to `.999.bu` with a nanosecond timestamp fallback). Local backups use atomic `os.link`/`os.unlink` and reject directories (`IsADirectoryError`).

## Installation

Requires Python 3.9 or newer:

```bash
pip install polyopen
```

*Runtime dependencies installed automatically:* `paramiko`, `zstandard`, `requests`, `tqdm`, and `ftputil` (imported lazily on first use).

## Quick Start

Use the unified `polyopen()` function for local or remote compressed streams in text (`str`) or binary (`bytes`) mode.

### Basic Usage

```python
from polyopen import polyopen

# 1. Stream a compressed text file line-by-line (local or remote)
with polyopen("events.jsonl.zst", "r") as f:
    for line in f:
        process(line)

# 2. Write a compressed file (optional compresslevel tunes speed vs ratio)
with polyopen("output.bz2", "w", compresslevel=6) as f:
    f.write("Hello, World!\n")

# 3. Append without creating a .bu backup file
with polyopen("output.bz2", "a") as f:
    f.writelines(["Appended line 1.\n", "Appended line 2.\n"])

# 4. Read and write raw binary streams (including non-UTF-8 payloads)
with polyopen("payload.bin.gz", "wb", backup=False) as f:
    f.write(b"\x00\xffrecord1\nrecord2\n")

with polyopen("payload.bin.gz", "rb") as f:
    for raw_line in f:
        process_bytes(raw_line)
```

## Capabilities and Support Matrix

`polyopen` handles single-file plain and compressed streams and rejects multi-file container archives to prevent corruption.

### Supported Stream Formats and Protocols

| Format / Protocol | Read | Write | Append | Notes |
| :--- | :---: | :---: | :---: | :--- |
| Plain Files (`.txt`, `.jsonl`, `.bin`, etc.) | Yes | Yes | Yes | UTF-8 text (`str`) or binary (`bytes`) streams |
| Zstandard (`.zst`) | Yes | Yes | Yes | Multi-frame line streaming; default write level 3 (tunable via `compresslevel`) |
| GZIP (`.gz`) | Yes | Yes | Yes | Standard Python `gzip`; default write level 9 (tunable via `compresslevel`) |
| BZIP2 (`.bz2`) | Yes | Yes | Yes | Standard Python `bz2`; default write level 9 (tunable via `compresslevel`) |
| LZMA (`.xz`) | Yes | Yes | Yes | Standard Python `lzma`; default write preset 6 (tunable via `compresslevel`) |
| Local File (`file://` or implicit) | Yes | Yes | Yes | Atomic `os.link`/`os.unlink` sequential `.bu` backups with `.001.bu` fast-path |
| SSH / SFTP (`ssh://`, `sftp://`) | Yes | Yes | Yes | System host-key verification, bounded 4 MiB SFTP `readv()` streaming, write pipelining, sequential `.bu` backups |
| FTP (`ftp://`) | Yes | Yes | No | Read and write with sequential `.bu` backups; append raises `UnsupportedProtocolError` |
| HTTP / HTTPS (`http://`, `https://`) | Yes | No | No | Thread-local cookie-free session reuse; write/append raises `ReadOnlyProtocolError` |

### Unsupported Archive Formats

Container archives embed directory tables and multi-file headers rather than a single data stream. Passing any of these extensions raises `UnsupportedArchiveError`:

| Container Extensions | Supported | Reason | Alternative |
| :--- | :---: | :--- | :--- |
| `.zip` | No | Stream writes overwrite the Central Directory table | Python `zipfile` |
| `.tar`, `.tar.gz`, `.tgz`, `.tar.bz2`, `.tbz`, `.tbz2`, `.tar.xz`, `.txz`, `.tar.zst`, `.tzst`, `.tar.lzma`, `.tlz`, `.gz.tar`, `.bz2.tar`, `.xz.tar`, `.zst.tar` | No | Contains POSIX tar archive headers | Python `tarfile` |
| `.rar`, `.7z` | No | Multi-file container formats, not single streams | Dedicated archive libraries |

### Remote Streaming Examples

Pass remote URLs directly; `polyopen` detects `.gz`, `.bz2`, `.xz`, and `.zst` from the URL path (ignoring query strings):

```python
from polyopen import polyopen

# Write over SSH/SFTP using SSH agent / ~/.ssh keys (preferred over inline passwords)
with polyopen("sftp://user@hostname/data.csv.zst", "w", backup=True, timeout=30) as f:
    f.write("id,name\n1,alice\n")

# Stream a remote compressed HTTP file line-by-line
with polyopen("https://example.com/data.jsonl.gz", "r", timeout=30) as f:
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

# Stream compressed files and report line/byte throughput
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

- [Public API Reference](docs/API.md): Signatures, text and binary modes, parameters, exceptions, and handle lifecycle semantics.
- [CLI Guide](docs/CLI.md): `--read`, `--write`, `--input-file` / `-i`, `--version`, and broadcast behavior.
- [Architecture Overview](ARCHITECTURE.md): Stream layering, transport wrappers, sequential `.bu` backups, and test structure.
- [Functional Specification](FUNCTIONAL_SPEC.md): Behavioral contracts, edge cases, non-goals, and security boundaries.
- [Design Decisions](DECISIONS.md): Rationale and trade-offs behind load-bearing invariants.
- [Guiding Principles](GUIDING_PRINCIPLES.md): Core engineering boundaries of `polyopen`.
- [Changelog](CHANGELOG.md): Release history.
- [Contributing](CONTRIBUTING.md): Development workflow and testing expectations.

## Known Limitations

- **Sequential Streaming Only:** Streams do not support random-access `seek()`/`tell()`, read-write modes (`r+`, `w+`, `x`), or asynchronous (`async`/`await`) I/O.
- **Cloud Object Store Exclusions:** Cloud schemes (`s3://`, `gs://`, `az://`, `hdfs://`) raise `UnsupportedProtocolError`. Use `smart_open`, `fsspec`, or dedicated cloud SDKs instead.
- **Write Progress Tracking (v0.1.0):** `show_progress=True` is implemented for `PolyReader` only; passing `show_progress=True` to `PolyWriter` or write/append modes raises `NotImplementedError`.
- **FTP Append Not Supported:** `ftp://` supports read and write only; append mode raises `UnsupportedProtocolError` because `ftputil` does not support FTP append.
- **Synchronous CLI Fan-Out:** Multi-target `--write` reads the source once in 64 KiB binary chunks via `contextlib.ExitStack` and writes exact bytes synchronously to each destination. Use the Python API with threads or processes for concurrent remote uploads.

## License and Citation

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

If you use `polyopen` in academic work, technical reports, or publications, please cite it using [CITATION.cff](CITATION.cff).
