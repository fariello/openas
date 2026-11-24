# Polyopen

A Python module for versatile file handling, providing unified stream-level access to both local and remote files with transparent compression support.

Extracted and refined from the monolithic `pylib` workspace, `polyopen` is designed to stand alone as an easy-to-use utility library for fetching and writing data across a variety of protocols and formats.

## Background & Philosophy

`polyopen` was originally developed circa 2007 alongside a larger private monolithic repository. At the time, no intuitive unified solution existed in the Python ecosystem to seamlessly juggle disparate compression formats and remote stream protocols behind a single standard API. It has been battle-tested (famous last words) and refined for personal usage for over a decade before being fully extracted into this dedicated, standalone package.

**Why not just use `smart_open`?**
While the fantastic `smart_open` library eventually emerged to solve a similar problem space, `polyopen` is maintained to provide a distinct, highly-focused alternative:
- **Lightweight Architecture:** `smart_open` is inherently wired for massive cloud data lakes, utilizing heavy overarching SDKs (`boto3`, GCS, etc.) to manage complex object chunking. `polyopen` strictly eschews massive cloud architectures in favor of blistering-fast import times, strictly lazy-loaded dependencies, and base-level infrastructure (Linux protocols, HTTP, FTP).
- **Inherent Resiliency:** `polyopen` uniquely features automatic backup rotation. When explicitly overwriting destination datasets locally or over SSH/FTP, `polyopen` seamlessly rolls sequential `.bu` backwards backups without forcing you to write complex filesystem manipulation logic.

## Features

- **Transparent Compression**: Reads and writes `.gz`, `.bz2`, `.xz`, and `.zst` files on the fly, with optional `compresslevel: Optional[int] = None` forwarding (when omitted or `None`, each compressor uses its library default: `zstd` level 3, `lzma` preset 6, `bz2` level 9, `gzip` level 9).
- **Multiple Protocols**: Reads from and writes to remote targets over `FTP`, `SSH`, and `SFTP`, and streams reads over `HTTP` and `HTTPS` using thread-local, cookie-free `requests.Session` reuse and `io.TextIOWrapper(response.raw)` streaming.
- **Fast, Collision-Safe Backups**: Checks `<path>.001.bu` in O(1) before falling back to a single directory listing (`os.scandir` locally, single `listdir` on SFTP/FTP), capping sequential backups at `.999.bu` with a collision-safe nanosecond timestamp fallback.
- **SFTP Pipelining and Clean Shutdown**: Uses bounded SFTP read prefetch (`max_concurrent_requests=64`), SFTP write pipelining (`set_pipelined(True)`), and deterministic, idempotent handle closure across all transports.
- **Lazy Imports and Fast Stream Paths**: Defers optional standard library and remote transport imports until needed, delegates uncompressed/compressed text iteration directly to `io.TextIOBase` when progress tracking is off, and supports `PolyWriter.writelines()`.
- **Easy Context Management**: Supports standard `with` statements, acting like Python's built-in `open()`.

## Installation

Install using standard Python package management (`pip`):

```bash
pip install polyopen
```

(*Note*: Depends on `paramiko`, `zstandard`, `requests`, `tqdm`, and `ftputil`.)

## Quick Start

The easiest way to use the library is with the unified `polyopen()` wrapper, which automatically routes to a `PolyReader` or `PolyWriter` and acts like Python's built-in `open()`.

You can use it universally for both standard I/O and remote/compressed files.

### Basic Usage

```python
from polyopen import polyopen

# 1. Writing to a local compressed file (optional compresslevel tunes speed vs ratio)
with polyopen("output.bz2", 'w', compresslevel=6) as f:
    f.write("Hello, World!\n")

# 2. Appending without producing backup files
with polyopen("output.bz2", 'a') as f:
    f.writelines(["Appended line 1.\n", "Appended line 2.\n"])

# 3. Reading it back efficiently
with polyopen("output.bz2", 'r') as f:
    for line in f:
        print(line.strip())
```

## Capabilities & Support Matrix

Because `polyopen` is designed absolutely for stream-based parsing, it supports appending natively to compatible stream formats, but actively restricts container archives to prevent unintentional corruption.

### Supported Stream Formats
| Format / Protocol | Read? | Write? | Append? | Notes |
| :--- | :---: | :---: | :---: | :--- |
| Plain Text (`.txt`, `.csv`, etc.) | ✓ | ✓ | ✓ | Standard UTF-8 streams |
| Zstandard (`.zst`) | ✓ | ✓ | ✓ | Multi-frame reads; default write level 3 (tunable via `compresslevel`) |
| GZIP (`.gz`) | ✓ | ✓ | ✓ | Standard Python `gzip`; default write level 9 (tunable via `compresslevel`) |
| BZIP2 (`.bz2`) | ✓ | ✓ | ✓ | Standard Python `bz2`; default write level 9 (tunable via `compresslevel`) |
| LZMA (`.xz`) | ✓ | ✓ | ✓ | Standard Python `lzma`; default write preset 6 (tunable via `compresslevel`) |
| Local File (`file://` or implicit) | ✓ | ✓ | ✓ | Atomic `os.link`/`os.unlink` backup rotation with `.001.bu` fast-path |
| SSH / SFTP (`ssh://`, `sftp://`) | ✓ | ✓ | ✓ | Bounded read prefetch (`max_concurrent_requests=64`), write pipelining, backups |
| FTP (`ftp://`) | ✓ | ✓ | ✓ | Standard FTP transfer with `.001.bu` fast-path and single-listing backups |
| HTTP / HTTPS (`http://`, `https://`) | ✓ | ✘ | ✘ | Thread-local cookie-free session reuse, `response.raw` streaming; write raises `ReadOnlyProtocolError` |

### Unsupported Archive Formats
These formats are **Archival Containers** (miniature file systems), not uniform text streams. If passed to `polyopen`, they will instantly raise an `UnsupportedArchiveError` to protect you from inadvertently corrupting them.

| Container Extension | Supported? | Reason | Alternative |
| --- | :---: | --- | --- |
| `.zip` | ✘ | Writing streams overwrites the Central Directory index map, corrupting the file. | Use Python's built-in `zipfile` |
| `.tar` | ✘ | Contains POSIX file headers and multiple internal hierarchy layers. | Use Python's built-in `tarfile` |
| `.rar`, `.7z` | ✘ | Binary filesystem maps rather than single-file text compression streams. | Use 3rd party archive managers |
| `.tar.gz`, `.tgz` | ✘ | Sits underneath a `.tar` extraction layer. | Use Python's built-in `tarfile` |

### Advanced Networking

Pass network protocols seamlessly without changing syntax. (`polyopen` will natively `.gz`, `.bz2`, and `.zst` wrap via the file extension)

```python
from polyopen import polyopen

# Write directly to a remote SSH/sftp box (auto-maintains .bu backups on overwrite!)
with polyopen("sftp://username:password@hostname/data.csv.zst", 'w', backup=True) as f:
    f.write("id,name\n1,john\n")

# Read a remote HTTP Stream
with polyopen("http://example.com/data.txt.gz", 'r') as f:
    for line in f:
        print(line.strip())
```

## Built-in Test CLI
You can test reading and single-pass multi-target writing directly without a Python script:

```bash
# Read a compressed file and report line/byte throughput
python -m polyopen.polyopen --read path/to/large.zst

# Single-pass broadcast (reads source once and writes to one or more destinations)
python -m polyopen.polyopen --write http://source.com/data.csv.gz local_copy.txt sftp://user:pass@host/dest.csv.zst
```

## Development
To set up a local development environment:

```bash
git clone <repository_url>
cd polyopen
pip install -e .[dev] pytest
python -m pytest tests/
```

## Full Documentation & Architecture

For details on transport routing, API contracts, and CLI behavior, see:
- [Public API Specs](docs/API.md): Function and class signatures, `compresslevel` defaults, exception hierarchy, and handle closure semantics.
- [CLI Behaviors](docs/CLI.md): `--read`, `--write`, `--input-file` / `-i` flags, and single-pass broadcast behavior.
- [Architecture Overview](ARCHITECTURE.md): Transport wrappers, SFTP pipelining, HTTP streaming, backup rotation, and test topology.
- [Functional Specification](FUNCTIONAL_SPEC.md): Operational contracts, edge cases, and security boundaries.

## Acknowledgements and Limitations

While the module covers common local and remote streaming workflows, note these explicit boundaries:
- **Cloud Object Store Exclusions**: Cloud object schemes (`s3://`, `gs://`, `az://`, `hdfs://`) are explicitly rejected with `UnsupportedProtocolError`. Use `smart_open` or dedicated cloud SDKs for those stores.
- **Write Tracking Limitation (v0.1.0)**: `show_progress=True` tracks raw bytes read across `PolyReader` streams (batching `tqdm` updates at 64 KiB and falling back to `total=None` when size is unknown), and raises `NotImplementedError` on `PolyWriter` and write/append modes.
- **Backup Rotation Cap and Atomicity**: Backup rotation checks `<path>.001.bu` first in O(1); if occupied, it lists the parent directory once (`os.scandir` locally, single `listdir` on SFTP/FTP) to select the lowest free slot up to `.999.bu`, then falls back to collision-safe nanosecond timestamps (`time.time_ns()`). Local backups use atomic `os.link()` + `os.unlink()` (with `os.rename()` fallback only when hard links are unsupported) to prevent concurrent overwrite races.
- **Synchronous CLI Fan-Out**: Multi-target `--write` reads the source once via `contextlib.ExitStack` and writes synchronously to each destination, closing all handles on error. Use the Python API with threads or processes if you need concurrent remote uploads.
