# openas

A lightweight Python library and CLI for streaming plain and compressed files (`.gz`, `.bz2`, `.xz`, `.zst`) line-by-line or in chunks across local paths and remote protocols (`http://`, `https://`, `ftp://`, `ssh://`, `sftp://`), in both UTF-8 text (`str`) and binary (`bytes`) modes.

## Background and Philosophy

`openas` started around 2007 as an internal utility to read and write compressed files (such as logs, CSVs, and JSONL) line-by-line without decompressing full files into memory or writing separate boilerplate for each codec and transport. It has now been extracted into a standalone Apache-2.0 package.

**How `openas` compares to `smart_open` and `fsspec`:**
`smart_open` and `fsspec` also support streaming compressed files line-by-line and excel at cloud object stores (`s3://`, `gs://`, Azure) or full virtual filesystem APIs. `openas` focuses on a smaller, self-contained scope:

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
pip install openas
```

*Runtime dependencies installed automatically:* `paramiko`, `zstandard`, `requests`, `tqdm`, and `ftputil` (imported lazily on first use).

## Quick Start

Use the unified `openas()` function for local or remote compressed streams in text (`str`) or binary (`bytes`) mode.

### Basic Usage

```python
from openas import openas

# 1. Write a compressed text file (optional compresslevel tunes speed vs ratio)
with openas("events.jsonl.zst", "w", compresslevel=3) as f:
    f.write('{"event": "start"}\n')

# 2. Append to the compressed file without creating a .bu backup
with openas("events.jsonl.zst", "a") as f:
    f.writelines(['{"event": "tick"}\n', '{"event": "stop"}\n'])

# 3. Stream the compressed text file back line-by-line
with openas("events.jsonl.zst", "r") as f:
    for line in f:
        print(line.strip())

# 4. Write and stream back a raw binary compressed file (including non-UTF-8 bytes)
with openas("payload.bin.gz", "wb", backup=False) as f:
    f.write(b"\x00\xffrecord1\nrecord2\n")

with openas("payload.bin.gz", "rb") as f:
    for raw_line in f:
        print(raw_line)
```

## Capabilities and Support Matrix

`openas` handles single-file plain and compressed streams and rejects multi-file container archives to prevent corruption.

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

Pass remote URLs directly; `openas` detects `.gz`, `.bz2`, `.xz`, and `.zst` from the URL path (ignoring query strings):

```python
from openas import openas

# Write over SSH/SFTP using SSH agent / ~/.ssh keys (preferred over inline passwords)
with openas("sftp://user@hostname/path/to/data.csv.zst", "w", backup=True, timeout=30) as f:
    f.write("id,name\n1,alice\n")

# Stream a remote compressed HTTP file line-by-line
with openas("https://example.com/data.jsonl.gz", "r", timeout=30) as f:
    for line in f:
        print(line.strip())
```

> **Credential safety:** Prefer SSH agent or `~/.ssh` key authentication (`sftp://user@host/path/to/file`) over inline passwords (`user:pass@host`). Avoid putting plaintext passwords on command lines where they can appear in shell history or process listings. `openas` redacts URL passwords and query strings in CLI output, `__repr__()`, and archive error messages.

> **Remote URL paths, authentication, and environment variables:** `sftp://` and `ssh://` URLs pass the URL path verbatim as an absolute remote path (for example `/path/to/data.csv.zst`) with no `~/` expansion and do not parse `~/.ssh/config`. Omitting the SSH username defaults to the local OS user authenticating via the SSH agent or `~/.ssh/id_*` keys; omitting FTP credentials logs in anonymously. `openas` defines no custom environment variables, while HTTP/HTTPS inherits `requests.Session` `trust_env=True` defaults (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`, `REQUESTS_CA_BUNDLE`, `CURL_CA_BUNDLE`, and `~/.netrc`).

## Command-Line Interface

Run the built-in CLI via `openas` (installed console script) or `python -m openas`:

```bash
# Show help and version
python -m openas --help
python -m openas --version

# Stream compressed files and report line/byte throughput
python -m openas --read path/to/large.zst

# Single-pass broadcast (reads source once and writes to one or more destinations)
python -m openas --write https://example.com/data.csv.gz local_copy.txt sftp://user@host/path/to/dest.csv.zst
```

## Development and Benchmarks

Set up a local development environment (Python 3.9+):

```bash
git clone https://github.com/fariello/openas.git
cd openas
pip install -e ".[dev]"
python -m pytest -v
```

Run the optional standalone performance benchmark suite (all temporary files stay inside `tempfile.TemporaryDirectory()`):

```bash
python benchmarks/bench_openas.py
```

## Documentation

- [Public API Reference](docs/API.md): Signatures, text and binary modes, parameters, exceptions, and handle lifecycle semantics.
- [CLI Guide](docs/CLI.md): `--read`, `--write`, `--input-file` / `-i`, `--version`, and broadcast behavior.
- [Architecture Overview](ARCHITECTURE.md): Stream layering, transport wrappers, sequential `.bu` backups, and test structure.
- [Functional Specification](FUNCTIONAL_SPEC.md): Behavioral contracts, edge cases, non-goals, and security boundaries.
- [Design Decisions](DECISIONS.md): Rationale and trade-offs behind load-bearing invariants.
- [Guiding Principles](GUIDING_PRINCIPLES.md): Core engineering boundaries of `openas`.
- [Changelog](CHANGELOG.md): Release history.
- [Contributing](CONTRIBUTING.md): Development workflow and testing expectations.

## Troubleshooting and Tips

- **SSH `known_hosts` / `RejectPolicy` errors:** `openas` loads system host keys (`load_system_host_keys()`) and enforces Paramiko's `RejectPolicy` to protect against Man-in-the-Middle attacks. If a connection fails because the host key is unknown, verify the server's fingerprint out of band and add the verified host key to `~/.ssh/known_hosts` (for example via an interactive `ssh` check or `ssh-keyscan` after verifying the fingerprint). Do not disable host-key verification.
- **Special characters in URL credentials or paths:** Percent-encode reserved characters (`@` as `%40`, `:` as `%3A`, `/` as `%2F`, space as `%20`, `%` as `%25`) in `ftp://`, `ssh://`, and `sftp://` URLs (for example `sftp://user@host/path/to/logs%202026/data.gz`, or if inline credentials cannot be avoided, `ftp://user:p%40ss@host/path/to/data.txt`). `openas` decodes FTP/SSH/SFTP credentials and paths automatically via `urllib.parse.unquote()`. Prefer SSH agent or `~/.ssh` key authentication over inline passwords.
- **`UnsupportedArchiveError` on `.zip` or `.tar.*` files:** `openas` handles single-file streams and rejects multi-file container archives to prevent corrupting archive headers or directory tables. Use Python's `zipfile` module for `.zip` archives and `tarfile` for `.tar` and `.tar.*` archives.
- **Streaming multi-GB `.zst` and `.gz` corpora in `'rb'` mode:** When downstream parsers accept `bytes` directly (for example JSON or byte-level log parsers), open large compressed files with `'rb'` instead of `'r'`. Binary mode avoids per-line UTF-8 decoding overhead and uses C-level `io.BufferedReader` line scanning; benchmark your own environment with `benchmarks/bench_openas.py`.

## Known Limitations

- **Sequential Streaming Only:** Streams do not support random-access `seek()`/`tell()`, read-write modes (`r+`, `w+`, `x`), or asynchronous (`async`/`await`) I/O.
- **Cloud Object Store Exclusions:** Cloud schemes (`s3://`, `gs://`, `az://`, `hdfs://`) raise `UnsupportedProtocolError`. Use `smart_open`, `fsspec`, or dedicated cloud SDKs instead.
- **Write Progress Tracking (v0.1.0):** `show_progress=True` is implemented for `OpenAsReader` only; passing `show_progress=True` to `OpenAsWriter` or write/append modes raises `NotImplementedError`.
- **FTP Append Not Supported:** `ftp://` supports read and write only; append mode raises `UnsupportedProtocolError` because `ftputil` does not support FTP append.
- **Synchronous CLI Fan-Out:** Multi-target `--write` reads the source once in 64 KiB binary chunks via `contextlib.ExitStack` and writes exact bytes synchronously to each destination. Use the Python API with threads or processes for concurrent remote uploads.

## License and Citation

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

If you use `openas` in academic work, technical reports, or publications, please cite it using [CITATION.cff](CITATION.cff).
