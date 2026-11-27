# Command-Line Interface (CLI) Guide

*[Return to Main README](../README.md)*

`openas` provides a built-in CLI for two tasks:

1. Reading local or remote compressed streams and reporting line and byte throughput (`--read`).
2. Copying one source stream to one or more local or remote destinations in a single pass (`--write`).

## Invocation

After installing `openas`, invoke the CLI with any of:

```bash
openas [-h] [--version] [--input-file INPUT_FILE] [--no-backup] [--compresslevel COMPRESSLEVEL] [--timeout TIMEOUT] [--progress] (--write | --read) [file ...]
python -m openas [-h] [--version] [--input-file INPUT_FILE] [--no-backup] [--compresslevel COMPRESSLEVEL] [--timeout TIMEOUT] [--progress] (--write | --read) [file ...]
python -m openas.openas [-h] [--version] [--input-file INPUT_FILE] [--no-backup] [--compresslevel COMPRESSLEVEL] [--timeout TIMEOUT] [--progress] (--write | --read) [file ...]
```

Prefer `openas` or `python -m openas` to avoid Python's `runpy` `RuntimeWarning` when executing a submodule already imported by `__init__.py`.

## Flags and Arguments

- `file ...` (Positional): Target paths or URLs.
  - With `--read`: one or more files to read sequentially. Passing zero files exits non-zero via `parser.error`.
  - With `--write` (without `--input-file`): `file[0]` is the source and `file[1:]` are destinations (at least two paths required).
  - With `--write --input-file SRC`: all positional arguments are destinations (at least one destination required).
- `--read`, `-r` (Mutually Exclusive): Reads every positional file in 64 KiB binary chunks and reports `b'\n'`-delimited line count (empty files report 0 lines; non-empty files without a trailing newline count the final line), 1-decimal base-1024 human-readable byte size (`B` through `YB`), line rate, byte rate (empty files or zero-byte rates format as `(0.0B/s)`), and elapsed seconds.
- `--write`, `-w` (Mutually Exclusive): Pre-validates `--timeout` and each destination before opening or rotating files (checking URL syntax/ports/paths, container-archive guards, read-only `http://`/`https://` protocols, `--compresslevel` ranges, existing non-symlink local directories with `IsADirectoryError`, and duplicate resolved destinations via `parser.error` exit code 2 with redacted URLs). Duplicate detection normalizes local paths (`./`, `file://`, and symlinked parent directories collide while distinct local symlinks such as `link.txt` and `real.txt` stay distinct) and remote URLs (`ssh://` and `sftp://` on the same host, effective port, and decoded path collide). Opens the source and all destinations in binary mode inside a single `contextlib.ExitStack`, reads the source once in 64 KiB chunks, writes exact bytes (preserving CRLF and arbitrary non-UTF-8 binary payloads) synchronously to each destination (moving existing destinations to sequential `.bu` backups unless `--no-backup` is passed), and closes all handles on error.
- `--input-file INPUT_FILE`, `-i INPUT_FILE`: Explicit source path or URL for `--write` mode. Rejected via `parser.error` (exit code 2) if combined with `--read` or if no destination is given.
- `--no-backup`: Disables sequential `.001.bu` backup creation on `--write` so existing destinations are overwritten in place. Rejected via `parser.error` (exit code 2) if combined with `--read`.
- `--compresslevel LEVEL`, `-c LEVEL`: Integer compression level or preset forwarded to compressed `--write` destinations (`.gz` 0..9, `.bz2` 1..9, `.xz` 0..9, `.zst` up to 22; ignored on plain files). Pre-validated across all destinations before opening or rotating any file (out-of-range values exit 1 with a concise `ValueError` message; non-integer values or use with `--read` exit 2 via `argparse` / `parser.error`).
- `--timeout SECONDS`: Finite positive network timeout in seconds forwarded to HTTP/HTTPS, FTP, and SSH/SFTP transfers on both `--read` and `--write`. Pre-validated before opening any stream (non-positive or non-finite values such as `0`, `-1`, `nan`, or `inf` exit 1 cleanly without touching outputs; non-numeric values exit 2 via `argparse`).
- `--progress`: Displays a `tqdm` raw-byte progress bar on `stderr` for the input stream (`OpenAsReader`) during `--read` and `--write`, keeping `stdout` summaries clean.
- `--version`: Prints `openas <version>` and exits 0.

## Exit Codes

- `0`: Command completed successfully, or `--help` / `--version` was printed.
- `1`: Operational guard, I/O, compression, or remote transport failure during execution (for example `UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`, `ValueError` from invalid `--timeout` or out-of-range `--compresslevel`, `FileNotFoundError`, `IsADirectoryError`, corrupt compressed stream errors, or remote connection errors). Printed concisely to `stderr` as `openas: error (<ExcName>): <message>` without a raw Python traceback. Unexpected internal errors still propagate with a full traceback.
- `2`: Usage or argument validation error reported by `argparse` / `parser.error` (for example missing `--read`/`--write`, zero files passed to `--read`, fewer than two positional files passed to `--write` without `--input-file`, `--input-file`/`--no-backup`/`--compresslevel` combined with `--read`, `--input-file` with no destinations, non-integer `--compresslevel`, non-numeric `--timeout`, or duplicate resolved `--write` destinations).

## Credential Redaction

URL passwords, non-numeric port suffixes on malformed URLs, and query strings are automatically masked (`user:***@host/path?<redacted>`) in CLI stdout messages, formatted CLI stderr error lines, and duplicate-destination usage errors. Even so, avoid passing inline passwords on shell command lines because they can remain visible in shell history and process listings; prefer SSH keys (`sftp://user@host/path/to/file`).

## Examples

### Reading Streams (`--read`)

```bash
python -m openas --read syslog.01.zst syslog.02.bz2 https://example.com/archive.gz
```

Example output:

```text
Reading syslog.01.zst
FINISHED Reading. syslog.01.zst. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
```

Because `--read` streams raw 64 KiB binary chunks, it also handles arbitrary non-UTF-8 binary files without decoding errors (counting `b'\n'`-delimited records):

```bash
python -m openas --read payload.bin.gz
```

For an empty file (0 bytes), the summary reports zero lines and `(0.0B/s)`:

```text
Reading empty.txt
FINISHED Reading. empty.txt. 0 lines / 0.0B at 0.0/s (0.0B/s). Total time: 0.00 Secs.
```

### Single-Pass Multi-Target Broadcast (`--write`)

```bash
# Positional form: first path is source, remaining paths are destinations
python -m openas --write dump.sql local_archive.gz local_archive.bz2 sftp://user@node/path/to/dump.zst

# Explicit --input-file (-i) form: all positional paths are destinations
python -m openas --write -i dump.sql local_archive.gz local_archive.bz2 sftp://user@node/path/to/dump.zst
```

Example output:

```text
Will Read from dump.sql
Writing local_archive.gz
Writing local_archive.bz2
Writing sftp://user@node/path/to/dump.zst
FINISHED Writing. local_archive.gz. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
FINISHED Writing. local_archive.bz2. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
FINISHED Writing. sftp://user@node/path/to/dump.zst. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
```

## Known Limitations

- **Synchronous fan-out:** `--write` reads the source once in 64 KiB binary chunks and writes each chunk synchronously to all destinations in sequence. Use the Python `openas()` API with threads or processes if you need concurrent remote uploads.
