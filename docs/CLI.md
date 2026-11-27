# Command-Line Interface (CLI) Guide

*[Return to Main README](../README.md)*

`openas` provides a built-in CLI for two tasks:

1. Reading local or remote compressed streams and reporting line and byte throughput (`--read`).
2. Copying one source stream to one or more local or remote destinations in a single pass (`--write`).

## Invocation

After installing `openas`, invoke the CLI with any of:

```bash
openas [-h] [--version] [--input-file INPUT_FILE] (--write | --read) [file ...]
python -m openas [-h] [--version] [--input-file INPUT_FILE] (--write | --read) [file ...]
python -m openas.openas [-h] [--version] [--input-file INPUT_FILE] (--write | --read) [file ...]
```

Prefer `openas` or `python -m openas` to avoid Python's `runpy` `RuntimeWarning` when executing a submodule already imported by `__init__.py`.

## Flags and Arguments

- `file ...` (Positional): Target paths or URLs.
  - With `--read`: one or more files to read sequentially. Passing zero files exits non-zero via `parser.error`.
  - With `--write` (without `--input-file`): `file[0]` is the source and `file[1:]` are destinations (at least two paths required).
  - With `--write --input-file SRC`: all positional arguments are destinations (at least one destination required).
- `--read`, `-r` (Mutually Exclusive): Reads every positional file in 64 KiB binary chunks and reports `b'\n'`-delimited line count (empty files report 0 lines; non-empty files without a trailing newline count the final line), 1-decimal base-1024 human-readable byte size (`B` through `YB`), line rate, byte rate (empty files or zero-byte rates format as `(0.0B/s)`), and elapsed seconds.
- `--write`, `-w` (Mutually Exclusive): Pre-validates destination protocol and archive guards, opens the source and all destinations in binary mode inside a single `contextlib.ExitStack`, reads the source once in 64 KiB chunks, writes exact bytes (preserving CRLF and arbitrary non-UTF-8 binary payloads) synchronously to each destination (moving existing destinations to sequential `.bu` backups), and closes all handles on error.
- `--input-file INPUT_FILE`, `-i INPUT_FILE`: Explicit source path or URL for `--write` mode. Rejected via `parser.error` if combined with `--read` or if no destination is given.
- `--version`: Prints `openas <version>` and exits 0.

## Exit Codes

- `0`: Command completed successfully, or `--help` / `--version` was printed.
- `1`: Operational guard, I/O, compression, or remote transport failure during execution (for example `UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`, `FileNotFoundError`, `IsADirectoryError`, corrupt compressed stream errors, or remote connection errors). Printed concisely to `stderr` as `openas: error (<ExcName>): <message>` without a raw Python traceback. Unexpected internal errors still propagate with a full traceback.
- `2`: Usage or argument validation error reported by `argparse` / `parser.error` (for example missing `--read`/`--write`, zero files passed to `--read`, fewer than two positional files passed to `--write` without `--input-file`, `--input-file` combined with `--read`, or `--input-file` with no destinations).

## Credential Redaction

URL passwords and query strings are automatically masked (`user:***@host/path?<redacted>`) in both CLI stdout messages and formatted CLI stderr error lines. Even so, avoid passing inline passwords on shell command lines because they can remain visible in shell history and process listings; prefer SSH keys (`sftp://user@host/path/to/file`).

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
- **Unexposed Python API options:** CLI flags do not expose `--no-backup`, `--compresslevel`, `--timeout`, or `--progress`. The CLI always uses `backup=True` on writes, `compresslevel=None` (each codec's default), `timeout=None`, and no progress bar; use the Python `openas()` API to customize these options.
