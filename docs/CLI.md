# Command-Line Interface (CLI) Guide

*[Return to Main README](../README.md)*

`polyopen` provides a built-in CLI for two tasks:

1. Reading local or remote compressed streams and reporting line and byte throughput (`--read`).
2. Copying one source stream to one or more local or remote destinations in a single pass (`--write`).

## Invocation

After installing `polyopen`, invoke the CLI with any of:

```bash
polyopen [-h] [--version] [--input-file INPUT_FILE] (--write | --read) [file ...]
python -m polyopen [-h] [--version] [--input-file INPUT_FILE] (--write | --read) [file ...]
python -m polyopen.polyopen [-h] [--version] [--input-file INPUT_FILE] (--write | --read) [file ...]
```

Prefer `polyopen` or `python -m polyopen` to avoid Python's `runpy` `RuntimeWarning` when executing a submodule already imported by `__init__.py`.

## Flags and Arguments

- `file ...` (Positional): Target paths or URLs.
  - With `--read`: one or more files to read sequentially. Passing zero files exits non-zero via `parser.error`.
  - With `--write` (without `--input-file`): `file[0]` is the source and `file[1:]` are destinations (at least two paths required).
  - With `--write --input-file SRC`: all positional arguments are destinations (at least one destination required).
- `--read`, `-r` (Mutually Exclusive): Reads every positional file and reports line count, UTF-8 byte size (`B` through `YB`), line rate, byte rate, and elapsed seconds.
- `--write`, `-w` (Mutually Exclusive): Pre-validates destination protocol and archive guards, opens the source and all destinations in UTF-8 text mode inside a single `contextlib.ExitStack`, reads the source once, writes every line synchronously to each destination (moving existing destinations to sequential `.bu` backups), and closes all handles on error.
- `--input-file INPUT_FILE`, `-i INPUT_FILE`: Explicit source path or URL for `--write` mode. Rejected via `parser.error` if combined with `--read` or if no destination is given.
- `--version`: Prints `polyopen <version>` and exits 0.

## Credential Redaction

URL passwords and query strings are automatically masked (`user:***@host/path?<redacted>`) in CLI stdout messages. Even so, avoid passing inline passwords on shell command lines because they can remain visible in shell history and process listings; prefer SSH keys (`sftp://user@host/path`).

## Examples

### Reading Streams (`--read`)

```bash
python -m polyopen --read syslog.01.zst syslog.02.bz2 https://example.com/archive.gz
```

Example output:

```text
TESTING reading syslog.01.zst
FINISHED Reading. syslog.01.zst. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
```

### Single-Pass Multi-Target Broadcast (`--write`)

```bash
# Positional form: first path is source, remaining paths are destinations
python -m polyopen --write dump.sql local_archive.gz local_archive.bz2 sftp://user@node/dump.zst

# Explicit --input-file (-i) form: all positional paths are destinations
python -m polyopen --write -i dump.sql local_archive.gz local_archive.bz2 sftp://user@node/dump.zst
```

Example output:

```text
Will Read from dump.sql
Writing local_archive.gz
Writing local_archive.bz2
Writing sftp://user@node/dump.zst
FINISHED Writing. local_archive.gz. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
FINISHED Writing. local_archive.bz2. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
FINISHED Writing. sftp://user@node/dump.zst. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
```

## Known Limitations

- **Synchronous fan-out:** `--write` reads the source once and writes each line synchronously to all destinations in sequence. Use the Python `polyopen()` API with threads or processes if you need concurrent remote uploads.
