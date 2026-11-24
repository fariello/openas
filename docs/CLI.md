# Command Line Interface (CLI) Guide

*[Return to Main README](../README.md)*

While `polyopen` is natively configured as a backend scripting dependency wrapper module utilized internally inside larger Python topologies, it also ships alongside an executable CLI environment structure.

The CLI serves principally dual-purposes natively:
1. Validating structural algorithmic IO streaming speeds.
2. Rapidly mapping file-broadcast architectures explicitly across remote nodes.

## Usage Blueprint

```bash
python -m polyopen.polyopen [-h] (--write | --read) [--input-file INPUT_FILE] [file ...]
```

## Flags & Arguments

* `file ...` (Positional): Target file paths or URLs. In `--read` mode, all positional files are read sequentially. In `--write` mode without `--input-file`, `file[0]` is the source and `file[1:]` are destination targets; with `--input-file`, all positional files are destination targets.
* `--read`, `-r` (Mutually Exclusive Boolean): Reads all positional `file` arguments and reports line count, UTF-8 byte count (formatted across `B`, `KB`, `MB`, `GB`, `TB`, `PB`, `EB`, `ZB`, and `YB`), line rate, byte rate, and elapsed seconds.
* `--write`, `-w` (Mutually Exclusive Boolean): Opens the source file and all destination files under a single `contextlib.ExitStack`, reads the source once, writes every line to each destination synchronously, prints `Writing <dest>` and `FINISHED Writing. <dest>.` statistics for each output, and closes all open handles cleanly if any writer fails.
* `--input-file INPUT_FILE`, `-i INPUT_FILE` (Optional String): Specifies the source file for `--write` mode so all positional `file` arguments act as destinations. Valid only with `--write` (rejected via `parser.error` if combined with `--read` or if no destination file is supplied).

## Practical Examples

### Benchmarking Streams (`--read`)
Passing one or more paths or URLs reads each input sequentially and reports line count, UTF-8 byte count, throughput, and elapsed time:

```bash
python -m polyopen.polyopen --read syslog.01.zst syslog.02.bz2 https://server.com/archive.gz
```
**Output Profile:**
```
TESTING reading syslog.01.zst
FINISHED Reading. syslog.01.zst. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
...
```

### Single-Pass Multi-Target Broadcast (`--write`)
In `--write` mode, `polyopen` reads the source file once (instead of K times for K destinations) and fans out each line synchronously to every destination writer managed by `contextlib.ExitStack`:

```bash
# Positional form: first positional file is the source, remaining files are destinations
python -m polyopen.polyopen --write postgres_dump.sql local_archive.gz local_archive.bz2 sftp://user@node/dump.zst

# Explicit --input-file (-i) form: all positional files are destinations
python -m polyopen.polyopen --write -i postgres_dump.sql local_archive.gz local_archive.bz2 sftp://user@node/dump.zst
```
**Output Profile:**
```
Will Read from postgres_dump.sql
Writing local_archive.gz
Writing local_archive.bz2
Writing sftp://user@node/dump.zst
FINISHED Writing. local_archive.gz. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
FINISHED Writing. local_archive.bz2. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
FINISHED Writing. sftp://user@node/dump.zst. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
```

## Known Limitations
* **Synchronous Fan-Out:** While `--write` reads the source file only once, it writes each line synchronously to all K destination writers in a single loop inside `contextlib.ExitStack`. If you need concurrent network uploads across many remote destinations, use the programmatic `polyopen(...)` API with threads or processes.
