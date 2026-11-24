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
* `--write`, `-w` (Mutually Exclusive Boolean): Copies lines from the source file to each destination file and reports UTF-8 byte statistics for each output.
* `--input-file INPUT_FILE`, `-i INPUT_FILE` (Optional String): Specifies the source file for `--write` mode so all positional `file` arguments act as destinations. Valid only with `--write` (rejected via `parser.error` if combined with `--read` or if no destination file is supplied).

## Practical Examples

### Benchmarking Streams Natively (`--read`)
Passing isolated compression bundles iteratively triggers an assessment routine extracting explicit ingestion rates uniformly across target payload sequences.

```bash
python -m polyopen.polyopen --read syslog.01.zst syslog.02.bz2 https://server.com/archive.gz
```
**Output Profile:**
```
TESTING reading syslog.01.zst
FINISHED Reading. syslog.01.zst. 238,901 lines / 48.2MB at 41,200.5/s (8.1MB/s). Total time: 5.79 Secs.
...
```

### Broadcasting Datasets Structurally (`--write`)
By combining `--write` flags natively against an `--input-file` declaration loop, `polyopen` sequentially parses identical string arrays natively injecting datasets consecutively across an endless matrix array dynamically broadcasting payloads natively!

```bash
# Iteratively parse the central SQL-dump map, converting it continuously across
# native GZIP, BZ2, and remote SFTP topological endpoints sequentially perfectly formatted!
python -m polyopen.polyopen --write -i postgres_dump.sql local_archive.gz local_archive.bz2 sftp://user@node/dump.zst
```

## Known Limitations
* **Broadcast Execution Modality:** Because `polyopen` is an inherently linear streaming evaluation block tracking `io.TextIOWrapper` endpoints iteratively, providing dozens of positional broadcast parameters utilizing `--write` behaves strictly synchronously (O(n) latency map). High concurrency multithreading execution boundaries natively dictating remote parallel IO bursts must be configured outside the CLI by leveraging the programmatic Python `polyopen(...)` library directly cleanly.
