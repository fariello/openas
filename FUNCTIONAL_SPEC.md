# Functional Specification: `openas`

*[Return to Main README](README.md)*

**Version:** 0.1.0
**Last Updated:** 2026-11-27

## 1. Purpose, Audience, and Scope

`openas` is a lightweight Python library and CLI for streaming plain and compressed files (`.gz`, `.bz2`, `.xz`, `.zst`) line-by-line or in chunks across local files and remote protocols (`http://`, `https://`, `ftp://`, `ssh://`, `sftp://`), in both UTF-8 text (`str`) and binary (`bytes`) modes, with optional sequential `.bu` overwrite backups.

- **Target audience:** Python developers, data engineers, and operators streaming local or remote compressed files (such as logs, CSV, JSONL, or binary records) without pulling in cloud object-store SDKs.
- **Explicit non-goals (v0.1.0):**
  - No cloud object-store support (`s3://`, `gs://`, `az://`, `hdfs://`).
  - No container archive manipulation (`.zip`, `.tar`, `.tar.*`, `.rar`, `.7z`).
  - No random-access `seek()`/`tell()` operations or `r+`/`w+`/`x` modes.
  - No asynchronous (`async`/`await`) I/O.
  - No HTTP/HTTPS write or append support.
  - No FTP append support (`ftputil` does not support FTP append).
  - No concurrent multi-target upload fan-out in the CLI.

## 2. Core Concepts

- **Unified Streaming Wrapper:** `openas()` routes text and binary read, write, and append modes to `OpenAsReader` or `OpenAsWriter`. Optional standard library and third-party modules are imported lazily on first use.
- **Line-by-Line Transparent Compression:** Uses Python's `gzip`, `bz2`, `lzma`, `zstandard`, `io.TextIOWrapper` (text mode), and `io.BufferedReader` (`64 KiB` buffers for binary `.gz`, `.bz2`, `.xz`, and plain binary network streams; `128 KiB` `read_size` and `io.BufferedReader` for `.zst` text and binary streams) to stream decompressed and compressed data across chunk boundaries in O(1) memory without buffering entire files.
- **Sequential `.bu` Overwrite Backups:** On overwrite (`backup=True`, default for write modes), `openas` validates `compresslevel` and imports required codecs first, then checks `<path>.001.bu` in O(1); if occupied, it lists the parent directory once (`os.scandir` locally, single `listdir` on SFTP/FTP) to select the lowest free 3-digit slot up to `.999.bu`, falling back to nanosecond timestamps (`time.time_ns()`) past 999. Existing `.bu` files are never deleted or pruned automatically.

## 3. Supported Protocols and Codecs

### 3.1 Transports

- **Local (`file://` or plain path):** Read, write, and append in text and binary modes. `file://` URIs are parsed with `urllib.parse.urlsplit()`, require an empty host or `localhost` without userinfo (remote or userinfo-bearing `file://` authorities raise `UnsupportedProtocolError` with redacted credentials), require a non-empty path (empty paths raise `ValueError`), and decode percent-encoded paths via `url2pathname`.
- **FTP (`ftp://`):** Read and write in text and binary modes via `ftputil` (`FTPSessionWrapper`, default port 21, custom ports validated in `1..65535` and optional `timeout` forwarded, credentials and paths percent-decoded). The percent-decoded URL path (`urlsplit(url).path`, preserving semicolons) must be non-empty (empty paths raise `ValueError`) and is passed directly to `ftputil.FTPHost.open()`. Omitted credentials fall back to anonymous FTP login. Append mode (`'a'`, `'at'`, `'ab'`) raises `UnsupportedProtocolError` before connecting.
- **SSH / SFTP (`ssh://`, `sftp://`):** Read, write, and append in text and binary modes via `paramiko` (`load_system_host_keys()`, default `RejectPolicy`, default port 22, custom ports validated in `1..65535`, optional `timeout` and optional `key_filename` forwarded, credentials and paths percent-decoded). The percent-decoded URL path (`urlsplit(url).path`, preserving semicolons) must be non-empty (empty paths raise `ValueError`) and is passed verbatim as an absolute remote path with no `~/` expansion; `~/.ssh/config` is not parsed; an omitted SSH username defaults to the local OS user authenticating via the SSH agent, default `~/.ssh/id_*` keys, or `key_filename`. Reads cache `stat().st_size` once and, when `file_size > 0` and `readv` is available, stream through `_BoundedSFTPReadStream` in bounded 4 MiB sliding `readv()` windows split into `MAX_REQUEST_SIZE` sub-chunks with `max_concurrent_prefetch_requests=64` (falling back to single-argument `readv(sub_chunks)` on Paramiko `<3.3`, and falling back to sequential `sftp_file.read()` when `readv` is absent or `file_size` is unknown or `<= 0`). Writes open binary SFTP handles with `set_pipelined(True)` and wrap uncompressed streams in `_BinaryStreamAdapter` (plus `io.TextIOWrapper` in text mode).
- **HTTP / HTTPS (`http://`, `https://`):** Read-only streaming in text and binary modes via lock-initialized, thread-local, cookie-free `requests.Session` reuse (`DefaultCookiePolicy(allowed_domains=[])`) with optional `timeout` and per-request `headers` forwarding (defensively copied and isolated from the thread-local session), port validation in `1..65535`, and empty paths allowed. URLs are split with `urlsplit()` so semicolons stay part of the path. Exited worker threads reclaim and close their `requests.Session` via `weakref.finalize` alongside interpreter-exit `atexit` cleanup. Compressed extensions send `Accept-Encoding: identity` unless the caller provides an `Accept-Encoding` header (checked case-insensitively); uncompressed responses set `response.raw.decode_content = True`. Write/append raises `ReadOnlyProtocolError`.
- **Rejected schemes:** Cloud schemes (`s3://`, `gs://`, `az://`, `hdfs://`) and unrecognized multi-letter `scheme://` URLs raise `UnsupportedProtocolError`.

### 3.2 Compression Formats

All supported write/append codecs accept optional `compresslevel: Optional[int] = None` (boolean or non-integer values raise `TypeError`, including on plain files; out-of-range integers raise `ValueError` before any backup is created):

1. `.bz2` (`bz2`; range 1..9; default 9)
2. `.gz` (`gzip`; range 0..9; default 9)
3. `.xz` (`lzma`; preset 0..9, optionally OR'd with `lzma.PRESET_EXTREME`; default 6)
4. `.zst` (`zstandard`; integers up to `zstandard.MAX_COMPRESSION_LEVEL` (`22`), including negative fast levels; default level 3; multi-frame reads via `read_across_frames=True`; truncated `.zst` streams raise `EOFError` across local, HTTP, FTP, and SFTP reads via zero-copy RFC 8878 frame validation in `_ZstdFrameValidatingStream`)

**Container Archives Rejected:** `.zip`, `.tar`, `.tgz`, `.rar`, `.7z`, `.tar.gz`, `.tar.bz2`, `.tar.xz`, `.tar.zst`, `.tar.lzma`, `.tbz`, `.tbz2`, `.txz`, `.tzst`, `.tlz`, `.gz.tar`, `.bz2.tar`, `.xz.tar`, and `.zst.tar` raise `UnsupportedArchiveError`.

## 4. Public API

### `openas(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None, encoding=None, errors=None, newline=None, headers=None, key_filename=None) -> Union[OpenAsReader, OpenAsWriter]`

- **`filename`** (`Union[str, os.PathLike]`): Normalized via `os.fsdecode()`. Extension matching is case-insensitive and ignores URL query strings.
- **`mode`** (`str`):
  - `'r'`, `'rt'`: `OpenAsReader(..., binary=False)` returning decoded `str` (UTF-8 by default).
  - `'rb'`: `OpenAsReader(..., binary=True)` returning raw `bytes`.
  - `'w'`, `'wt'`: `OpenAsWriter(..., binary=False).open(append=False, ...)` accepting `str` (rejecting `bytes` with `TypeError`).
  - `'wb'`: `OpenAsWriter(..., binary=True).open(append=False, ...)` accepting `bytes`-like objects (rejecting `str` with `TypeError`).
  - `'a'`, `'at'`: `OpenAsWriter(..., binary=False).open(append=True, ...)` accepting `str`.
  - `'ab'`: `OpenAsWriter(..., binary=True).open(append=True, ...)` accepting `bytes`-like objects.
  - Any other mode raises `ValueError`.
- **`backup`** (`Optional[bool]`): Defaults to `None` (`True` on write, `False` on append). Passing `backup=True` with append raises `ValueError`.
- **`show_progress`** (`bool`): Enables a `tqdm` progress bar tracking exact raw bytes read (`OpenAsReader`) or written (`OpenAsWriter`). On `OpenAsReader`, updates are batched at 64 KiB with exact flush on EOF/exit/`close()` and `total=None` when stream size is unknown or `Content-Encoding` transforms plain HTTP. On `OpenAsWriter` (write and append modes), `_CountingWriteStream` wraps the underlying binary stream across local, FTP, and SFTP transports with `total=None`, capturing header bytes written during `open()` in `initial` and flushing buffered payload and compressed trailer bytes on `flush()` and `close()`.
- **`compresslevel`** (`Optional[int]`): Forwarded to write/append codecs; ignored on read.
- **`timeout`** (`Optional[float]`): Optional finite positive timeout in seconds forwarded to HTTP (`requests.Session.get`), SSH/SFTP (`timeout`, `banner_timeout`, `auth_timeout`), and FTP (`connect`). Non-positive, non-finite (`nan`, `inf`, `-inf`), or boolean values raise `ValueError`.
- **`encoding`** (`Optional[str]`): Text encoding matching `builtins.open()` semantics (defaults to `'utf-8'` in text modes; validated via `codecs.lookup()` before any `.bu` backup or connection, raising `TypeError` or `LookupError`). Non-`None` values in binary mode raise `ValueError`.
- **`errors`** (`Optional[str]`): Codec error handler in text modes (defaults to Python's `'strict'`; non-`str` raises `TypeError` before any backup or connection). Non-`None` values in binary mode raise `ValueError`.
- **`newline`** (`Optional[str]`): Newline translation mode (`None`, `""`, `"\n"`, `"\r"`, or `"\r\n"`) matching `builtins.open()` across local and remote text streams. Default `None` applies universal newline translation on read and translates `\n` to `os.linesep` on write; pass `newline=""` for `csv.writer` or `newline="\n"` for exact LF wire output. Invalid types raise `TypeError`; invalid strings or binary-mode usage raise `ValueError`.
- **`headers`** (`Optional[Mapping[str, str]]`): Optional per-request HTTP/HTTPS headers (accepted by `openas()` and `OpenAsReader`, not `OpenAsWriter`). Validated unconditionally in `openas()` before any backup or connection: non-`Mapping` values and non-`str` keys/values raise `TypeError`; empty/whitespace names or CR/LF/NUL characters raise `ValueError` without echoing header values. Defensively copied and never stored on `requests.Session.headers` or exposed in `__repr__()`.
- **`key_filename`** (`Optional[Union[str, os.PathLike, Sequence[Union[str, os.PathLike]]]]`): Optional SSH private key path or non-empty sequence of paths forwarded to `paramiko.SSHClient.connect(key_filename=...)` (omitted when `None`). Validated and normalized via `os.fsdecode()` before any `.bu` backup or connection (`bytes`/`bytearray`/invalid types raise `TypeError`; empty strings or sequences raise `ValueError`) and omitted from `__repr__()`.

Both `OpenAsReader` and `OpenAsWriter` expose `binary`, `closed`, `readable()`, `writable()`, `seekable()`, `encoding` (effective encoding string in text mode, `None` in binary mode), `flush()`, credential-redacted `__repr__()`, context management, and deterministic idempotent `close()` (catching `BaseException` during progress flush and per-handle close so interrupts still close every remaining handle). `OpenAsReader` additionally exposes binary-only `read1(size=-1)` (raising `io.UnsupportedOperation` in text mode) so binary readers compose directly with `io.TextIOWrapper(openas(path, "rb"))` with zero `ResourceWarning`. Operations on closed handles raise `ValueError`. Top-level `openas.__all__` excludes internal `FTPSessionWrapper`.

## 5. Command-Line Interface

Available via `openas`, `python -m openas`, and `python -m openas.openas` (streams 64 KiB binary chunks, preserving exact bytes including CRLF and non-UTF-8 payloads, and counts `b'\n'`-delimited lines, including a final non-empty line without a trailing newline):

- `--read` (`-r`): Requires at least one positional file; rejects `--input-file`, `--no-backup`, and `--compresslevel` via `parser.error` (exit code 2); pre-validates `--timeout` before opening any stream; reads sequentially in 64 KiB binary chunks (forwarding `--timeout` and `--progress` to `OpenAsReader`) and prints line count, 1-decimal base-1024 human-readable byte size (`B` through `YB`), throughput, and elapsed time.
- `--write` (`-w`): Pre-validates `--timeout` and each destination before opening or rotating files (resolving target URLs, rejecting container archives, rejecting read-only `http://`/`https://` targets, validating `--compresslevel` per destination, rejecting existing non-symlink local directories with `IsADirectoryError`, and rejecting duplicate resolved destinations via `parser.error` exit code 2 with redacted URLs), opens the source (`OpenAsReader(..., show_progress=args.progress, timeout=args.timeout, binary=True)`) and all destinations (`OpenAsWriter(..., timeout=args.timeout, binary=True).open(append=False, backup=not args.no_backup, compresslevel=args.compresslevel)`) inside `contextlib.ExitStack`, reads the source once in 64 KiB chunks, broadcasts exact bytes synchronously to all destinations (moving existing destinations to sequential `.bu` backups unless `--no-backup` is passed), prints redacted status/summary lines, and closes all handles on error.
- `--input-file` (`-i`): Source file for `--write` mode (all positional arguments become destinations). Rejected via `parser.error` if combined with `--read` or without at least one destination.
- `--no-backup`: Disables `.bu` backup creation on `--write` (`backup=False`). Rejected via `parser.error` (exit code 2) if combined with `--read`.
- `--compresslevel` (`-c`): Integer compression level or preset forwarded to `--write` destinations and pre-validated before opening or rotating files. Invalid levels exit 1 cleanly via `_cli_main()`; non-integer values or use with `--read` exit 2.
- `--timeout`: Finite positive network timeout in seconds forwarded to both `OpenAsReader` and `OpenAsWriter`. Pre-validated before opening any stream (non-positive or non-finite values exit 1 cleanly; non-numeric values exit 2).
- `--progress`: Forwards `show_progress=True` to `OpenAsReader` on `--read` and to the source `OpenAsReader` on `--write`, rendering a single raw-byte progress bar on `stderr` while keeping `stdout` summaries clean.
- `--version`: Prints `openas <version>` and exits 0.

## 6. Edge Cases and Boundary Conditions

1. **Append and Backup Conflict:** `openas(path, 'a', backup=True)`, `openas(path, 'ab', backup=True)`, and `OpenAsWriter(path).open(append=True, backup=True)` raise `ValueError`.
2. **Local Backup Safety:** Existing local directories raise `IsADirectoryError` rather than being renamed to `.001.bu`. Both broken and valid local symlinks are moved as symlinks to `.NNN.bu` (using `os.link(..., follow_symlinks=False)` when supported or collision-checked `os.rename()` otherwise) without following, creating, hard-linking, or modifying symlink targets. Existing broken symlinks at backup candidate paths are detected via `os.path.lexists` and skipped rather than overwritten. Local sequential backups are intended for non-concurrent writers; `os.link`/`os.unlink` prevents overwrite of existing `.bu` slots on POSIX filesystems supporting hard links.
3. **Remote Backup Semantics:** SFTP and FTP `.bu` backups also reject existing remote directories with `IsADirectoryError` without calling `rename()`, and close acquired transport handles on error. SFTP retries `rename()` collisions up to 5 times across free slots. FTP rename atomicity depends on the remote FTP server; additionally, `ftputil.file.FTPFile.close()` ignores FTP `426`/`450`/`451` transfer-abort responses.
4. **Pre-Validation Ordering:** `encoding`, `errors`, `newline`, `headers`, `key_filename`, and `compresslevel` validation and optional codec imports execute before any `.bu` backup move so invalid arguments do not displace existing files.
5. **Multi-Frame Zstandard, Truncation Detection, and Large Lines:** Multi-frame concatenated `.zst` streams (`read_across_frames=True`) and lines exceeding 128 KiB work consistently in both text and binary modes across local, FTP, SFTP, and HTTP transports. Truncated `.zst` streams raise `EOFError` across all transports via zero-copy RFC 8878 frame-boundary validation (`_ZstdFrameValidatingStream`) instead of silently returning early EOF.

## 7. Security and Resource Boundaries

1. **SSH Host-Key Verification:** `paramiko.SSHClient()` calls `load_system_host_keys()` and preserves Paramiko's default `RejectPolicy` (unknown host keys are rejected rather than auto-accepted to prevent Man-in-the-Middle attacks).
2. **Credential Redaction and HTTP Header Isolation:** URL passwords, non-numeric port suffixes on malformed no-`@` URLs (such as `sftp://user:secret:1234/file.txt`), and query parameters are masked by `_redact_url()` in CLI stdout/stderr, `__repr__()`, `file://` authority errors, invalid port/path `ValueError` messages, and `UnsupportedArchiveError` messages. Inline URL passwords in shell commands remain visible to OS process tables; prefer SSH keys (`sftp://user@host/path/to/file`, optionally with `key_filename`). HTTP `headers` reject CR, LF, and NUL injection without echoing header values in exceptions, are never placed on the shared thread-local `requests.Session.headers`, and are omitted from `__repr__()`. On cross-host HTTP redirects, `requests` strips `Authorization` via `Session.rebuild_auth`, while other custom headers (such as `X-API-Key`) are forwarded across redirects by `requests`.
3. **Archive Corruption Prevention:** Container archive extensions fail fast before opening streams for reading or writing.
4. **Bounded SFTP Prefetch and Zstandard Window Boundaries (v0.1.0):**
   - SFTP reads stream in bounded 4 MiB sliding `readv()` windows (`<= 4 MiB` active prefetch buffer per stream, O(1) memory on multi-gigabyte files, early exit stops requesting further windows).
   - `ZstdDecompressor` uses `max_window_size=2**31` (2 GiB) to support `--long=31` archives. Treat untrusted `.zst` inputs with caution because a crafted header can request a large decompression window.
5. **Cleartext FTP:** `ftp://` uses standard `ftplib.FTP` (no TLS); use `sftp://` or `https://` for encrypted transfers.
6. **Network Timeouts:** `timeout` defaults to `None` for compatibility with slow bulk transfers; pass an explicit `timeout` in automated services to bound stalled connections.
