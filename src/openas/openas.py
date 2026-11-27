"""
openas - Unified streaming I/O across local files, remote protocols, and compression formats.

Supports plain, `.gz`, `.bz2`, `.xz`, and `.zst` streams over local files (`file://` or plain
paths), `http://` and `https://` (read-only), `ftp://` (read and write), and `ssh://` / `sftp://`
(read, write, and append). Text modes (`'r'`/`'rt'`, `'w'`/`'wt'`, `'a'`/`'at'`) operate on encoded
`str` (UTF-8 by default); binary modes (`'rb'`, `'wb'`, `'ab'`) operate on raw `bytes`.

Public API
----------
- `openas(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None, encoding=None, errors=None, newline=None, headers=None, key_filename=None)`:
  Unified factory returning an opened `OpenAsReader` or `OpenAsWriter`.
- `OpenAsReader`: Stream reader supporting `read()`, `read1()`, `readline()`, `readlines()`,
  `flush()`, line-by-line iteration (`str` in text mode, `bytes` in binary mode), and optional
  raw-byte `tqdm` progress bars.
- `OpenAsWriter`: Stream writer supporting `write()`, `writelines()`, `flush()`, `compresslevel`,
  optional raw-byte `tqdm` progress bars (`show_progress=True`), and collision-safe sequential
  `.NNN.bu` backups on overwrite.
- `OpenAsError`, `UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`:
  Exception hierarchy for unsupported archive containers and protocols.

Usage
-----
Read a local or remote compressed stream:
    >>> from openas import openas
    >>> with openas("path/to/data.gz", "r") as reader:
    ...     for line in reader:
    ...         process(line)

Read over SSH/SFTP using SSH keys (preferred over inline passwords):
    >>> with openas("sftp://user@hostname/path/to/remote.zst", "r") as reader:
    ...     content = reader.read()

Write a compressed file with optional sequential .bu backup on overwrite:
    >>> with openas("path/to/output.bz2", "w", compresslevel=6) as writer:
    ...     writer.write("Hello, world!\\n")

Author
------
Gabriele Fariello

Version
-------
0.1.0
"""

from __future__ import annotations

__version__ = "0.1.0"
__author__ = "Gabriele Fariello"

class OpenAsError(Exception):
    """Base exception for all openas-specific errors."""

class UnsupportedArchiveError(OpenAsError):
    """Raised when a container archive format (such as .zip or .tar.gz) is passed to openas."""

class ReadOnlyProtocolError(OpenAsError):
    """Raised when attempting to write or append to a read-only protocol (http:// or https://)."""

class UnsupportedProtocolError(OpenAsError):
    """Raised when an unsupported protocol or operation (such as s3:// or FTP append) is requested."""

UNSUPPORTED_ARCHIVES = (
    '.zip', '.tar', '.tgz', '.rar', '.7z',
    '.gz.tar', '.bz2.tar', '.xz.tar', '.zst.tar',
    '.tar.gz', '.tar.bz2', '.tar.xz', '.tar.zst', '.tar.lzma',
    '.tbz', '.tbz2', '.txz', '.tzst', '.tlz',
)

from collections.abc import Mapping, Sequence
import errno
import io
import os
import threading
import time
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from urllib.parse import SplitResult

_NETWORK_SCHEMES = frozenset(('http', 'https', 'ftp', 'ssh', 'sftp', 's3', 'gs', 'az', 'hdfs'))
_SUPPORTED_SCHEMES = frozenset(('file', 'http', 'https', 'ftp', 'ssh', 'sftp'))
_CLOUD_SCHEMES = frozenset(('s3', 'gs', 'az', 'hdfs'))
_LINK_FALLBACK_ERRNOS = frozenset(
    code
    for name in ('EOPNOTSUPP', 'ENOTSUP', 'ENOSYS', 'EPERM', 'EXDEV', 'EMLINK')
    for code in (getattr(errno, name, None),)
    if code is not None
)
_SFTP_PREFETCH_WINDOW_BYTES = 4 * 1024 * 1024
_BINARY_BUFFER_SIZE = 65536
_ZST_BUFFER_SIZE = 131072
_PROGRESS_BATCH_BYTES = 65536
_CLI_CHUNK_SIZE = 65536


class _BinaryStreamAdapter(io.BufferedIOBase):
    """io.BufferedIOBase adapter for binary streams (e.g. ftputil FTPFile, Paramiko SFTPFile)."""

    def __init__(self, raw, *, readable: bool = False, writable: bool = False):
        self._raw = raw
        self._readable = readable
        self._writable = writable
        self._closed = False
        self._raw_close_attempted = False

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return self._readable

    def writable(self) -> bool:
        return self._writable

    def seekable(self) -> bool:
        return False

    def read(self, size: int = -1):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if size is None or size < 0:
            return self._raw.read()
        return self._raw.read(size)

    def read1(self, size: int = -1):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        read1_fn = getattr(self._raw, 'read1', None)
        if read1_fn is not None:
            return read1_fn(size)
        return self.read(size)

    def readinto(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        with memoryview(b) as raw_mv, raw_mv.cast('B') as mv:
            if len(mv) == 0:
                return 0
            readinto_fn = getattr(self._raw, 'readinto', None)
            if readinto_fn is not None:
                return readinto_fn(mv)
            data = self.read(len(mv))
            if data is None:
                return None
            n = len(data)
            mv[:n] = data
            return n

    def write(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        data = b if isinstance(b, bytes) else memoryview(b).tobytes()
        res = self._raw.write(data)
        return res if isinstance(res, int) else len(data)

    def flush(self):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        flush_fn = getattr(self._raw, 'flush', None)
        if flush_fn is not None:
            flush_fn()

    def close(self):
        if not self._closed:
            self._closed = True
            self._raw_close_attempted = True
            close_fn = getattr(self._raw, 'close', None)
            if close_fn is not None:
                close_fn()


class _HttpRawStream(io.BufferedIOBase):
    """Binary stream adapter over urllib3 HTTPResponse that stays open until close() is called."""

    def __init__(self, raw):
        self._raw = raw
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._raw.read(size)

    def read1(self, size: int = -1):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        read1_fn = getattr(self._raw, 'read1', None)
        if read1_fn is not None:
            return read1_fn(size)
        return self._raw.read(size)

    def readinto(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        with memoryview(b) as raw_mv, raw_mv.cast('B') as mv:
            if len(mv) == 0:
                return 0
            readinto_fn = getattr(self._raw, 'readinto', None)
            if readinto_fn is not None:
                return readinto_fn(mv)
            data = self._raw.read(len(mv))
            if data is None:
                return None
            n = len(data)
            mv[:n] = data
            return n

    def close(self):
        if not self._closed:
            self._closed = True
            close_fn = getattr(self._raw, 'close', None)
            if close_fn is not None:
                close_fn()


class _BoundedSFTPReadStream(io.BufferedIOBase):
    """Bounded-memory sliding-window SFTP read stream backed by 4 MiB readv() windows."""

    def __init__(
        self,
        sftp_file,
        file_size: int,
        window_size: int = _SFTP_PREFETCH_WINDOW_BYTES,
    ):
        self._sftp_file = sftp_file
        self._file_size = file_size
        self._window_size = window_size
        self._offset = 0
        self._eof = False
        self._closed = False
        self._raw_close_attempted = False
        self._gen = None
        self._buf = b""
        self._buf_pos = 0

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def _close_gen(self) -> None:
        gen = self._gen
        self._gen = None
        if gen is not None:
            close_fn = getattr(gen, 'close', None)
            if close_fn is not None:
                try:
                    close_fn()
                except Exception:
                    pass

    def _next_chunk(self) -> bool:
        while self._buf_pos >= len(self._buf):
            if self._gen is not None:
                try:
                    chunk = next(self._gen)
                except StopIteration:
                    self._gen = None
                    continue
                if not chunk:
                    self._eof = True
                    self._close_gen()
                    self._buf = b""
                    self._buf_pos = 0
                    return False
                self._buf = chunk if isinstance(chunk, bytes) else bytes(chunk)
                self._buf_pos = 0
                return True

            if self._eof or self._offset >= self._file_size:
                self._eof = True
                return False

            window = min(self._window_size, self._file_size - self._offset)
            chunk_size = getattr(self._sftp_file, 'MAX_REQUEST_SIZE', 32768)
            if not isinstance(chunk_size, int) or isinstance(chunk_size, bool) or chunk_size <= 0:
                chunk_size = 32768

            sub_chunks = []
            pos = self._offset
            end = pos + window
            while pos < end:
                step = min(chunk_size, end - pos)
                sub_chunks.append((pos, step))
                pos += step

            try:
                it = self._sftp_file.readv(sub_chunks, max_concurrent_prefetch_requests=64)
            except TypeError:
                it = self._sftp_file.readv(sub_chunks)
            self._gen = iter(it)
            self._offset = end
        return True

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if size == 0:
            return b""
        if size is None or size < 0:
            chunks = []
            while self._next_chunk():
                chunks.append(self._buf[self._buf_pos:] if self._buf_pos else self._buf)
                self._buf = b""
                self._buf_pos = 0
            return b"".join(chunks)

        if not self._next_chunk():
            return b""
        avail = len(self._buf) - self._buf_pos
        if avail >= size:
            end = self._buf_pos + size
            out = self._buf[self._buf_pos:end]
            if end >= len(self._buf):
                self._buf = b""
                self._buf_pos = 0
            else:
                self._buf_pos = end
            return out

        parts = [self._buf[self._buf_pos:] if self._buf_pos else self._buf]
        remaining = size - avail
        self._buf = b""
        self._buf_pos = 0
        while remaining > 0 and self._next_chunk():
            avail = len(self._buf) - self._buf_pos
            if avail <= remaining:
                parts.append(self._buf[self._buf_pos:] if self._buf_pos else self._buf)
                remaining -= avail
                self._buf = b""
                self._buf_pos = 0
            else:
                end = self._buf_pos + remaining
                parts.append(self._buf[self._buf_pos:end])
                self._buf_pos = end
                remaining = 0
        return b"".join(parts)

    def read1(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if size == 0:
            return b""
        if not self._next_chunk():
            return b""
        avail = len(self._buf) - self._buf_pos
        if size is None or size < 0 or size >= avail:
            out = self._buf[self._buf_pos:] if self._buf_pos else self._buf
            self._buf = b""
            self._buf_pos = 0
            return out
        end = self._buf_pos + size
        out = self._buf[self._buf_pos:end]
        self._buf_pos = end
        return out

    def readinto(self, b) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        with memoryview(b) as raw_mv, raw_mv.cast('B') as mv:
            if len(mv) == 0:
                return 0
            data = self.read(len(mv))
            n = len(data)
            mv[:n] = data
            return n

    def close(self):
        if not self._closed:
            self._closed = True
            self._close_gen()
            self._buf = b""
            self._buf_pos = 0
            self._raw_close_attempted = True
            close_fn = getattr(self._sftp_file, 'close', None)
            if close_fn is not None:
                close_fn()


class _CountingByteStream:
    """Lightweight binary stream proxy that counts exact raw bytes read and batches progress updates."""

    def __init__(self, stream, on_progress=None, batch_size: int = _PROGRESS_BATCH_BYTES):
        self._stream = stream
        self.bytes_read = 0
        self._on_progress = on_progress
        self._batch_size = batch_size
        self._pending = 0

    def set_progress_callback(self, callback) -> None:
        self._on_progress = callback
        self._pending = 0

    def flush_progress(self) -> None:
        pending = self._pending
        if pending > 0:
            self._pending = 0
            if self._on_progress is not None:
                self._on_progress(pending)

    def _account(self, n: int) -> None:
        self.bytes_read += n
        if self._on_progress is not None:
            self._pending += n
            if self._pending >= self._batch_size:
                pending = self._pending
                self._pending = 0
                self._on_progress(pending)

    def read(self, size: int = -1):
        data = self._stream.read(size)
        if data:
            self._account(len(data))
            if size is None or size < 0:
                self.flush_progress()
        elif size is None or size != 0:
            self.flush_progress()
        return data

    def read1(self, size: int = -1):
        read1_fn = getattr(self._stream, 'read1', None)
        if read1_fn is not None:
            data = read1_fn(size)
        else:
            data = self._stream.read(size)
        if data:
            self._account(len(data))
        elif size is None or size != 0:
            self.flush_progress()
        return data

    def readinto(self, b):
        with memoryview(b) as raw_mv, raw_mv.cast('B') as mv:
            if len(mv) == 0:
                return 0
            readinto_fn = getattr(self._stream, 'readinto', None)
            if readinto_fn is not None:
                n = readinto_fn(mv)
                if n:
                    self._account(n)
                elif n == 0:
                    self.flush_progress()
                return n
            data = self._stream.read(len(mv))
            if data is None:
                return None
            n = len(data)
            if n:
                mv[:n] = data
                self._account(n)
            else:
                self.flush_progress()
            return n

    def readable(self) -> bool:
        fn = getattr(self._stream, 'readable', None)
        return fn() if fn is not None else True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        fn = getattr(self._stream, 'seekable', None)
        return fn() if fn is not None else False

    def close(self):
        close_fn = getattr(self._stream, 'close', None)
        if close_fn is not None:
            close_fn()

    def __getattr__(self, name):
        return getattr(self._stream, name)


class _CountingWriteStream(io.BufferedIOBase):
    """io.BufferedIOBase proxy that counts exact raw bytes written and batches progress updates."""

    def __init__(self, stream, on_progress=None, batch_size: int = _PROGRESS_BATCH_BYTES):
        self._stream = stream
        self.bytes_written = 0
        self._on_progress = on_progress
        self._batch_size = batch_size
        self._pending = 0
        self._closed = False
        self._raw_close_attempted = False

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return False

    def writable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def set_progress_callback(self, callback) -> None:
        self._on_progress = callback
        self._pending = 0

    def flush_progress(self) -> None:
        pending = self._pending
        if pending > 0:
            self._pending = 0
            if self._on_progress is not None:
                self._on_progress(pending)

    def _account(self, n: int) -> None:
        self.bytes_written += n
        if self._on_progress is not None:
            self._pending += n
            if self._pending >= self._batch_size:
                pending = self._pending
                self._pending = 0
                self._on_progress(pending)

    def write(self, b) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        with memoryview(b) as raw_mv, raw_mv.cast('B') as mv:
            n_in = len(mv)
            data = b if isinstance(b, bytes) else mv.tobytes()
        res = self._stream.write(data)
        written = res if (isinstance(res, int) and not isinstance(res, bool)) else n_in
        if written > 0:
            self._account(written)
        return written

    def flush(self):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        flush_fn = getattr(self._stream, 'flush', None)
        if flush_fn is not None:
            flush_fn()
        self.flush_progress()

    def close(self):
        if not self._closed:
            self._closed = True
            self._raw_close_attempted = True
            close_fn = getattr(self._stream, 'close', None)
            if close_fn is not None:
                close_fn()


_ZSTD_MAGIC = 0xFD2FB528
_ZSTD_SKIPPABLE_MAGIC_MASK = 0xFFFFFFF0
_ZSTD_SKIPPABLE_MAGIC_BASE = 0x184D2A50
_ZSTD_DID_SIZES = (0, 1, 2, 4)


class _ZstdFrameValidatingStream:
    """Zero-copy RFC 8878 frame-boundary validator wrapping a binary stream for ZstdDecompressor.stream_reader().

    python-zstandard's stream_reader() silently returns EOF when the underlying compressed
    byte stream ends mid-frame. This lightweight state machine tracks Zstandard and skippable
    frame/block wire lengths without copying payload bytes and raises EOFError if the stream
    reaches EOF before a frame completes.
    """

    _S_MAGIC = 0
    _S_FHD = 1
    _S_SKIP_LEN = 2
    _S_BLK_HDR = 3
    _S_SKIP_BYTES = 4
    _S_PASSTHROUGH = 5

    def __init__(self, stream):
        self._stream = stream
        self._state = self._S_MAGIC
        self._buf = bytearray()
        self._need = 4
        self._skip = 0
        self._next_state = self._S_MAGIC
        self._has_checksum = False

    def _feed(self, data) -> None:
        with memoryview(data) as raw_mv, raw_mv.cast('B') as mv:
            pos = 0
            n = len(mv)
            while pos < n:
                st = self._state
                if st == self._S_PASSTHROUGH:
                    return
                if st == self._S_SKIP_BYTES:
                    take = min(self._skip, n - pos)
                    self._skip -= take
                    pos += take
                    if self._skip == 0:
                        self._state = self._next_state
                        if self._state == self._S_MAGIC:
                            self._need = 4
                        elif self._state == self._S_BLK_HDR:
                            self._need = 3
                    continue

                rem = self._need - len(self._buf)
                take = min(rem, n - pos)
                self._buf.extend(mv[pos:pos + take])
                pos += take
                if len(self._buf) < self._need:
                    return

                buf = self._buf
                if st == self._S_MAGIC:
                    magic = int.from_bytes(buf, 'little')
                    buf.clear()
                    if magic == _ZSTD_MAGIC:
                        self._state = self._S_FHD
                        self._need = 1
                    elif (magic & _ZSTD_SKIPPABLE_MAGIC_MASK) == _ZSTD_SKIPPABLE_MAGIC_BASE:
                        self._state = self._S_SKIP_LEN
                        self._need = 4
                    else:
                        # Unknown magic: defer to zstandard so it raises ZstdError.
                        self._state = self._S_PASSTHROUGH
                        return
                elif st == self._S_SKIP_LEN:
                    skip_len = int.from_bytes(buf, 'little')
                    buf.clear()
                    if skip_len == 0:
                        self._state = self._S_MAGIC
                        self._need = 4
                    else:
                        self._skip = skip_len
                        self._next_state = self._S_MAGIC
                        self._state = self._S_SKIP_BYTES
                elif st == self._S_FHD:
                    fhd = buf[0]
                    buf.clear()
                    fcs_flag = (fhd >> 6) & 3
                    single_seg = (fhd >> 5) & 1
                    self._has_checksum = bool((fhd >> 2) & 1)
                    did_flag = fhd & 3
                    win_len = 0 if single_seg else 1
                    did_len = _ZSTD_DID_SIZES[did_flag]
                    if fcs_flag == 0:
                        fcs_len = 1 if single_seg else 0
                    elif fcs_flag == 1:
                        fcs_len = 2
                    elif fcs_flag == 2:
                        fcs_len = 4
                    else:
                        fcs_len = 8
                    rest_len = win_len + did_len + fcs_len
                    if rest_len == 0:
                        self._state = self._S_BLK_HDR
                        self._need = 3
                    else:
                        self._skip = rest_len
                        self._next_state = self._S_BLK_HDR
                        self._state = self._S_SKIP_BYTES
                elif st == self._S_BLK_HDR:
                    hdr = buf[0] | (buf[1] << 8) | (buf[2] << 16)
                    buf.clear()
                    last_block = bool(hdr & 1)
                    btype = (hdr >> 1) & 3
                    bsize = hdr >> 3
                    if btype == 3:
                        # Reserved block type: defer to zstandard so it raises ZstdError.
                        self._state = self._S_PASSTHROUGH
                        return
                    wire_len = 1 if btype == 1 else bsize
                    if last_block:
                        if self._has_checksum:
                            wire_len += 4
                        after = self._S_MAGIC
                    else:
                        after = self._S_BLK_HDR
                    if wire_len == 0:
                        self._state = after
                        self._need = 4 if after == self._S_MAGIC else 3
                    else:
                        self._skip = wire_len
                        self._next_state = after
                        self._state = self._S_SKIP_BYTES

    def _check_eof(self) -> None:
        if self._state == self._S_PASSTHROUGH:
            return
        if self._state != self._S_MAGIC or len(self._buf) > 0:
            raise EOFError("Truncated zstd stream: unexpected end of file before frame completion.")

    def read(self, size: int = -1):
        data = self._stream.read(size)
        if data:
            self._feed(data)
        elif size is None or size != 0:
            self._check_eof()
        return data

    def read1(self, size: int = -1):
        read1_fn = getattr(self._stream, 'read1', None)
        data = read1_fn(size) if read1_fn is not None else self._stream.read(size)
        if data:
            self._feed(data)
        elif size is None or size != 0:
            self._check_eof()
        return data

    def readinto(self, b):
        with memoryview(b) as raw_mv, raw_mv.cast('B') as mv:
            if len(mv) == 0:
                return 0
            readinto_fn = getattr(self._stream, 'readinto', None)
            if readinto_fn is not None:
                n = readinto_fn(mv)
                if n:
                    with mv[:n] as chunk:
                        self._feed(chunk)
                elif n == 0:
                    self._check_eof()
                return n
            data = self._stream.read(len(mv))
            if data is None:
                return None
            n = len(data)
            if n:
                mv[:n] = data
                self._feed(data)
            else:
                self._check_eof()
            return n

    def readable(self) -> bool:
        fn = getattr(self._stream, 'readable', None)
        return fn() if fn is not None else True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def close(self):
        close_fn = getattr(self._stream, 'close', None)
        if close_fn is not None:
            close_fn()

    def __getattr__(self, name):
        return getattr(self._stream, name)


def format_bytes(num: float) -> str:
    """Format bytes to human readable sizes."""
    import math
    if not math.isfinite(num):
        return f"{num}B"
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}"
        num /= 1024.0
    return f"{num:3.1f}YB"


def _redact_authority(authority: str) -> str:
    """Mask userinfo passwords and non-numeric hostport suffixes in a URL authority."""
    if '@' in authority:
        userinfo, hostport = authority.rsplit('@', 1)
        if ':' in userinfo:
            user, _ = userinfo.split(':', 1)
            userinfo = f"{user}:***"
    else:
        userinfo = None
        hostport = authority

    if hostport.startswith('['):
        if ']:' in hostport:
            host_part, suffix = hostport.split(']:', 1)
            if suffix and not (suffix.isascii() and suffix.isdigit()):
                hostport = f"{host_part}]:***"
    elif ':' in hostport:
        host_part, suffix = hostport.split(':', 1)
        if suffix and not (suffix.isascii() and suffix.isdigit()):
            hostport = f"{host_part}:***"

    if userinfo is not None:
        return f"{userinfo}@{hostport}"
    return hostport


def _redact_url(target: str) -> str:
    """Mask URL passwords, non-numeric port suffixes, and query strings so credentials never leak."""
    if '://' not in target:
        return target
    from urllib.parse import urlsplit, urlunsplit
    try:
        parts = urlsplit(target)
    except ValueError:
        scheme, rest = target.split('://', 1)
        scheme_prefix = f"{scheme}://"
        before_frag, has_frag, frag = rest.partition('#')
        before_query, _, query = before_frag.partition('?')
        authority, has_slash, path = before_query.partition('/')
        authority = _redact_authority(authority)
        return (
            f"{scheme_prefix}{authority}"
            + (f"/{path}" if has_slash else "")
            + ("?<redacted>" if query else "")
            + (f"#{frag}" if has_frag else "")
        )
    netloc = _redact_authority(parts.netloc)
    if netloc == parts.netloc and not parts.query:
        return target
    query = '<redacted>' if parts.query else ''
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


_VALID_NEWLINES = frozenset(('', '\n', '\r', '\r\n'))


def _validate_text_options(
    binary: bool,
    encoding: Optional[str],
    errors: Optional[str],
    newline: Optional[str],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Validate and normalize builtins.open()-compatible text options before I/O or backups."""
    if binary:
        if encoding is not None:
            raise ValueError("binary mode doesn't take an encoding argument")
        if errors is not None:
            raise ValueError("binary mode doesn't take an errors argument")
        if newline is not None:
            raise ValueError("binary mode doesn't take a newline argument")
        return None, None, None

    if encoding is None:
        effective_encoding = 'utf-8'
    else:
        if not isinstance(encoding, str):
            raise TypeError(f"encoding must be a str or None, got {type(encoding).__name__}.")
        import codecs
        codecs.lookup(encoding)
        effective_encoding = encoding

    if errors is not None and not isinstance(errors, str):
        raise TypeError(f"errors must be a str or None, got {type(errors).__name__}.")

    if newline is not None:
        if not isinstance(newline, str):
            raise TypeError(f"newline must be a str or None, got {type(newline).__name__}.")
        if newline not in _VALID_NEWLINES:
            raise ValueError(f"illegal newline value: {newline!r}")

    return effective_encoding, errors, newline


def _validate_headers(headers: Optional[Mapping[str, str]]) -> Optional[dict[str, str]]:
    """Validate optional HTTP headers and return a defensive dict copy (never echoing secret values)."""
    if headers is None:
        return None
    if not isinstance(headers, Mapping):
        raise TypeError(f"headers must be a mapping of str to str or None, got {type(headers).__name__}.")
    validated: dict[str, str] = {}
    for name, value in headers.items():
        if not isinstance(name, str):
            raise TypeError(f"HTTP header names must be str, got {type(name).__name__}.")
        if not isinstance(value, str):
            raise TypeError(f"HTTP header values must be str, got {type(value).__name__}.")
        if not name or not name.strip():
            raise ValueError("HTTP header name must be a non-empty string.")
        if '\r' in name or '\n' in name or '\x00' in name:
            raise ValueError("HTTP header name must not contain CR, LF, or NUL characters.")
        if '\r' in value or '\n' in value or '\x00' in value:
            raise ValueError("HTTP header value must not contain CR, LF, or NUL characters.")
        validated[name] = value
    return validated


def _validate_key_filename(
    key_filename: Optional[Union[str, os.PathLike, Sequence[Union[str, os.PathLike]]]],
) -> Optional[Union[str, list[str]]]:
    """Validate and normalize optional SSH key_filename (single path or non-empty sequence of paths)."""
    if key_filename is None:
        return None
    if isinstance(key_filename, (bytes, bytearray)):
        raise TypeError(
            f"key_filename must be a str, os.PathLike, sequence of paths, or None, "
            f"got {type(key_filename).__name__}."
        )
    if isinstance(key_filename, (str, os.PathLike)):
        path_str = os.fsdecode(key_filename)
        if not path_str:
            raise ValueError("key_filename must not be an empty path.")
        return path_str
    if isinstance(key_filename, Sequence):
        if len(key_filename) == 0:
            raise ValueError("key_filename sequence must not be empty.")
        normalized: list[str] = []
        for item in key_filename:
            if isinstance(item, (bytes, bytearray)) or not isinstance(item, (str, os.PathLike)):
                raise TypeError(
                    f"key_filename elements must be str or os.PathLike, got {type(item).__name__}."
                )
            item_str = os.fsdecode(item)
            if not item_str:
                raise ValueError("key_filename elements must not be empty paths.")
            normalized.append(item_str)
        return normalized
    raise TypeError(
        f"key_filename must be a str, os.PathLike, sequence of paths, or None, "
        f"got {type(key_filename).__name__}."
    )


def _validate_timeout(timeout: Optional[float]) -> Optional[float]:
    """Validate optional network timeout (finite positive float/int or None)."""
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError(f"timeout must be a finite positive number (seconds) or None, got {timeout!r}.")
    import math
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError(f"timeout must be a finite positive number (seconds) or None, got {timeout!r}.")
    return float(timeout)


def _validate_write_options(norm: str, compresslevel: Optional[int]) -> None:
    """Validate compresslevel and import optional codecs before creating .bu backups or opening files."""
    if compresslevel is not None:
        if isinstance(compresslevel, bool) or not isinstance(compresslevel, int):
            raise TypeError(f"compresslevel must be an int or None, got {type(compresslevel).__name__}.")
    if norm.endswith('.zst'):
        import zstandard as zstd
        if compresslevel is not None:
            zstd.ZstdCompressor(level=compresslevel)
    elif norm.endswith('.bz2'):
        if compresslevel is not None and not (1 <= compresslevel <= 9):
            raise ValueError(f"bz2 compresslevel must be between 1 and 9, got {compresslevel}.")
    elif norm.endswith('.gz'):
        if compresslevel is not None and not (0 <= compresslevel <= 9):
            raise ValueError(f"gzip compresslevel must be between 0 and 9, got {compresslevel}.")
    elif norm.endswith('.xz'):
        import lzma
        if compresslevel is not None:
            preset_level = compresslevel & ~lzma.PRESET_EXTREME
            if preset_level < 0 or preset_level > 9 or (compresslevel & ~(lzma.PRESET_EXTREME | 0xF)) != 0:
                raise ValueError(f"lzma preset must be between 0 and 9 (optionally OR'd with PRESET_EXTREME), got {compresslevel}.")


def _resolve_target(filename: str):
    """Parse and validate target URL or local path, returning (resolved_filename, parsed, scheme)."""
    if '://' not in filename:
        return filename, None, ''
    from urllib.parse import urlsplit
    parsed = urlsplit(filename)
    scheme = parsed.scheme.lower()
    if not scheme or not filename.lower().startswith(f"{scheme}://"):
        return filename, None, ''
    if scheme == 'file':
        if parsed.netloc and parsed.netloc.lower() != 'localhost':
            redacted = _redact_url(filename)
            redacted_netloc = urlsplit(redacted).netloc
            raise UnsupportedProtocolError(
                f"Unsupported remote host '{redacted_netloc}' in file:// URI: {redacted}. "
                f"Only local file:// URIs (empty host or localhost) are supported."
            )
        if not parsed.path:
            raise ValueError(f"Missing path in file:// URI: {_redact_url(filename)}")
        from urllib.request import url2pathname
        local_path = url2pathname(parsed.path)
        return local_path, None, ''
    if scheme in _CLOUD_SCHEMES:
        raise UnsupportedProtocolError(
            f"openas does not support cloud object stores ('{parsed.scheme}://'). "
            f"Use 'smart_open' or a dedicated cloud SDK (such as boto3) instead."
        )
    if scheme in _SUPPORTED_SCHEMES:
        if not parsed.hostname:
            raise ValueError(
                f"Missing hostname in {scheme}:// URL: {_redact_url(filename)} "
                f"(expected '{scheme}://host/path')"
            )
        try:
            port = parsed.port
        except ValueError:
            raise ValueError(f"Invalid port in {scheme}:// URL: {_redact_url(filename)}") from None
        if port is not None and not (1 <= port <= 65535):
            raise ValueError(
                f"Invalid port in {scheme}:// URL (must be between 1 and 65535): {_redact_url(filename)}"
            )
        if scheme in ('ftp', 'ssh', 'sftp') and not parsed.path:
            raise ValueError(f"Missing path in {scheme}:// URL: {_redact_url(filename)}")
        return filename, parsed, scheme
    if len(scheme) > 1 and f"{scheme}://" in filename.lower():
        raise UnsupportedProtocolError(
            f"Unsupported protocol '{parsed.scheme}://'. "
            f"Supported protocols: file, http, https, ftp, ssh, sftp."
        )
    return filename, parsed, ''


def _unquote_remote_parts(parsed: SplitResult):
    """Percent-decode username, password, and path components of FTP/SSH/SFTP URLs."""
    from urllib.parse import unquote
    username = unquote(parsed.username) if parsed.username is not None else None
    password = unquote(parsed.password) if parsed.password is not None else None
    remote_path = unquote(parsed.path)
    return username, password, remote_path


def _normalized_path(filename: str, parsed: Optional[SplitResult] = None) -> str:
    """Return percent-decoded lowercase path component for network URLs and lowercase filename for local paths."""
    if parsed is not None and parsed.scheme.lower() in _NETWORK_SCHEMES:
        from urllib.parse import unquote
        return unquote(parsed.path).lower()
    return filename.lower()


def _record_raw_close_attempted(obj, seen: set) -> None:
    """Mark underlying raw handles as already closed when an adapter closed them."""
    cur = obj
    visited = set()
    for _ in range(8):
        if cur is None or id(cur) in visited:
            break
        visited.add(id(cur))
        if isinstance(cur, _BinaryStreamAdapter):
            if cur._raw_close_attempted:
                seen.add(id(cur._raw))
            break
        if isinstance(cur, _BoundedSFTPReadStream):
            if cur._raw_close_attempted:
                seen.add(id(cur._sftp_file))
            break
        if isinstance(cur, _CountingWriteStream):
            if cur._raw_close_attempted:
                seen.add(id(cur._stream))
            cur = cur._stream
        elif isinstance(cur, (_CountingByteStream, _ZstdFrameValidatingStream)):
            cur = cur._stream
        elif isinstance(cur, io.IOBase):
            nxt = getattr(cur, 'buffer', None)
            if nxt is None:
                nxt = getattr(cur, 'raw', None)
            cur = nxt
        else:
            break


def _close_handles(*handles):
    """Close each distinct non-None handle in order, propagating the first error after closing all."""
    first_err = None
    seen = set()
    for handle in handles:
        if handle is None or id(handle) in seen:
            continue
        seen.add(id(handle))
        if getattr(handle, 'closed', False) is True:
            _record_raw_close_attempted(handle, seen)
            continue
        close_fn = getattr(handle, 'close', None)
        if close_fn is None:
            continue
        try:
            close_fn()
        except BaseException as err:
            if first_err is None:
                first_err = err
        _record_raw_close_attempted(handle, seen)
    if first_err is not None:
        raise first_err


def _parse_backup_entries(basename: str, entry_names) -> tuple:
    """Extract occupied 3-digit ASCII backup slots (1..999) and all matching .bu filenames."""
    prefix = f"{basename}."
    suffix = ".bu"
    prefix_len = len(prefix)
    suffix_len = len(suffix)
    used_slots = set()
    existing_backups = set()
    for name in entry_names:
        if name.startswith(prefix) and name.endswith(suffix):
            existing_backups.add(name)
            middle = name[prefix_len:-suffix_len]
            if len(middle) == 3 and middle.isascii() and middle.isdigit():
                slot = int(middle)
                if 1 <= slot <= 999:
                    used_slots.add(slot)
    return used_slots, existing_backups


def _select_backup_filename(path: str, basename: str, entry_names, reserved_slots=()) -> str:
    """Choose the lowest free 001..999 backup path, or a collision-safe timestamp fallback."""
    used_slots, existing_backups = _parse_backup_entries(basename, entry_names)
    if reserved_slots:
        used_slots.update(reserved_slots)
    for num in range(1, 1000):
        if num not in used_slots:
            return f"{path}.{num:03d}.bu"
    ts_ns = time.time_ns()
    while f"{basename}.{ts_ns}.bu" in existing_backups:
        ts_ns += 1
    return f"{path}.{ts_ns}.bu"


_http_session_lock = threading.Lock()
_http_session_local = None
_http_sessions_registry = []
_http_atexit_registered = False


class _HttpSessionHolder:
    """Weak-referenceable holder for a thread-local requests.Session."""

    __slots__ = ('session', '__weakref__')

    def __init__(self, session):
        self.session = session


def _reclaim_http_session(session):
    """Remove and close an exited thread's requests.Session if still registered."""
    found = False
    with _http_session_lock:
        for idx, reg_session in enumerate(_http_sessions_registry):
            if reg_session is session:
                del _http_sessions_registry[idx]
                found = True
                break
    if found:
        try:
            session.close()
        except Exception:
            pass


def _close_registered_http_sessions():
    """Close all thread-local requests.Session objects created by _get_http_session() at interpreter exit."""
    with _http_session_lock:
        sessions = list(_http_sessions_registry)
        _http_sessions_registry.clear()
    for session in sessions:
        try:
            session.close()
        except Exception:
            pass


def _get_http_session():
    """Return a thread-local requests.Session with cookie persistence disabled."""
    global _http_session_local, _http_atexit_registered
    local = _http_session_local
    if local is None:
        with _http_session_lock:
            local = _http_session_local
            if local is None:
                local = threading.local()
                _http_session_local = local
    holder = getattr(local, 'holder', None)
    if holder is None:
        import atexit
        import http.cookiejar
        import weakref
        import requests
        session = requests.Session()
        session.cookies.set_policy(http.cookiejar.DefaultCookiePolicy(allowed_domains=[]))
        holder = _HttpSessionHolder(session)
        with _http_session_lock:
            _http_sessions_registry.append(session)
            if not _http_atexit_registered:
                atexit.register(_close_registered_http_sessions)
                _http_atexit_registered = True
        finalizer = weakref.finalize(holder, _reclaim_http_session, session)
        finalizer.atexit = False
        local.holder = holder
    return holder.session


_FTP_SESSION_WRAPPER_CLASS = None


def _ftp_session_wrapper_class():
    """Lazily import ftplib and return the cached FTPSessionWrapper subclass."""
    global _FTP_SESSION_WRAPPER_CLASS
    if _FTP_SESSION_WRAPPER_CLASS is None:
        import ftplib

        class FTPSessionWrapper(ftplib.FTP):
            """
            A wrapper around ftplib.FTP for handling FTP connections with a specified port.

            This class extends ftplib.FTP to allow connections to FTP servers on non-standard ports.
            It is primarily intended for internal use within the OpenAsReader and OpenAsWriter classes
            and not for direct use by end users.

            Attributes
            ----------
            host : str
                The hostname of the FTP server.
            userid : str
                The username for FTP login.
            password : str
                The password for FTP login.
            port : int
                The port to connect to on the FTP server.

            Methods
            -------
            __init__(host, userid, password, port=21, timeout=None):
                Initializes the FTP connection (with optional socket timeout) and logs in with the provided credentials.
            """

            def __init__(
                self,
                host: str,
                userid: str,
                password: str,
                port: Optional[int] = 21,
                timeout: Optional[float] = None,
            ):
                """
                Initialize the FTPSessionWrapper with the given host, userid, password, and port.

                This method connects to the specified FTP server on the given port and logs in
                using the provided credentials.

                Parameters
                ----------
                host : str
                    The hostname of the FTP server.
                userid : str
                    The username for FTP login.
                password : str
                    The password for FTP login.
                port : int or None, optional
                    The port to connect to on the FTP server (defaults to 21 when None).
                timeout : float or None, optional
                    Optional socket timeout in seconds.
                """
                if port is None:
                    port = 21
                # Initialize the parent ftplib.FTP class
                ftplib.FTP.__init__(self)
                try:
                    # Connect to the specified FTP server on the given port
                    if timeout is not None:
                        self.connect(host, port, timeout=timeout)
                    else:
                        self.connect(host, port)
                    # Log in to the FTP server with the provided username and password
                    self.login(userid, password)
                except BaseException:
                    try:
                        self.close()
                    except Exception:
                        pass
                    raise

        _FTP_SESSION_WRAPPER_CLASS = FTPSessionWrapper
    return _FTP_SESSION_WRAPPER_CLASS


def __getattr__(name: str):
    """Lazily resolve optional attributes such as FTPSessionWrapper (PEP 562)."""
    if name == 'FTPSessionWrapper':
        return _ftp_session_wrapper_class()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class OpenAsReader:
    """
    Streaming reader supporting `.gz`, `.bz2`, `.xz`, and `.zst` decompression
    over local files (`file://` or plain paths), HTTP/HTTPS, FTP, and SSH/SFTP.

    Operates on decoded text (`str`, UTF-8 by default) or raw `bytes` when `binary=True`.

    Attributes
    ----------
    filename : str
        Normalized path or URL to read.
    show_progress : bool
        Whether to display a `tqdm` progress bar tracking raw bytes read.
    timeout : float or None
        Optional network timeout in seconds.
    binary : bool
        When True, stream operations return raw `bytes`; when False, decoded `str`.
    encoding : str or None
        Effective text encoding in text mode (`'utf-8'` by default), or `None` in binary mode.
    closed : bool
        True when no underlying stream handle is open.

    Methods
    -------
    open() -> OpenAsReader
        Open the stream for reading.
    read(size=-1) -> str or bytes, read1(size=-1) -> bytes, readline(size=-1) -> str or bytes, readlines(hint=-1) -> list
        Standard stream read operations (`str` in text mode, `bytes` in binary mode; `read1()` is binary-only).
    flush() -> None
        No-op on an open reader; raises `ValueError` when closed (enables `io.TextIOWrapper` composition).
    readable() -> bool, writable() -> bool, seekable() -> bool
        Standard file-like capability predicates.
    close() -> None
        Idempotently close all acquired stream and transport handles.

    Examples
    --------
    >>> with OpenAsReader("data.gz", show_progress=True) as r:
    ...     for line in r:
    ...         process(line)
    >>> with OpenAsReader("https://example.com/data.zst", binary=True, timeout=30) as r:
    ...     raw = r.read()
    """

    @property
    def encoding(self) -> Optional[str]:
        """Return the effective text encoding in text mode or `None` in binary mode."""
        return self._encoding

    @property
    def closed(self) -> bool:
        """Return True if the reader is closed."""
        return self._fh is None

    def readable(self) -> bool:
        """Return True (OpenAsReader is a read-only stream)."""
        return True

    def writable(self) -> bool:
        """Return False (OpenAsReader is a read-only stream)."""
        return False

    def seekable(self) -> bool:
        """Return False (OpenAsReader does not support random-access seeking)."""
        return False

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        mode = "rb" if self.binary else "r"
        return f"OpenAsReader({_redact_url(self.filename)!r}, mode={mode!r}, state={state!r})"

    def __init__(
        self,
        filename: Union[str, os.PathLike],
        show_progress: bool = False,
        timeout: Optional[float] = None,
        binary: bool = False,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        headers: Optional[Mapping[str, str]] = None,
        key_filename: Optional[Union[str, os.PathLike, Sequence[Union[str, os.PathLike]]]] = None,
    ):
        """
        Initialize OpenAsReader with a filename.

        Parameters
        ----------
        filename : str or os.PathLike
            The name of the file to read.
        show_progress : bool
            Whether to show a progress bar.
        timeout : float or None, optional
            Optional network timeout in seconds for HTTP, FTP, and SSH/SFTP connections.
        binary : bool, optional
            If True, read raw `bytes` instead of decoded `str` (default is False).
        encoding : str or None, optional
            Text encoding in text mode (defaults to `'utf-8'`). Must be None in binary mode.
        errors : str or None, optional
            Codec error handler in text mode (defaults to Python's `'strict'`). Must be None in binary mode.
        newline : str or None, optional
            Newline translation mode (`None`, `''`, `'\\n'`, `'\\r'`, or `'\\r\\n'`). Must be None in binary mode.
        headers : Mapping[str, str] or None, optional
            Optional per-request HTTP/HTTPS headers (defensively copied; ignored on non-HTTP targets).
        key_filename : str, os.PathLike, Sequence[str | os.PathLike], or None, optional
            Optional SSH private key path or sequence of paths forwarded to Paramiko `connect()`.
        """
        self.filename = os.fsdecode(filename)
        self.show_progress = show_progress
        self.timeout = _validate_timeout(timeout)
        self.binary = bool(binary)
        self._encoding, self._errors, self._newline = _validate_text_options(
            self.binary, encoding, errors, newline
        )
        self._headers = _validate_headers(headers)
        self._key_filename = _validate_key_filename(key_filename)
        self._fh = None
        self._raw_fh = None
        self._ssh_client = None
        self._sftp_client = None
        self._sftp_file = None
        self._sftp_file_size = None
        self._sftp_stat_checked = False
        self._ftp = None
        self._ftp_path = None
        self._response = None
        self._progress = None
        self._counting_stream = None
        self._progress_pending = 0
        self._progress_encoder = None
        self._progress_bom_encoding = False
        if not self.binary:
            import codecs
            self._progress_bom_encoding = (
                codecs.lookup(self._encoding or 'utf-8').name in ('utf-16', 'utf-32', 'utf-8-sig')
            )
        self._http_file_size = None

    def _make_counted_stream(self, stream):
        """Wrap binary stream in _CountingByteStream when show_progress is enabled."""
        if self.show_progress:
            self._counting_stream = _CountingByteStream(stream)
            return self._counting_stream
        return stream

    def _ensure_open(self):
        if self._fh is None:
            raise ValueError("I/O operation on closed file.")

    def _encoded_byte_len(self, text: str) -> int:
        """Compute an approximate byte length for plain-local text progress without raising on lossy text."""
        enc = self._encoding or 'utf-8'
        err = self._errors or 'strict'
        if self._progress_bom_encoding:
            encoder = self._progress_encoder
            if encoder is None:
                import codecs
                encoder = codecs.getincrementalencoder(enc)(err)
                self._progress_encoder = encoder
            try:
                return len(encoder.encode(text))
            except (UnicodeError, LookupError, TypeError):
                pass
        try:
            return len(text.encode(enc, err))
        except (UnicodeError, LookupError, TypeError):
            try:
                return len(text.encode(enc, 'replace'))
            except (UnicodeError, LookupError, TypeError):
                return len(text)

    def _apply_plain_progress(self, progress, delta: int) -> None:
        """Update plain-local tqdm progress, clamping BOM/lossy codec estimates to total."""
        if delta <= 0:
            return
        if (
            (self._progress_bom_encoding or self._errors not in (None, 'strict'))
            and progress.total is not None
        ):
            remaining = progress.total - progress.n
            if remaining <= 0:
                return
            if delta > remaining:
                delta = remaining
        progress.update(delta)

    def _flush_progress(self) -> None:
        """Apply any buffered progress delta to the progress bar once."""
        if self._counting_stream is not None:
            self._counting_stream.flush_progress()
            self._progress_pending = 0
            return
        progress = self._progress
        if progress is None:
            self._progress_pending = 0
            return
        if self._progress_pending > 0:
            pending = self._progress_pending
            self._progress_pending = 0
            self._apply_plain_progress(progress, pending)

    def _update_progress(self, data: Union[str, bytes] = ""):
        if not (self.show_progress and self._progress is not None):
            return
        if self._counting_stream is not None:
            self._counting_stream.flush_progress()
            return
        if self._progress_pending > 0:
            self._flush_progress()
        if data:
            delta = len(data) if isinstance(data, (bytes, bytearray)) else self._encoded_byte_len(data)
            self._apply_plain_progress(self._progress, delta)

    def _reconcile_crlf_eof_progress(self) -> None:
        """Advance plain local text progress to total at natural EOF when decoding/newlines shortened estimates."""
        if (
            self._counting_stream is not None
            or self.binary
            or self._progress is None
            or self._progress.total is None
        ):
            return
        should_reconcile = self._progress_bom_encoding or self._errors not in (None, 'strict')
        if not should_reconcile and self._newline is None:
            newlines = getattr(self._fh, 'newlines', None)
            if isinstance(newlines, str):
                should_reconcile = newlines in ('\r\n', '\r')
            elif isinstance(newlines, tuple):
                should_reconcile = '\r\n' in newlines or '\r' in newlines
        if not should_reconcile:
            return
        self._flush_progress()
        remaining = self._progress.total - self._progress.n
        if remaining > 0:
            self._progress.update(remaining)

    def __enter__(self) -> 'OpenAsReader':
        """
        Open the file when entering the context.

        This method is called when the context is entered using the `with` statement.

        Returns
        -------
        OpenAsReader
            The OpenAsReader instance with the file opened for reading.
        """
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the file when exiting the context.

        This method is called when the context is exited using the `with` statement.

        Parameters
        ----------
        exc_type : type
            The exception type.
        exc_value : Exception
            The exception instance.
        traceback : traceback
            The traceback object.

        Returns
        -------
        None
        """
        self.close()

    def __iter__(self):
        """
        Return an iterator over lines in the stream.

        Returns the underlying handle's C-level iterator directly whenever the handle is
        already in the target mode (`io.TextIOBase` for text or any binary stream when
        `binary=True`) and either progress tracking is disabled or `_CountingByteStream`
        owns batched raw-byte progress updates.
        """
        self._ensure_open()
        if self.show_progress:
            if self._counting_stream is not None and (self.binary or isinstance(self._fh, io.TextIOBase)):
                return iter(self._fh)
            return self._iter_progress()
        if self.binary or isinstance(self._fh, io.TextIOBase):
            return iter(self._fh)
        return self._iter_decoded()

    def _iter_decoded(self):
        """Yield decoded text lines from a non-text handle (binary stream)."""
        enc = self._encoding or 'utf-8'
        err = self._errors or 'strict'
        for line in self._fh:
            if isinstance(line, bytes):
                yield line.decode(enc, err)
            else:
                yield line

    def _iter_progress(self):
        """Yield lines (`str` or `bytes`) while batching tqdm progress updates to 64 KiB."""
        try:
            fh = self._fh
            progress = self._progress
            binary = self.binary
            is_text = isinstance(fh, io.TextIOBase)
            counting = self._counting_stream
            enc = self._encoding or 'utf-8'
            err = self._errors or 'strict'

            if progress is None:
                if binary or is_text:
                    yield from fh
                else:
                    for line in fh:
                        yield line.decode(enc, err) if isinstance(line, bytes) else line
                return

            if counting is not None:
                if binary or is_text:
                    yield from fh
                else:
                    for line in fh:
                        yield line.decode(enc, err) if isinstance(line, bytes) else line
                return

            update = progress.update
            if binary:
                for line in fh:
                    self._progress_pending += len(line)
                    if self._progress_pending >= _PROGRESS_BATCH_BYTES:
                        pending = self._progress_pending
                        self._progress_pending = 0
                        update(pending)
                    yield line
            elif is_text:
                byte_len = self._encoded_byte_len
                apply_delta = self._apply_plain_progress
                for line in fh:
                    self._progress_pending += byte_len(line)
                    if self._progress_pending >= _PROGRESS_BATCH_BYTES:
                        pending = self._progress_pending
                        self._progress_pending = 0
                        apply_delta(progress, pending)
                    yield line
                self._reconcile_crlf_eof_progress()
            else:
                byte_len = self._encoded_byte_len
                apply_delta = self._apply_plain_progress
                for line in fh:
                    if isinstance(line, bytes):
                        line_bytes = len(line)
                        line = line.decode(enc, err)
                    else:
                        line_bytes = byte_len(line)
                    self._progress_pending += line_bytes
                    if self._progress_pending >= _PROGRESS_BATCH_BYTES:
                        pending = self._progress_pending
                        self._progress_pending = 0
                        apply_delta(progress, pending)
                    yield line
                self._reconcile_crlf_eof_progress()
        finally:
            self._flush_progress()

    def flush(self) -> None:
        """No-op flush on an open reader; raises ValueError when closed (matches io.IOBase.flush)."""
        self._ensure_open()

    def readline(self, size: int = -1) -> Union[str, bytes]:
        """Read and return one line from the stream, or up to size characters/bytes."""
        self._ensure_open()
        line = self._fh.readline() if (size is None or size < 0) else self._fh.readline(size)
        if not self.binary and isinstance(line, bytes):
            line = line.decode(self._encoding or 'utf-8', self._errors or 'strict')
        self._update_progress(line)
        if line == "" and (size is None or size != 0):
            self._reconcile_crlf_eof_progress()
        return line

    def read(self, size: int = -1) -> Union[str, bytes]:
        """Read and return up to size characters/bytes, or the entire remaining stream if size < 0."""
        self._ensure_open()
        data = self._fh.read() if (size is None or size < 0) else self._fh.read(size)
        if not self.binary and isinstance(data, bytes):
            data = data.decode(self._encoding or 'utf-8', self._errors or 'strict')
        self._update_progress(data)
        if (size is None or size < 0) or (size > 0 and data == ""):
            self._reconcile_crlf_eof_progress()
        return data

    def read1(self, size: int = -1) -> bytes:
        """Read and return up to size bytes with at most one underlying read (binary mode only)."""
        self._ensure_open()
        if not self.binary:
            raise io.UnsupportedOperation("read1() is only supported in binary mode.")
        unbounded = size is None or size < 0
        read1_fn = getattr(self._fh, 'read1', None)
        if read1_fn is not None:
            data = read1_fn() if unbounded else read1_fn(size)
        else:
            data = self._fh.read() if unbounded else self._fh.read(size)
        self._update_progress(data)
        return data

    def readlines(self, hint: int = -1) -> list:
        """Read and return a list of lines from the stream."""
        self._ensure_open()
        unbounded = hint is None or hint <= 0
        if (self.binary or isinstance(self._fh, io.TextIOBase)) and (
            not self.show_progress or self._counting_stream is not None
        ):
            lines = self._fh.readlines() if unbounded else self._fh.readlines(hint)
            if self._counting_stream is not None:
                self._flush_progress()
            return lines
        if self.show_progress and self._counting_stream is None and unbounded:
            return list(self._iter_progress())
        lines = []
        total = 0
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            total += len(line)
            if not unbounded and total >= hint:
                break
        return lines

    def __next__(self) -> Union[str, bytes]:
        """
        Provide the next line in the file.

        This method is used to retrieve the next line when iterating over the file.

        Returns
        -------
        str or bytes
            The next line in the file (`bytes` in binary mode, `str` in text mode).

        Raises
        ------
        StopIteration
            When the end of the file is reached.
        """
        self._ensure_open()
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def _open_zst_reader(self, stream):
        """Wrap a binary stream with Zstandard multi-frame decompression and truncation detection."""
        import zstandard as zstd
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        validated = _ZstdFrameValidatingStream(stream)
        sr = dctx.stream_reader(validated, read_across_frames=True, read_size=_ZST_BUFFER_SIZE, closefd=False)
        buffered = io.BufferedReader(sr, buffer_size=_ZST_BUFFER_SIZE)
        return (
            buffered
            if self.binary
            else io.TextIOWrapper(buffered, encoding=self._encoding, errors=self._errors, newline=self._newline)
        )

    def _wrap_ftp(self, parsed: SplitResult):
        """
        Wrap self._fh to handle FTP connections and file operations.

        Parameters
        ----------
        parsed : SplitResult
            The parsed URL containing the FTP connection details and file path.

        Returns
        -------
        None
        """
        import ftputil
        port = 21 if parsed.port is None else parsed.port
        username, password, remote_path = _unquote_remote_parts(parsed)
        ftp_kwargs = {
            'port': port,
            'session_factory': _ftp_session_wrapper_class(),
        }
        if self.timeout is not None:
            ftp_kwargs['timeout'] = self.timeout
        self._ftp = ftputil.FTPHost(
            parsed.hostname,
            username,
            password,
            **ftp_kwargs,
        )
        self._ftp_path = remote_path
        raw_fh = self._ftp.open(remote_path, 'rb')
        self._raw_fh = raw_fh
        adapted = _BinaryStreamAdapter(raw_fh, readable=True, writable=False)
        self._raw_fh = adapted
        stream = self._make_counted_stream(adapted)

        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            self._fh = self._open_zst_reader(stream)
        elif norm.endswith('.bz2'):
            import bz2
            self._fh = (
                io.BufferedReader(bz2.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else bz2.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.xz'):
            import lzma
            self._fh = (
                io.BufferedReader(lzma.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else lzma.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.gz'):
            import gzip
            self._fh = (
                io.BufferedReader(gzip.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else gzip.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        else:
            self._fh = (
                io.BufferedReader(stream, buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else io.TextIOWrapper(stream, encoding=self._encoding, errors=self._errors, newline=self._newline)
            )

    def _wrap_ssh(self, parsed: SplitResult):
        """
        Wrap self._fh to handle SSH/SFTP connections and file operations.

        This method establishes an SSH connection using paramiko, opens the specified file via SFTP,
        and wraps it with bounded 4 MiB sliding-window readv() streaming (when supported and size
        is positive) and appropriate decompression handlers based on the file extension.

        Parameters
        ----------
        parsed : SplitResult
            The parsed URL containing the SSH/SFTP connection details and file path.

        Returns
        -------
        None
        """
        import paramiko
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        port = 22 if parsed.port is None else parsed.port
        username, password, remote_path = _unquote_remote_parts(parsed)
        connect_kwargs = {
            'port': port,
            'username': username,
            'password': password,
        }
        if self.timeout is not None:
            connect_kwargs['timeout'] = self.timeout
            connect_kwargs['banner_timeout'] = self.timeout
            connect_kwargs['auth_timeout'] = self.timeout
        if self._key_filename is not None:
            connect_kwargs['key_filename'] = self._key_filename
        try:
            self._ssh_client = client
            client.connect(parsed.hostname, **connect_kwargs)
            sftp = client.open_sftp()
            self._sftp_client = sftp
            if self.timeout is not None:
                get_ch = getattr(sftp, 'get_channel', None)
                channel = get_ch() if get_ch is not None else None
                settimeout_fn = getattr(channel, 'settimeout', None)
                if settimeout_fn is not None:
                    settimeout_fn(self.timeout)
            sftp_file = sftp.file(remote_path, 'rb')
            self._sftp_file = sftp_file

            file_size = None
            self._sftp_stat_checked = True
            stat_fn = getattr(sftp_file, 'stat', None)
            if stat_fn is not None:
                try:
                    st = stat_fn()
                    candidate_size = getattr(st, 'st_size', None)
                    if isinstance(candidate_size, int) and not isinstance(candidate_size, bool):
                        file_size = candidate_size
                except OSError:
                    file_size = None
            self._sftp_file_size = file_size

            if file_size is not None and file_size > 0 and getattr(sftp_file, 'readv', None) is not None:
                raw_stream = _BoundedSFTPReadStream(sftp_file, file_size)
                self._raw_fh = raw_stream
            else:
                raw_stream = sftp_file
        except BaseException:
            self.close()
            raise

        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            stream = self._make_counted_stream(raw_stream)
            self._fh = self._open_zst_reader(stream)
        elif norm.endswith('.bz2'):
            import bz2
            stream = self._make_counted_stream(raw_stream)
            self._fh = (
                io.BufferedReader(bz2.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else bz2.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.xz'):
            import lzma
            stream = self._make_counted_stream(raw_stream)
            self._fh = (
                io.BufferedReader(lzma.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else lzma.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.gz'):
            import gzip
            stream = self._make_counted_stream(raw_stream)
            self._fh = (
                io.BufferedReader(gzip.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else gzip.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        else:
            if self.binary:
                if isinstance(raw_stream, _BoundedSFTPReadStream):
                    stream = self._make_counted_stream(raw_stream)
                else:
                    adapted = _BinaryStreamAdapter(raw_stream, readable=True, writable=False)
                    stream = self._make_counted_stream(adapted)
                self._fh = io.BufferedReader(stream, buffer_size=_BINARY_BUFFER_SIZE)
            else:
                stream = self._make_counted_stream(raw_stream)
                self._fh = io.TextIOWrapper(stream, encoding=self._encoding, errors=self._errors, newline=self._newline)

    def _wrap_http(self, parsed: Optional[SplitResult] = None):
        """
        Wrap self._fh to handle HTTP/HTTPS connections and file operations.

        This method fetches the specified file via HTTP/HTTPS, wraps the response content
        with appropriate decompression handlers based on the file extension, and sets the file
        handle to the decompressed content. It also stores the file size in self._http_file_size.

        Parameters
        ----------
        parsed : SplitResult or None, optional
            The parsed URL (parsed from self.filename if omitted).

        Returns
        -------
        None
        """
        if parsed is None:
            from urllib.parse import urlsplit
            parsed = urlsplit(self.filename)
        norm = _normalized_path(self.filename, parsed)
        is_compressed = norm.endswith(('.zst', '.bz2', '.xz', '.gz'))
        get_kwargs = {'stream': True}
        req_headers = dict(self._headers) if self._headers else {}
        if is_compressed and not any(k.lower() == 'accept-encoding' for k in req_headers):
            req_headers['Accept-Encoding'] = 'identity'
        if req_headers:
            get_kwargs['headers'] = req_headers
        if self.timeout is not None:
            get_kwargs['timeout'] = self.timeout
        response = _get_http_session().get(self.filename, **get_kwargs)
        self._response = response
        try:
            response.raise_for_status()
        except BaseException:
            self.close()
            raise

        content_length = response.headers.get('Content-Length')
        try:
            self._http_file_size = int(content_length) if content_length is not None else None
        except (TypeError, ValueError):
            self._http_file_size = None

        if norm.endswith('.zst'):
            stream = self._make_counted_stream(response.raw)
            self._fh = self._open_zst_reader(stream)
        elif norm.endswith('.bz2'):
            import bz2
            stream = self._make_counted_stream(response.raw)
            self._fh = (
                io.BufferedReader(bz2.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else bz2.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.xz'):
            import lzma
            stream = self._make_counted_stream(response.raw)
            self._fh = (
                io.BufferedReader(lzma.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else lzma.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.gz'):
            import gzip
            stream = self._make_counted_stream(response.raw)
            self._fh = (
                io.BufferedReader(gzip.open(stream, 'rb'), buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else gzip.open(stream, 'rt', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        else:
            content_encoding = response.headers.get('Content-Encoding')
            if isinstance(content_encoding, str) and content_encoding.strip().lower() not in ('', 'identity'):
                self._http_file_size = None
            try:
                response.raw.decode_content = True
            except AttributeError:
                pass
            stream = self._make_counted_stream(_HttpRawStream(response.raw))
            self._fh = (
                io.BufferedReader(stream, buffer_size=_BINARY_BUFFER_SIZE)
                if self.binary
                else io.TextIOWrapper(stream, encoding=self._encoding, errors=self._errors, newline=self._newline)
            )

    def _wrap_local(self, parsed: Optional[SplitResult] = None):
        """
        Wrap self._fh to handle local file operations with optional decompression.

        This method opens the specified local file and wraps it with appropriate decompression
        handlers based on the file extension, setting the file handle to the decompressed content.

        Parameters
        ----------
        parsed : SplitResult or None, optional
            The parsed path/URL (optional; omitted for plain local paths).

        Returns
        -------
        None
        """
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            self._raw_fh = open(self.filename, 'rb')
            stream = self._make_counted_stream(self._raw_fh)
            self._fh = self._open_zst_reader(stream)
        elif norm.endswith('.bz2'):
            import bz2
            open_kwargs = (
                {'mode': 'rb'}
                if self.binary
                else {'mode': 'rt', 'encoding': self._encoding, 'errors': self._errors, 'newline': self._newline}
            )
            if self.show_progress:
                self._raw_fh = open(self.filename, 'rb')
                fh = bz2.open(self._make_counted_stream(self._raw_fh), **open_kwargs)
            else:
                fh = bz2.open(self.filename, **open_kwargs)
            self._fh = io.BufferedReader(fh, buffer_size=_BINARY_BUFFER_SIZE) if self.binary else fh
        elif norm.endswith('.xz'):
            import lzma
            open_kwargs = (
                {'mode': 'rb'}
                if self.binary
                else {'mode': 'rt', 'encoding': self._encoding, 'errors': self._errors, 'newline': self._newline}
            )
            if self.show_progress:
                self._raw_fh = open(self.filename, 'rb')
                fh = lzma.open(self._make_counted_stream(self._raw_fh), **open_kwargs)
            else:
                fh = lzma.open(self.filename, **open_kwargs)
            self._fh = io.BufferedReader(fh, buffer_size=_BINARY_BUFFER_SIZE) if self.binary else fh
        elif norm.endswith('.gz'):
            import gzip
            open_kwargs = (
                {'mode': 'rb'}
                if self.binary
                else {'mode': 'rt', 'encoding': self._encoding, 'errors': self._errors, 'newline': self._newline}
            )
            if self.show_progress:
                self._raw_fh = open(self.filename, 'rb')
                fh = gzip.open(self._make_counted_stream(self._raw_fh), **open_kwargs)
            else:
                fh = gzip.open(self.filename, **open_kwargs)
            self._fh = io.BufferedReader(fh, buffer_size=_BINARY_BUFFER_SIZE) if self.binary else fh
        else:
            self._fh = (
                open(self.filename, 'rb')
                if self.binary
                else open(self.filename, 'r', encoding=self._encoding, errors=self._errors, newline=self._newline)
            )

    def open(self) -> 'OpenAsReader':
        """
        Open the file for reading, decompressing it on the fly if necessary.

        This method determines the file type and source, and opens the file
        with the appropriate handler and decompression method.

        Returns
        -------
        OpenAsReader
            The OpenAsReader instance with the file opened for reading.
        """
        if self._fh is not None:
            return self

        self.filename, parsed, scheme = _resolve_target(self.filename)

        if _normalized_path(self.filename, parsed).endswith(UNSUPPORTED_ARCHIVES):
            raise UnsupportedArchiveError(
                f"openas does not support reading archive formats directly: {_redact_url(self.filename)}. "
                f"Use Python's zipfile or tarfile module instead."
            )

        try:
            if scheme == 'ftp':
                self._wrap_ftp(parsed)
            elif scheme in ('sftp', 'ssh'):
                self._wrap_ssh(parsed)
            elif scheme in ('http', 'https'):
                self._wrap_http(parsed)
            else:
                self._wrap_local(parsed)

            if self.show_progress:
                from tqdm import tqdm
                file_size = self._get_file_size()
                initial_bytes = self._counting_stream.bytes_read if self._counting_stream is not None else 0
                self._progress_pending = 0
                total = file_size if (file_size is not None and file_size > 0) else None
                self._progress = tqdm(total=total, initial=initial_bytes, unit='B', unit_scale=True)
                if self._counting_stream is not None:
                    self._counting_stream.set_progress_callback(self._progress.update)
        except BaseException:
            self.close()
            raise
        return self

    def _get_file_size(self) -> Optional[int]:
        """
        Get the size of the file in bytes.

        This method determines the size of the file, whether it is local, on an SFTP or FTP server,
        or accessible via HTTP/HTTPS.

        Returns
        -------
        int or None
            The size of the file in bytes, or None if unknown.
        """
        if self._sftp_file_size is not None:
            return self._sftp_file_size
        if self._sftp_file is not None:
            if self._sftp_stat_checked:
                return None
            try:
                size = self._sftp_file.stat().st_size
                return size if (isinstance(size, int) and not isinstance(size, bool)) else None
            except OSError:
                return None
        if self._ftp is not None:
            try:
                return self._ftp.path.getsize(self._ftp_path)
            except Exception:
                return None
        if self._response is not None:
            return self._http_file_size
        try:
            return os.path.getsize(self.filename)
        except OSError:
            return None

    def close(self):
        """
        Close the file.

        This method closes the file handle and any associated resources idempotently,
        resetting all handle and state attributes even if one handle fails to close.

        Returns
        -------
        None
        """
        flush_err = None
        try:
            self._flush_progress()
        except BaseException as err:
            flush_err = err
        finally:
            if self._counting_stream is not None:
                self._counting_stream.set_progress_callback(None)
            fh = self._fh
            raw_fh = self._raw_fh
            sftp_file = self._sftp_file
            sftp_client = self._sftp_client
            ssh_client = self._ssh_client
            ftp = self._ftp
            response = self._response
            progress = self._progress

            self._fh = None
            self._raw_fh = None
            self._sftp_file = None
            self._sftp_file_size = None
            self._sftp_stat_checked = False
            self._sftp_client = None
            self._ssh_client = None
            self._ftp = None
            self._ftp_path = None
            self._response = None
            self._http_file_size = None
            self._progress = None
            self._counting_stream = None
            self._progress_pending = 0
            self._progress_encoder = None

        try:
            _close_handles(fh, raw_fh, sftp_file, sftp_client, ssh_client, ftp, response, progress)
        except BaseException as close_err:
            if flush_err is not None:
                raise flush_err from close_err
            raise
        if flush_err is not None:
            raise flush_err


class OpenAsWriter:
    """
    Streaming writer supporting `.gz`, `.bz2`, `.xz`, and `.zst` compression
    over local files (`file://` or plain paths), FTP (write only), and SSH/SFTP (write/append),
    with collision-safe sequential `.NNN.bu` backups on overwrite.

    Operates on encoded text (`str`, UTF-8 by default) or raw `bytes`-like objects when `binary=True`.

    Attributes
    ----------
    filename : str
        Normalized path or URL to write.
    show_progress : bool
        Whether to display a `tqdm` progress bar tracking raw bytes written.
    timeout : float or None
        Optional network timeout in seconds.
    binary : bool
        When True, stream operations accept raw `bytes`-like data; when False, `str`.
    encoding : str or None
        Effective text encoding in text mode (`'utf-8'` by default), or `None` in binary mode.
    closed : bool
        True when no underlying stream handle is open.

    Methods
    -------
    open(append=False, backup=True, compresslevel=None) -> OpenAsWriter
        Open the stream for writing or appending.
    write(data) -> int, writelines(lines) -> None, flush() -> None
        Standard stream write operations (`str` in text mode, `bytes`-like in binary mode).
    readable() -> bool, writable() -> bool, seekable() -> bool
        Standard file-like capability predicates.
    close() -> None
        Idempotently close all acquired stream and transport handles.

    Examples
    --------
    >>> with OpenAsWriter("out.gz").open(append=False, backup=True, compresslevel=6) as w:
    ...     w.write("hello\\n")
    >>> with OpenAsWriter("out.gz").open(append=True, backup=False) as w:
    ...     w.write("more\\n")
    """

    @property
    def encoding(self) -> Optional[str]:
        """Return the effective text encoding in text mode or `None` in binary mode."""
        return self._encoding

    @property
    def closed(self) -> bool:
        """Return True if the writer is closed."""
        return self._fh is None

    def readable(self) -> bool:
        """Return False (OpenAsWriter is a write-only stream)."""
        return False

    def writable(self) -> bool:
        """Return True (OpenAsWriter is a write-only stream)."""
        return True

    def seekable(self) -> bool:
        """Return False (OpenAsWriter does not support random-access seeking)."""
        return False

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        if self._append:
            mode = "ab" if self.binary else "a"
        else:
            mode = "wb" if self.binary else "w"
        return f"OpenAsWriter({_redact_url(self.filename)!r}, mode={mode!r}, state={state!r})"

    def __init__(
        self,
        filename: Union[str, os.PathLike],
        show_progress: bool = False,
        timeout: Optional[float] = None,
        binary: bool = False,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        key_filename: Optional[Union[str, os.PathLike, Sequence[Union[str, os.PathLike]]]] = None,
    ):
        """
        Initialize OpenAsWriter with a filename.

        Parameters
        ----------
        filename : str or os.PathLike
            The name of the file to write to.
        show_progress : bool, optional
            Whether to display a `tqdm` progress bar tracking exact raw bytes written (default is False).
        timeout : float or None, optional
            Optional network timeout in seconds for FTP and SSH/SFTP connections.
        binary : bool, optional
            If True, write raw `bytes`-like objects instead of encoded `str` (default is False).
        encoding : str or None, optional
            Text encoding in text mode (defaults to `'utf-8'`). Must be None in binary mode.
        errors : str or None, optional
            Codec error handler in text mode (defaults to Python's `'strict'`). Must be None in binary mode.
        newline : str or None, optional
            Newline translation mode (`None`, `''`, `'\\n'`, `'\\r'`, or `'\\r\\n'`). Must be None in binary mode.
        key_filename : str, os.PathLike, Sequence[str | os.PathLike], or None, optional
            Optional SSH private key path or sequence of paths forwarded to Paramiko `connect()`.
        """
        self.filename = os.fsdecode(filename)
        self.show_progress = bool(show_progress)
        self.timeout = _validate_timeout(timeout)
        self.binary = bool(binary)
        self._encoding, self._errors, self._newline = _validate_text_options(
            self.binary, encoding, errors, newline
        )
        self._key_filename = _validate_key_filename(key_filename)
        self._append = False
        self._fh = None
        self._raw_fh = None
        self._ftp = None
        self._ssh_client = None
        self._sftp_client = None
        self._sftp_file = None
        self._progress = None
        self._counting_stream = None

    def _make_counted_stream(self, stream):
        """Wrap binary write stream in _CountingWriteStream when show_progress is enabled."""
        if self.show_progress:
            self._counting_stream = _CountingWriteStream(stream)
            return self._counting_stream
        return stream

    def __enter__(self) -> 'OpenAsWriter':
        """
        Open the file for writing when entering the context.

        This method is called when the context is entered using the `with` statement.

        Returns
        -------
        OpenAsWriter
            The OpenAsWriter instance with the file opened for writing.
        """
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the file when exiting the context.

        This method is called when the context is exited using the `with` statement.

        Parameters
        ----------
        exc_type : type
            The exception type.
        exc_value : Exception
            The exception instance.
        traceback : traceback
            The traceback object.

        Returns
        -------
        None
        """
        self.close()

    def _wrap_ftp(self, parsed: SplitResult, append: bool, backup: bool, compresslevel: Optional[int] = None):
        """
        Wrap self._fh to handle FTP connections and file operations.

        This method establishes an FTP connection using ftputil, opens the specified file for writing,
        and wraps it with appropriate compression handlers based on the file extension.

        Parameters
        ----------
        parsed : SplitResult
            The parsed URL containing the FTP connection details and file path.
        append : bool
            Whether to open the file in append mode.
        backup : bool
            Whether to create a backup of the file if it exists.
        compresslevel : int or None, optional
            Optional compression level/preset forwarded to the underlying compressor.

        Returns
        -------
        None
        """
        if append:
            raise UnsupportedProtocolError(
                "Protocol 'ftp' does not support append mode in openas (ftputil does not support FTP append). "
                "Use write mode ('w'/'wb') on ftp:// or append over sftp:// instead."
            )
        import ftputil
        port = 21 if parsed.port is None else parsed.port
        username, password, remote_path = _unquote_remote_parts(parsed)
        ftp_kwargs = {
            'port': port,
            'session_factory': _ftp_session_wrapper_class(),
        }
        if self.timeout is not None:
            ftp_kwargs['timeout'] = self.timeout
        self._ftp = ftputil.FTPHost(
            parsed.hostname,
            username,
            password,
            **ftp_kwargs,
        )

        if backup and self._ftp.path.exists(remote_path):
            isdir_fn = getattr(self._ftp.path, 'isdir', None)
            if isdir_fn is not None and isdir_fn(remote_path) is True:
                raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), remote_path)
            first_candidate = f"{remote_path}.001.bu"
            if not self._ftp.path.exists(first_candidate):
                backup_filename = first_candidate
            else:
                import posixpath
                parent = posixpath.dirname(remote_path) or "."
                basename = posixpath.basename(remote_path)
                entries = self._ftp.listdir(parent)
                backup_filename = _select_backup_filename(
                    remote_path, basename, entries, reserved_slots=(1,)
                )
            self._ftp.rename(remote_path, backup_filename)

        raw_fh = self._ftp.open(remote_path, 'wb')
        self._raw_fh = raw_fh
        adapted = _BinaryStreamAdapter(raw_fh, readable=False, writable=True)
        self._raw_fh = adapted
        target = self._make_counted_stream(adapted)
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            cctx = zstd.ZstdCompressor() if compresslevel is None else zstd.ZstdCompressor(level=compresslevel)
            sw = cctx.stream_writer(target, closefd=False)
            self._fh = (
                sw
                if self.binary
                else io.TextIOWrapper(sw, encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.bz2'):
            import bz2
            bz2_kwargs = (
                {'mode': 'wb'}
                if self.binary
                else {'mode': 'wt', 'encoding': self._encoding, 'errors': self._errors, 'newline': self._newline}
            )
            if compresslevel is not None:
                bz2_kwargs['compresslevel'] = compresslevel
            self._fh = bz2.open(target, **bz2_kwargs)
        elif norm.endswith('.xz'):
            import lzma
            lzma_kwargs = (
                {'mode': 'wb'}
                if self.binary
                else {'mode': 'wt', 'encoding': self._encoding, 'errors': self._errors, 'newline': self._newline}
            )
            if compresslevel is not None:
                lzma_kwargs['preset'] = compresslevel
            self._fh = lzma.open(target, **lzma_kwargs)
        elif norm.endswith('.gz'):
            import gzip
            gz_kwargs = (
                {'mode': 'wb'}
                if self.binary
                else {'mode': 'wt', 'encoding': self._encoding, 'errors': self._errors, 'newline': self._newline}
            )
            if compresslevel is not None:
                gz_kwargs['compresslevel'] = compresslevel
            self._fh = gzip.open(target, **gz_kwargs)
        else:
            self._fh = (
                target
                if self.binary
                else io.TextIOWrapper(target, encoding=self._encoding, errors=self._errors, newline=self._newline)
            )

    def _ssh_backup_if(self, sftp, path):
        """
        Backup the specified file on the SSH/SFTP server if it exists.

        Checks `<path>.001.bu` first in O(1); if it already exists, lists the parent
        directory once and selects the lowest free 001..999 slot (or collision-safe
        timestamp fallback when all 999 slots are occupied), retrying on rename collision.

        Parameters
        ----------
        sftp : paramiko.SFTPClient
            The SFTP client instance.
        path : str
            The path of the file to be backed up.

        Returns
        -------
        None
        """
        try:
            st = sftp.stat(path)
        except FileNotFoundError:
            return
        st_mode = getattr(st, 'st_mode', None)
        if isinstance(st_mode, int) and not isinstance(st_mode, bool):
            import stat
            if stat.S_ISDIR(st_mode):
                raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), path)

        first_candidate = f"{path}.001.bu"
        try:
            sftp.stat(first_candidate)
        except FileNotFoundError:
            try:
                sftp.rename(path, first_candidate)
                return
            except OSError:
                try:
                    sftp.stat(path)
                except FileNotFoundError:
                    return

        import posixpath
        parent = posixpath.dirname(path) or "."
        basename = posixpath.basename(path)
        reserved = {1}
        last_err = None
        for _ in range(5):
            entries = sftp.listdir(parent)
            backup_filename = _select_backup_filename(
                path, basename, entries, reserved_slots=reserved
            )
            try:
                sftp.rename(path, backup_filename)
                return
            except OSError as exc:
                last_err = exc
                try:
                    sftp.stat(path)
                except FileNotFoundError:
                    return
                suffix_part = backup_filename[len(path) + 1:-3]
                if len(suffix_part) == 3 and suffix_part.isascii() and suffix_part.isdigit():
                    reserved.add(int(suffix_part))
        if last_err is not None:
            raise last_err

    def _wrap_ssh(self, parsed: SplitResult, append: bool, backup: bool, compresslevel: Optional[int] = None):
        """
        Wrap self._fh to handle SSH/SFTP connections and file operations.

        This method establishes an SSH connection using paramiko, opens the specified file via SFTP,
        and wraps it with appropriate compression handlers based on the file extension.

        Parameters
        ----------
        parsed : SplitResult
            The parsed URL containing the SSH/SFTP connection details and file path.
        append : bool
            Whether to open the file in append mode.
        backup : bool
            Whether to create a backup of the file if it exists.
        compresslevel : int or None, optional
            Optional compression level/preset forwarded to the underlying compressor.

        Returns
        -------
        None
        """
        import paramiko
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        port = 22 if parsed.port is None else parsed.port
        username, password, remote_path = _unquote_remote_parts(parsed)
        norm = _normalized_path(self.filename, parsed)
        sftp_mode = 'ab' if append else 'wb'
        connect_kwargs = {
            'port': port,
            'username': username,
            'password': password,
        }
        if self.timeout is not None:
            connect_kwargs['timeout'] = self.timeout
            connect_kwargs['banner_timeout'] = self.timeout
            connect_kwargs['auth_timeout'] = self.timeout
        if self._key_filename is not None:
            connect_kwargs['key_filename'] = self._key_filename

        try:
            self._ssh_client = client
            client.connect(parsed.hostname, **connect_kwargs)
            sftp = client.open_sftp()
            self._sftp_client = sftp
            if self.timeout is not None:
                get_ch = getattr(sftp, 'get_channel', None)
                channel = get_ch() if get_ch is not None else None
                settimeout_fn = getattr(channel, 'settimeout', None)
                if settimeout_fn is not None:
                    settimeout_fn(self.timeout)

            if backup:
                self._ssh_backup_if(sftp, remote_path)

            sftp_file = sftp.file(remote_path, sftp_mode)
            self._sftp_file = sftp_file

            set_pipelined_fn = getattr(sftp_file, 'set_pipelined', None)
            if set_pipelined_fn is not None:
                set_pipelined_fn(True)
        except BaseException:
            self.close()
            raise

        codec_mode = ('ab' if append else 'wb') if self.binary else ('at' if append else 'wt')
        if norm.endswith('.zst'):
            import zstandard as zstd
            cctx = zstd.ZstdCompressor() if compresslevel is None else zstd.ZstdCompressor(level=compresslevel)
            target = self._make_counted_stream(self._sftp_file)
            sw = cctx.stream_writer(target, closefd=False)
            self._fh = (
                sw
                if self.binary
                else io.TextIOWrapper(sw, encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.bz2'):
            import bz2
            bz2_kwargs = {'mode': codec_mode}
            if not self.binary:
                bz2_kwargs['encoding'] = self._encoding
                bz2_kwargs['errors'] = self._errors
                bz2_kwargs['newline'] = self._newline
            if compresslevel is not None:
                bz2_kwargs['compresslevel'] = compresslevel
            target = self._make_counted_stream(self._sftp_file)
            self._fh = bz2.open(target, **bz2_kwargs)
        elif norm.endswith('.xz'):
            import lzma
            lzma_kwargs = {'mode': codec_mode}
            if not self.binary:
                lzma_kwargs['encoding'] = self._encoding
                lzma_kwargs['errors'] = self._errors
                lzma_kwargs['newline'] = self._newline
            if compresslevel is not None:
                lzma_kwargs['preset'] = compresslevel
            target = self._make_counted_stream(self._sftp_file)
            self._fh = lzma.open(target, **lzma_kwargs)
        elif norm.endswith('.gz'):
            import gzip
            gz_kwargs = {'mode': codec_mode}
            if not self.binary:
                gz_kwargs['encoding'] = self._encoding
                gz_kwargs['errors'] = self._errors
                gz_kwargs['newline'] = self._newline
            if compresslevel is not None:
                gz_kwargs['compresslevel'] = compresslevel
            target = self._make_counted_stream(self._sftp_file)
            self._fh = gzip.open(target, **gz_kwargs)
        else:
            adapted = _BinaryStreamAdapter(self._sftp_file, readable=False, writable=True)
            target = self._make_counted_stream(adapted)
            self._fh = (
                target
                if self.binary
                else io.TextIOWrapper(target, encoding=self._encoding, errors=self._errors, newline=self._newline)
            )

    def _backup_local_file(self):
        """Atomically move an existing local file to the next free sequential .bu slot."""
        src = self.filename
        if not os.path.lexists(src):
            return
        if os.path.isdir(src) and not os.path.islink(src):
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), src)

        def _rename_fallback(dst: str) -> str:
            if os.path.lexists(dst):
                return 'exists'
            try:
                os.rename(src, dst)
            except FileNotFoundError:
                if not os.path.lexists(src):
                    return 'gone'
                raise
            return 'moved'

        def _try_move(dst: str) -> str:
            """Return 'moved', 'exists', or 'gone'."""
            link_fn = getattr(os, 'link', None)
            supports_no_follow = link_fn is not None and link_fn in getattr(os, 'supports_follow_symlinks', ())
            if not supports_no_follow and os.path.islink(src):
                return _rename_fallback(dst)
            try:
                if supports_no_follow:
                    os.link(src, dst, follow_symlinks=False)
                else:
                    os.link(src, dst)
            except FileExistsError:
                return 'exists'
            except FileNotFoundError:
                if not os.path.lexists(src):
                    return 'gone'
                return _rename_fallback(dst)
            except (NotImplementedError, AttributeError):
                return _rename_fallback(dst)
            except OSError as exc:
                if exc.errno in _LINK_FALLBACK_ERRNOS:
                    if os.path.isdir(src) and not os.path.islink(src):
                        raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), src) from exc
                    return _rename_fallback(dst)
                raise
            try:
                os.unlink(src)
            except FileNotFoundError:
                pass
            return 'moved'

        first_candidate = f"{src}.001.bu"
        if not os.path.lexists(first_candidate):
            outcome = _try_move(first_candidate)
            if outcome in ('moved', 'gone'):
                return

        parent = os.path.dirname(src) or "."
        basename = os.path.basename(src)
        with os.scandir(parent) as it:
            names = [entry.name for entry in it]
        used_slots, existing_backups = _parse_backup_entries(basename, names)
        used_slots.add(1)

        for num in range(1, 1000):
            if num in used_slots:
                continue
            candidate = f"{src}.{num:03d}.bu"
            outcome = _try_move(candidate)
            if outcome in ('moved', 'gone'):
                return

        ts_ns = time.time_ns()
        for _ in range(10000):
            bu_name = f"{basename}.{ts_ns}.bu"
            if bu_name in existing_backups:
                ts_ns += 1
                continue
            candidate = f"{src}.{ts_ns}.bu"
            outcome = _try_move(candidate)
            if outcome in ('moved', 'gone'):
                return
            existing_backups.add(bu_name)
            ts_ns += 1
        raise FileExistsError(f"Could not allocate unique backup name for {src}")

    def _wrap_local(self, append: bool, backup: bool, parsed: Optional[SplitResult] = None, compresslevel: Optional[int] = None):
        """
        Wrap self._fh to handle local file operations with optional compression.

        This method opens the specified local file for writing and wraps it with appropriate
        compression handlers based on the file extension. It also handles file appending and backup creation.

        Parameters
        ----------
        append : bool
            Whether to open the file in append mode.
        backup : bool
            Whether to create a backup of the file if it exists.
        parsed : SplitResult or None, optional
            The parsed path/URL (parsed from self.filename if omitted).
        compresslevel : int or None, optional
            Optional compression level/preset forwarded to the underlying compressor.

        Returns
        -------
        None
        """
        if backup:
            self._backup_local_file()

        norm = _normalized_path(self.filename, parsed)
        mode = 'ab' if append else 'wb'
        codec_mode = mode if self.binary else ('at' if append else 'wt')
        if norm.endswith('.zst'):
            import zstandard as zstd
            cctx = zstd.ZstdCompressor() if compresslevel is None else zstd.ZstdCompressor(level=compresslevel)
            self._raw_fh = open(self.filename, mode)
            target = self._make_counted_stream(self._raw_fh)
            sw = cctx.stream_writer(target, closefd=False)
            self._fh = (
                sw
                if self.binary
                else io.TextIOWrapper(sw, encoding=self._encoding, errors=self._errors, newline=self._newline)
            )
        elif norm.endswith('.bz2'):
            import bz2
            bz2_kwargs = {'mode': codec_mode}
            if not self.binary:
                bz2_kwargs['encoding'] = self._encoding
                bz2_kwargs['errors'] = self._errors
                bz2_kwargs['newline'] = self._newline
            if compresslevel is not None:
                bz2_kwargs['compresslevel'] = compresslevel
            if self.show_progress:
                self._raw_fh = open(self.filename, mode)
                self._fh = bz2.open(self._make_counted_stream(self._raw_fh), **bz2_kwargs)
            else:
                self._fh = bz2.open(self.filename, **bz2_kwargs)
        elif norm.endswith('.xz'):
            import lzma
            lzma_kwargs = {'mode': codec_mode}
            if not self.binary:
                lzma_kwargs['encoding'] = self._encoding
                lzma_kwargs['errors'] = self._errors
                lzma_kwargs['newline'] = self._newline
            if compresslevel is not None:
                lzma_kwargs['preset'] = compresslevel
            if self.show_progress:
                self._raw_fh = open(self.filename, mode)
                self._fh = lzma.open(self._make_counted_stream(self._raw_fh), **lzma_kwargs)
            else:
                self._fh = lzma.open(self.filename, **lzma_kwargs)
        elif norm.endswith('.gz'):
            import gzip
            gz_kwargs = {'mode': codec_mode}
            if not self.binary:
                gz_kwargs['encoding'] = self._encoding
                gz_kwargs['errors'] = self._errors
                gz_kwargs['newline'] = self._newline
            if compresslevel is not None:
                gz_kwargs['compresslevel'] = compresslevel
            if self.show_progress:
                self._raw_fh = open(self.filename, mode)
                self._fh = gzip.open(self._make_counted_stream(self._raw_fh), **gz_kwargs)
            else:
                self._fh = gzip.open(self.filename, **gz_kwargs)
        else:
            if self.show_progress:
                self._raw_fh = open(self.filename, mode)
                target = self._make_counted_stream(self._raw_fh)
                self._fh = (
                    target
                    if self.binary
                    else io.TextIOWrapper(target, encoding=self._encoding, errors=self._errors, newline=self._newline)
                )
            else:
                self._fh = (
                    open(self.filename, mode)
                    if self.binary
                    else open(
                        self.filename,
                        'a' if append else 'w',
                        encoding=self._encoding,
                        errors=self._errors,
                        newline=self._newline,
                    )
                )

    def open(self, append: bool = False, backup: bool = True, compresslevel: Optional[int] = None) -> 'OpenAsWriter':
        """
        Open the file for writing, compressing it on the fly if necessary.

        This method determines the file type and source, and opens the file
        with the appropriate handler and compression method.

        Parameters
        ----------
        append : bool, optional
            Whether to open the file in append mode (default is False).
        backup : bool, optional
            Whether to create a backup of the file if it exists (default is True).
        compresslevel : int or None, optional
            Optional compression level/preset forwarded to `.zst`, `.gz`, `.bz2`, or `.xz`
            writers; when None, each compression library uses its own default.

        Returns
        -------
        OpenAsWriter
            The OpenAsWriter instance with the file opened for writing.

        Raises
        ------
        ValueError
            If both append and backup are True.
        """
        if self._fh is not None:
            return self

        self.filename, parsed, scheme = _resolve_target(self.filename)
        norm = _normalized_path(self.filename, parsed)

        if norm.endswith(UNSUPPORTED_ARCHIVES):
            raise UnsupportedArchiveError(
                f"openas does not support writing or appending to archive formats safely: {_redact_url(self.filename)}. "
                f"Use Python's zipfile or tarfile module instead."
            )

        if append and backup:
            raise ValueError(
                "Cannot set both append=True and backup=True. "
                "Pass backup=False when appending (or use openas(path, 'a') / openas(path, 'ab'), which defaults backup=False)."
            )

        if scheme in ('http', 'https'):
            raise ReadOnlyProtocolError(
                f"Protocol '{parsed.scheme}' does not support write/append operations in openas. "
                "HTTP/HTTPS is read-only; write to a local path, ftp://, or sftp:// target instead."
            )

        _validate_write_options(norm, compresslevel)

        try:
            if scheme == 'ftp':
                self._wrap_ftp(parsed, append, backup, compresslevel=compresslevel)
            elif scheme in ('sftp', 'ssh'):
                self._wrap_ssh(parsed, append, backup, compresslevel=compresslevel)
            else:
                self._wrap_local(append, backup, parsed, compresslevel=compresslevel)

            if self.show_progress:
                from tqdm import tqdm
                initial_bytes = (
                    self._counting_stream.bytes_written
                    if self._counting_stream is not None
                    else 0
                )
                self._progress = tqdm(
                    total=None,
                    initial=initial_bytes,
                    unit='B',
                    unit_scale=True,
                )
                if self._counting_stream is not None:
                    self._counting_stream.set_progress_callback(self._progress.update)
        except BaseException:
            self.close()
            raise

        self._append = bool(append)
        if type(self).write is OpenAsWriter.write:
            self.write = self._fh.write

        return self

    def write(self, data):
        """
        Write data to the file.

        Parameters
        ----------
        data : str or bytes-like
            The data to write to the file (`str` in text mode, `bytes`-like in binary mode).

        Returns
        -------
        int
            The number of characters (text mode) or bytes (binary mode) written.
        """
        if self._fh is None:
            raise ValueError("I/O operation on closed file.")
        return self._fh.write(data)

    def writelines(self, lines) -> None:
        """Write an iterable of lines (`str` in text mode, `bytes`-like in binary mode) to the file."""
        if self._fh is None:
            raise ValueError("I/O operation on closed file.")
        if self.binary:
            for line in lines:
                self._fh.write(line)
        else:
            self._fh.writelines(lines)

    def flush(self) -> None:
        """Flush buffered write data to the underlying stream."""
        if self._fh is None:
            raise ValueError("I/O operation on closed file.")
        self._fh.flush()
        if self._counting_stream is not None:
            self._counting_stream.flush_progress()

    def close(self):
        """
        Close the file.

        This method closes the file handle and any associated resources idempotently,
        resetting all handle and progress attributes even if one handle fails to close.

        Returns
        -------
        None
        """
        self.__dict__.pop('write', None)
        fh = self._fh
        counting_stream = self._counting_stream
        raw_fh = self._raw_fh
        sftp_file = self._sftp_file
        sftp_client = self._sftp_client
        ssh_client = self._ssh_client
        ftp = self._ftp
        progress = self._progress

        self._append = False
        self._fh = None
        self._counting_stream = None
        self._raw_fh = None
        self._sftp_file = None
        self._sftp_client = None
        self._ssh_client = None
        self._ftp = None
        self._progress = None

        first_err = None
        seen = set()

        # Close fh first while progress callback is still active so TextIOWrapper
        # and compressors flush buffered payload and trailer bytes through _CountingWriteStream.
        if fh is not None:
            seen.add(id(fh))
            if getattr(fh, 'closed', False) is True:
                _record_raw_close_attempted(fh, seen)
            else:
                close_fn = getattr(fh, 'close', None)
                if close_fn is not None:
                    try:
                        close_fn()
                    except BaseException as err:
                        first_err = err
                _record_raw_close_attempted(fh, seen)

        if counting_stream is not None:
            try:
                counting_stream.flush_progress()
            except BaseException as err:
                if first_err is None:
                    first_err = err
            finally:
                counting_stream.set_progress_callback(None)

        remaining = [
            h
            for h in (counting_stream, raw_fh, sftp_file, sftp_client, ssh_client, ftp, progress)
            if h is not None and id(h) not in seen
        ]
        try:
            _close_handles(*remaining)
        except BaseException as err:
            if first_err is None:
                first_err = err

        if first_err is not None:
            raise first_err


def openas(
    filename: Union[str, os.PathLike],
    mode: str = 'r',
    backup: Optional[bool] = None,
    show_progress: bool = False,
    compresslevel: Optional[int] = None,
    timeout: Optional[float] = None,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    headers: Optional[Mapping[str, str]] = None,
    key_filename: Optional[Union[str, os.PathLike, Sequence[Union[str, os.PathLike]]]] = None,
) -> Union[OpenAsReader, OpenAsWriter]:
    """
    Open a local or remote stream with transparent compression in text (`str`) or binary (`bytes`) mode.

    Supports plain, `.gz`, `.bz2`, `.xz`, and `.zst` over local paths (`file://` or implicit),
    `http://`/`https://` (read-only), `ftp://` (read/write), and `ssh://`/`sftp://` (read/write/append).

    Text modes (`'r'`/`'rt'`, `'w'`/`'wt'`, `'a'`/`'at'`) decode and encode `str` (UTF-8 by default,
    configurable via `encoding`, `errors`, and `newline`).
    Binary modes (`'rb'`, `'wb'`, `'ab'`) read and write raw `bytes`.

    Parameters
    ----------
    filename : str or os.PathLike
        Path or URL to open (extension matching is case-insensitive and ignores URL query strings).
    mode : str, optional
        One of `'r'`, `'rt'` (text read, default `'r'`), `'rb'` (binary read),
        `'w'`, `'wt'` (text write/truncate), `'wb'` (binary write/truncate),
        `'a'`, `'at'` (text append), or `'ab'` (binary append).
    backup : bool or None, optional
        Move an existing target to sequential `.001.bu`..`.999.bu` (or nanosecond timestamp fallback)
        before overwriting. Defaults to `True` in write modes and `False` in append modes; passing
        `backup=True` with an append mode raises `ValueError`.
    show_progress : bool, optional
        Display a `tqdm` progress bar tracking exact raw bytes read (`OpenAsReader`) or written
        (`OpenAsWriter`, with `total=None`). Default is `False`.
    compresslevel : int or None, optional
        Optional compression level/preset forwarded to `.zst` (default 3), `.xz` (default 6),
        `.bz2` (default 9), and `.gz` (default 9) writers; ignored in read modes.
    timeout : float or None, optional
        Optional network timeout in seconds for HTTP, FTP, and SSH/SFTP connections.
    encoding : str or None, optional
        Text encoding in text modes (defaults to `'utf-8'`). Passing a non-None value in binary mode
        raises `ValueError`.
    errors : str or None, optional
        Codec error handling scheme in text modes (defaults to Python's `'strict'`). Passing a non-None
        value in binary mode raises `ValueError`.
    newline : str or None, optional
        Newline translation mode (`None`, `''`, `'\\n'`, `'\\r'`, or `'\\r\\n'`) matching `builtins.open()`.
        Passing a non-None value in binary mode raises `ValueError`.
    headers : Mapping[str, str] or None, optional
        Optional per-request HTTP/HTTPS headers. Validated unconditionally; valid headers are ignored
        on non-HTTP targets and write/append modes.
    key_filename : str, os.PathLike, Sequence[str | os.PathLike], or None, optional
        Optional SSH private key path or non-empty sequence of paths forwarded to Paramiko `connect()`
        on `ssh://` and `sftp://` targets.

    Returns
    -------
    OpenAsReader or OpenAsWriter
        An opened reader or writer context manager.

    Raises
    ------
    UnsupportedArchiveError
        If the target matches a container archive extension (e.g., `.zip`, `.tar`, `.tar.gz`, `.tar.xz`).
    ReadOnlyProtocolError
        If writing or appending to `http://` or `https://`.
    UnsupportedProtocolError
        If invoking an unsupported scheme (e.g., `s3://`, `gs://`, `az://`, `hdfs://`) or FTP append.
    IsADirectoryError
        If attempting a local, SFTP, or FTP `.bu` backup on an existing directory when writing.
    EOFError
        If reading a truncated `.zst` stream across local, HTTP, FTP, or SFTP transports.
    LookupError
        If `encoding` names an unknown text codec.
    TypeError
        If `encoding`, `errors`, `newline`, `headers`, `key_filename`, or `compresslevel` has an invalid type.
    ValueError
        If `mode` is unsupported, `backup=True` is combined with append, `timeout` is non-positive
        or non-finite, text options are passed in binary mode, `newline` is invalid, `headers` contains
        an empty name or CR/LF/NUL characters, `key_filename` is empty, a URL omits a hostname, specifies
        an invalid or zero port (outside `1..65535`), or has an empty path on `file://`, `ftp://`,
        `ssh://`, or `sftp://`, or `compresslevel` is out of range.

    Examples
    --------
    >>> with openas("data.gz", "w", compresslevel=6) as f:
    ...     f.write("hello\\n")
    >>> with openas("data.gz", "r") as f:
    ...     print(f.read())
    >>> with openas("data.zst", "rb") as f:
    ...     raw_line = f.readline()
    """
    filename = os.fsdecode(filename)
    if mode in ('r', 'rt', 'rb'):
        return OpenAsReader(
            filename,
            show_progress=show_progress,
            timeout=timeout,
            binary=(mode == 'rb'),
            encoding=encoding,
            errors=errors,
            newline=newline,
            headers=headers,
            key_filename=key_filename,
        ).open()
    elif mode in ('w', 'wt', 'wb'):
        _validate_headers(headers)
        effective_backup = True if backup is None else backup
        return OpenAsWriter(
            filename,
            show_progress=show_progress,
            timeout=timeout,
            binary=(mode == 'wb'),
            encoding=encoding,
            errors=errors,
            newline=newline,
            key_filename=key_filename,
        ).open(
            append=False,
            backup=effective_backup,
            compresslevel=compresslevel,
        )
    elif mode in ('a', 'at', 'ab'):
        _validate_headers(headers)
        effective_backup = False if backup is None else backup
        return OpenAsWriter(
            filename,
            show_progress=show_progress,
            timeout=timeout,
            binary=(mode == 'ab'),
            encoding=encoding,
            errors=errors,
            newline=newline,
            key_filename=key_filename,
        ).open(
            append=True,
            backup=effective_backup,
            compresslevel=compresslevel,
        )
    else:
        raise ValueError(
            f"Unsupported mode: '{mode}'. Supported modes are 'r', 'rt', 'rb', 'w', 'wt', 'wb', 'a', 'at', 'ab'."
        )


def main():
    """
    Core CLI runner for `openas` (wrapped by `_cli_main()` for console-script and `python -m openas` invocation).

    Streams 64 KiB binary chunks for `--read` (with newline-delimited line and byte
    throughput reporting) and single-pass multi-target `--write` across plain, `.gz`,
    `.bz2`, `.xz`, and `.zst` files over local paths, HTTP/HTTPS, FTP, and SSH/SFTP.
    """
    def format_rate(rate: float) -> str:
        """Format byte rate with standard units."""
        if not rate:
            return "0.0B/s"
        return f"{format_bytes(rate)}/s"

    def pinfo(preface: str, filename: str, line_count: int, bytes_count: int, seconds: float):
        """Print processing summary statistics for a file."""
        rate = line_count / seconds if seconds > 0 else 0
        byterate = bytes_count / seconds if seconds > 0 else 0
        print(
            f"{preface} {filename}. "
            f"{line_count:,d} lines / {format_bytes(bytes_count)} "
            f"at {rate:,.1f}/s ({format_rate(byterate)}). "
            f"Total time: {seconds:0.2f} Secs."
        )

    import argparse
    parser = argparse.ArgumentParser(
        prog="openas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Read or broadcast binary and text streams across plain, .gz, .bz2, .xz, and .zst files "
            "over local paths, HTTP/HTTPS, FTP, and SSH/SFTP."
        ),
        epilog=(
            "Examples:\n"
            "  openas --read data.txt.gz https://example.com/log.zst\n"
            "  openas --write source.gz copy.txt backup.bz2\n"
            "  openas --write -i source.gz copy.txt sftp://user@host/abs/path/dest.zst\n\n"
            "Notes:\n"
            "  --write moves existing destination files to sequential .001.bu backups before overwriting (pass --no-backup to disable).\n"
            "  sftp://user@host/abs/path uses absolute remote paths and system SSH keys / ~/.ssh/known_hosts."
        ),
    )

    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {__version__}',
    )
    parser.add_argument(
        'files',
        type=str,
        nargs='*',
        default=[],
        metavar="file",
        help=(
            "Target path(s) or URL(s): files to read with --read; SRC DEST... with --write "
            "(or DEST... when --input-file/-i is given)."
        ),
    )
    parser.add_argument(
        '--input-file',
        '-i',
        type=str,
        default=None,
        help='Source path or URL for --write mode (makes all positional file arguments destinations).',
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        default=False,
        help='Overwrite existing destination files in --write mode without creating .001.bu backups.',
    )
    parser.add_argument(
        '--compresslevel',
        '-c',
        type=int,
        default=None,
        help='Compression level or preset for compressed --write destinations (.gz 0..9, .bz2 1..9, .xz 0..9, .zst up to 22).',
    )
    parser.add_argument(
        '--timeout',
        type=float,
        default=None,
        help='Network timeout in seconds for HTTP/HTTPS, FTP, and SSH/SFTP transfers (finite positive number).',
    )
    parser.add_argument(
        '--progress',
        action='store_true',
        default=False,
        help='Display a tqdm raw-byte progress bar on stderr while reading the input stream.',
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--write',
        '-w',
        action='store_true',
        help=(
            'Read one source stream in 64 KiB binary chunks and broadcast exact bytes to one or more destinations '
            '(moves existing destinations to sequential .001.bu backups before writing unless --no-backup is set).'
        ),
    )
    group.add_argument(
        '--read',
        '-r',
        action='store_true',
        help='Read one or more files sequentially in 64 KiB binary chunks and report line/byte throughput.',
    )

    args = parser.parse_args()

    if args.read:
        if args.input_file is not None:
            parser.error("--input-file/-i is only valid with --write")
        if args.no_backup:
            parser.error("--no-backup is only valid with --write")
        if args.compresslevel is not None:
            parser.error("--compresslevel/-c is only valid with --write")
        if len(args.files) < 1:
            parser.error("Read mode requires at least one file (e.g., 'openas -r path/to/file.gz')")
        _validate_timeout(args.timeout)
        for input_file in args.files:
            print(f"Reading {_redact_url(input_file)}")
            with OpenAsReader(
                input_file,
                show_progress=args.progress,
                timeout=args.timeout,
                binary=True,
            ) as reader:
                line_count = 0
                bytes_count = 0
                last_byte = None
                t0 = time.monotonic()
                read_fn = reader.read
                while True:
                    chunk = read_fn(_CLI_CHUNK_SIZE)
                    if not chunk:
                        break
                    bytes_count += len(chunk)
                    line_count += chunk.count(b'\n')
                    last_byte = chunk[-1]
                if bytes_count > 0 and last_byte != 10:
                    line_count += 1
                pinfo("FINISHED Reading.", _redact_url(reader.filename), line_count, bytes_count, time.monotonic() - t0)
    else:
        if args.input_file is not None:
            if len(args.files) < 1:
                parser.error("Write mode with --input-file/-i requires at least one destination file")
            source_file = args.input_file
            output_files = args.files
        else:
            if len(args.files) < 2:
                parser.error("Write mode requires at least one source file and one destination file (e.g., 'openas -w src dest')")
            source_file = args.files[0]
            output_files = args.files[1:]

        # Pre-validate timeout, destination schemes, archive guards, compression levels,
        # local directories, source aliasing, and duplicates before backing up or truncating outputs.
        _validate_timeout(args.timeout)

        def _canonical_target_keys(resolved_target: str, parsed_target, scheme_target: str):
            if not scheme_target:
                abs_path = os.path.abspath(resolved_target)
                entry_key = (
                    'local',
                    os.path.normcase(
                        os.path.join(os.path.realpath(os.path.dirname(abs_path)), os.path.basename(abs_path))
                    ),
                )
                real_key = ('local-real', os.path.normcase(os.path.realpath(abs_path)))
                return entry_key, real_key
            from urllib.parse import unquote
            default_port = 21 if scheme_target == 'ftp' else (80 if scheme_target == 'http' else (443 if scheme_target == 'https' else 22))
            effective_port = default_port if parsed_target.port is None else parsed_target.port
            remote_key = (
                'sftp' if scheme_target == 'ssh' else scheme_target,
                parsed_target.hostname.lower(),
                effective_port,
                unquote(parsed_target.path),
            )
            return remote_key, remote_key

        resolved_src, parsed_src, scheme_src = _resolve_target(source_file)
        src_entry_key, src_real_key = _canonical_target_keys(resolved_src, parsed_src, scheme_src)

        seen_destinations = set()
        seen_real_destinations = set()
        for output_file in output_files:
            resolved_out, parsed_out, scheme_out = _resolve_target(output_file)
            norm_out = _normalized_path(resolved_out, parsed_out)
            if norm_out.endswith(UNSUPPORTED_ARCHIVES):
                raise UnsupportedArchiveError(
                    f"openas does not support writing or appending to archive formats safely: {_redact_url(output_file)}. "
                    f"Use Python's zipfile or tarfile module instead."
                )
            if scheme_out in ('http', 'https'):
                raise ReadOnlyProtocolError(
                    f"Protocol '{parsed_out.scheme}' does not support write/append operations in openas. "
                    "HTTP/HTTPS is read-only; write to a local path, ftp://, or sftp:// target instead."
                )
            _validate_write_options(norm_out, args.compresslevel)
            if not scheme_out:
                if os.path.isdir(resolved_out) and not os.path.islink(resolved_out):
                    raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), resolved_out)
            dest_key, dest_real_key = _canonical_target_keys(resolved_out, parsed_out, scheme_out)
            if dest_key == src_entry_key or (args.no_backup and dest_real_key == src_real_key):
                parser.error(f"Destination cannot be the same as source: {_redact_url(output_file)}")
            if dest_key in seen_destinations or (args.no_backup and dest_real_key in seen_real_destinations):
                parser.error(f"Duplicate destination specified: {_redact_url(output_file)}")
            seen_destinations.add(dest_key)
            seen_real_destinations.add(dest_real_key)

        import contextlib
        print(f"Will Read from {_redact_url(source_file)}")
        line_count = 0
        bytes_count = 0
        last_byte = None
        dest_names = []
        t0 = time.monotonic()
        with contextlib.ExitStack() as stack:
            reader = stack.enter_context(
                OpenAsReader(
                    source_file,
                    show_progress=args.progress,
                    timeout=args.timeout,
                    binary=True,
                )
            )
            writers = []
            for output_file in output_files:
                writer = stack.enter_context(
                    OpenAsWriter(
                        output_file,
                        timeout=args.timeout,
                        binary=True,
                    ).open(
                        append=False,
                        backup=not args.no_backup,
                        compresslevel=args.compresslevel,
                    )
                )
                print(f"Writing {_redact_url(writer.filename)}")
                writers.append(writer)
                dest_names.append(_redact_url(writer.filename))
            read_fn = reader.read
            write_fns = [w.write for w in writers]
            while True:
                chunk = read_fn(_CLI_CHUNK_SIZE)
                if not chunk:
                    break
                bytes_count += len(chunk)
                line_count += chunk.count(b'\n')
                last_byte = chunk[-1]
                for write_fn in write_fns:
                    write_fn(chunk)
            if bytes_count > 0 and last_byte != 10:
                line_count += 1
        elapsed = time.monotonic() - t0
        for filename in dest_names:
            pinfo("FINISHED Writing.", filename, line_count, bytes_count, elapsed)


_CLI_OPERATIONAL_EXCEPTION_MODULES = frozenset((
    'paramiko', 'ftputil', 'ftplib', 'zstandard', 'requests', 'urllib3', 'http', 'lzma', '_lzma', 'zlib',
))


def _is_cli_operational_error(exc: BaseException) -> bool:
    """Return True for expected operational CLI failures without eager third-party or codec imports."""
    if isinstance(exc, (OpenAsError, OSError, EOFError, ValueError, NotImplementedError)):
        return True
    top_module = type(exc).__module__.split('.', 1)[0]
    return top_module in _CLI_OPERATIONAL_EXCEPTION_MODULES


def _redact_urls_in_text(text: str) -> str:
    """Mask URL passwords and query strings in embedded URL tokens (imports re lazily)."""
    if '://' not in text:
        return text
    import re

    def _replace(match):
        token = match.group(0)
        trailing = ''
        while token and (
            token[-1] in '.,;:)'
            or (token[-1] == ']' and '[' not in token)
        ):
            trailing = token[-1] + trailing
            token = token[:-1]
        return _redact_url(token) + trailing

    return re.sub(r'[A-Za-z][A-Za-z0-9+.-]*://(?:<redacted>|[^\s\'"<>])+', _replace, text)


def _cli_main() -> None:
    """CLI entry wrapper formatting operational errors to stderr and exiting 1."""
    import sys

    try:
        main()
    except Exception as exc:
        if not _is_cli_operational_error(exc):
            raise
        msg = _redact_urls_in_text(str(exc)).strip()
        if msg:
            sys.stderr.write(f"openas: error ({type(exc).__name__}): {msg}\n")
        else:
            sys.stderr.write(f"openas: error ({type(exc).__name__})\n")
        sys.exit(1)


if __name__ == "__main__":
    _cli_main()

