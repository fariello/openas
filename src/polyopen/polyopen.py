#!/usr/bin/env python3
"""
polyopen - Unified UTF-8 stream I/O across local files, remote protocols, and compression formats.

Supports plain text, `.gz`, `.bz2`, `.xz`, and `.zst` streams over local files (`file://` or plain
paths), `http://` and `https://` (read-only), `ftp://` (read and write), and `ssh://` / `sftp://`
(read, write, and append). All modes operate on UTF-8 text (`str`).

Public API
----------
- `polyopen(filename, mode='r', backup=None, show_progress=False, compresslevel=None, timeout=None)`:
  Unified factory returning an opened `PolyReader` or `PolyWriter`.
- `PolyReader`: Stream reader supporting `read()`, `readline()`, `readlines()`, iteration, and
  optional raw-byte `tqdm` progress bars.
- `PolyWriter`: Stream writer supporting `write()`, `writelines()`, `flush()`, `compresslevel`,
  and collision-safe `.NNN.bu` backup rotation on overwrite.
- `PolyopenError`, `UnsupportedArchiveError`, `ReadOnlyProtocolError`, `UnsupportedProtocolError`:
  Exception hierarchy for unsupported archive containers and protocols.

Usage
-----
Read a local or remote compressed stream:
    >>> from polyopen import polyopen
    >>> with polyopen("path/to/data.gz", "r") as reader:
    ...     for line in reader:
    ...         process(line)

Read over SSH/SFTP using SSH keys (preferred over inline passwords):
    >>> with polyopen("sftp://user@hostname/path/to/remote.zst", "r") as reader:
    ...     content = reader.read()

Write a compressed file with automatic backup rotation:
    >>> with polyopen("path/to/output.bz2", "w", compresslevel=6) as writer:
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

class PolyopenError(Exception):
    """Base exception for all polyopen-specific errors."""

class UnsupportedArchiveError(PolyopenError):
    """Raised when a container archive format (such as .zip or .tar.gz) is passed to polyopen."""

class ReadOnlyProtocolError(PolyopenError):
    """Raised when attempting to write or append to a read-only protocol (http:// or https://)."""

class UnsupportedProtocolError(PolyopenError):
    """Raised when an unsupported protocol or operation (such as s3:// or FTP append) is requested."""

UNSUPPORTED_ARCHIVES = (
    '.zip', '.tar', '.tgz', '.rar', '.7z',
    '.gz.tar', '.bz2.tar', '.xz.tar', '.zst.tar',
    '.tar.gz', '.tar.bz2', '.tar.xz', '.tar.zst', '.tar.lzma',
    '.tbz', '.tbz2', '.txz', '.tzst', '.tlz',
)

import errno
import io
import os
import threading
import time
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from urllib.parse import ParseResult

_NETWORK_SCHEMES = frozenset(('http', 'https', 'ftp', 'ssh', 'sftp', 's3', 'gs', 'az', 'hdfs'))
_SUPPORTED_SCHEMES = frozenset(('file', 'http', 'https', 'ftp', 'ssh', 'sftp'))
_CLOUD_SCHEMES = frozenset(('s3', 'gs', 'az', 'hdfs'))
_LINK_FALLBACK_ERRNOS = frozenset(
    code
    for name in ('EOPNOTSUPP', 'ENOTSUP', 'ENOSYS', 'EPERM', 'EXDEV', 'EMLINK')
    for code in (getattr(errno, name, None),)
    if code is not None
)


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
        readinto_fn = getattr(self._raw, 'readinto', None)
        if readinto_fn is not None:
            return readinto_fn(b)
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def write(self, b):
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        data = bytes(b) if isinstance(b, memoryview) else b
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
        readinto_fn = getattr(self._raw, 'readinto', None)
        if readinto_fn is not None:
            return readinto_fn(b)
        data = self._raw.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def close(self):
        if not self._closed:
            self._closed = True
            close_fn = getattr(self._raw, 'close', None)
            if close_fn is not None:
                close_fn()


class _CountingByteStream:
    """Lightweight binary stream proxy that counts exact raw bytes read."""

    def __init__(self, stream):
        self._stream = stream
        self.bytes_read = 0

    def read(self, size: int = -1):
        data = self._stream.read(size)
        if data:
            self.bytes_read += len(data)
        return data

    def read1(self, size: int = -1):
        read1_fn = getattr(self._stream, 'read1', None)
        if read1_fn is not None:
            data = read1_fn(size)
        else:
            data = self._stream.read(size)
        if data:
            self.bytes_read += len(data)
        return data

    def readinto(self, b):
        readinto_fn = getattr(self._stream, 'readinto', None)
        if readinto_fn is not None:
            n = readinto_fn(b)
            if n:
                self.bytes_read += n
            return n
        data = self._stream.read(len(b))
        n = len(data)
        b[:n] = data
        self.bytes_read += n
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


def format_bytes(num: float) -> str:
    """Format bytes to human readable sizes."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}"
        num /= 1024.0
    return f"{num:3.1f}YB"


def _redact_url(target: str) -> str:
    """Mask URL passwords and query strings so credentials/tokens never leak in logs or errors."""
    if '://' not in target:
        return target
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(target)
    if parts.password is None and not parts.query:
        return target
    netloc = parts.netloc
    if parts.password is not None and '@' in netloc:
        userinfo, hostport = netloc.rsplit('@', 1)
        if ':' in userinfo:
            user, _ = userinfo.split(':', 1)
            netloc = f"{user}:***@{hostport}"
        else:
            netloc = f"***@{hostport}"
    query = '<redacted>' if parts.query else ''
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


def _validate_timeout(timeout: Optional[float]) -> Optional[float]:
    """Validate optional network timeout (positive float/int or None)."""
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ValueError("timeout must be a positive number or None.")
    return float(timeout)


def _validate_write_options(norm: str, compresslevel: Optional[int]) -> None:
    """Validate compresslevel and import optional codecs before rotating backups or opening files."""
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
    if ':' not in filename:
        return filename, None, ''
    from urllib.parse import urlparse
    parsed = urlparse(filename)
    scheme = parsed.scheme.lower()
    if scheme == 'file':
        if parsed.netloc and parsed.netloc.lower() != 'localhost':
            raise UnsupportedProtocolError(
                f"Unsupported remote host '{parsed.netloc}' in file:// URI: {_redact_url(filename)}. "
                f"Only local file:// URIs (empty host or localhost) are supported."
            )
        from urllib.request import url2pathname
        local_path = url2pathname(parsed.path)
        return local_path, None, ''
    if scheme in _CLOUD_SCHEMES:
        raise UnsupportedProtocolError(
            f"polyopen does not support cloud object stores ('{parsed.scheme}://'). "
            f"Use 'smart_open' or a dedicated cloud SDK (such as boto3) instead."
        )
    if scheme in _SUPPORTED_SCHEMES:
        if not parsed.hostname:
            raise ValueError(f"Missing hostname in {scheme}:// URL: {_redact_url(filename)}")
        return filename, parsed, scheme
    if len(scheme) > 1 and f"{scheme}://" in filename.lower():
        raise UnsupportedProtocolError(
            f"Unsupported protocol '{parsed.scheme}://'. "
            f"Supported protocols: file, http, https, ftp, ssh, sftp."
        )
    return filename, parsed, ''


def _unquote_remote_parts(parsed: ParseResult):
    """Percent-decode username, password, and path components of FTP/SSH/SFTP URLs."""
    from urllib.parse import unquote
    username = unquote(parsed.username) if parsed.username is not None else None
    password = unquote(parsed.password) if parsed.password is not None else None
    remote_path = unquote(parsed.path)
    return username, password, remote_path


def _normalized_path(filename: str, parsed: Optional[ParseResult] = None) -> str:
    """Return lowercase path component for network URLs and lowercase filename for local paths."""
    if parsed is not None and parsed.scheme.lower() in _NETWORK_SCHEMES:
        return parsed.path.lower()
    return filename.lower()


def _close_handles(*handles):
    """Close each distinct non-None handle in order, propagating the first error after closing all."""
    first_err = None
    seen = set()
    for handle in handles:
        if handle is None or id(handle) in seen:
            continue
        seen.add(id(handle))
        if getattr(handle, 'closed', False) is True:
            continue
        close_fn = getattr(handle, 'close', None)
        if close_fn is None:
            continue
        try:
            close_fn()
        except Exception as err:
            if first_err is None:
                first_err = err
        buf = getattr(handle, 'buffer', None)
        if isinstance(buf, _BinaryStreamAdapter) and buf._raw_close_attempted:
            seen.add(id(buf._raw))
        elif isinstance(handle, _BinaryStreamAdapter) and handle._raw_close_attempted:
            seen.add(id(handle._raw))
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
    session = getattr(local, 'session', None)
    if session is None:
        import atexit
        import http.cookiejar
        import requests
        session = requests.Session()
        session.cookies.set_policy(http.cookiejar.DefaultCookiePolicy(allowed_domains=[]))
        local.session = session
        with _http_session_lock:
            _http_sessions_registry.append(session)
            if not _http_atexit_registered:
                atexit.register(_close_registered_http_sessions)
                _http_atexit_registered = True
    return session


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
            It is primarily intended for internal use within the PolyReader and PolyWriter classes
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
            __init__(host, userid, password, port=21):
                Initializes the FTP connection and logs in with the provided credentials.
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


class PolyReader:
    """
    UTF-8 text stream reader supporting `.gz`, `.bz2`, `.xz`, and `.zst` decompression
    over local files (`file://` or plain paths), HTTP/HTTPS, FTP, and SSH/SFTP.

    Attributes
    ----------
    filename : str
        Normalized path or URL to read.
    show_progress : bool
        Whether to display a `tqdm` progress bar tracking raw bytes read.
    timeout : float or None
        Optional network timeout in seconds.
    encoding : str
        Always `'utf-8'`.
    closed : bool
        True when no underlying stream handle is open.

    Methods
    -------
    open() -> PolyReader
        Open the stream for reading.
    read(size=-1) -> str, readline(size=-1) -> str, readlines(hint=-1) -> list
        Standard text stream read operations.
    readable() -> bool, writable() -> bool, seekable() -> bool
        Standard file-like capability predicates.
    close() -> None
        Idempotently close all acquired stream and transport handles.
    """

    encoding = 'utf-8'

    @property
    def closed(self) -> bool:
        """Return True if the reader is closed."""
        return self._fh is None

    def readable(self) -> bool:
        """Return True (PolyReader is a read-only stream)."""
        return True

    def writable(self) -> bool:
        """Return False (PolyReader is a read-only stream)."""
        return False

    def seekable(self) -> bool:
        """Return False (PolyReader does not support random-access seeking)."""
        return False

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        return f"PolyReader({_redact_url(self.filename)!r}, state={state!r})"

    def __init__(
        self,
        filename: Union[str, os.PathLike],
        show_progress: bool = False,
        timeout: Optional[float] = None,
    ):
        """
        Initialize PolyReader with a filename.

        Parameters
        ----------
        filename : str or os.PathLike
            The name of the file to read.
        show_progress : bool
            Whether to show a progress bar.
        timeout : float or None, optional
            Optional network timeout in seconds for HTTP, FTP, and SSH/SFTP connections.
        """
        self.filename = os.fsdecode(filename)
        self.show_progress = show_progress
        self.timeout = _validate_timeout(timeout)
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
        self._progress_raw_bytes = 0
        self._progress_pending = 0
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

    def _flush_progress(self) -> None:
        """Apply any buffered progress delta to the progress bar once."""
        progress = self._progress
        if progress is None:
            self._progress_pending = 0
            return
        if self._counting_stream is not None:
            delta = self._counting_stream.bytes_read - self._progress_raw_bytes
            if delta > 0:
                self._progress_raw_bytes += delta
                self._progress_pending += delta
        if self._progress_pending > 0:
            progress.update(self._progress_pending)
            self._progress_pending = 0

    def _update_progress(self, text: str = ""):
        if not (self.show_progress and self._progress is not None):
            return
        if self._progress_pending > 0:
            self._flush_progress()
        if self._counting_stream is not None:
            delta = self._counting_stream.bytes_read - self._progress_raw_bytes
            if delta > 0:
                self._progress.update(delta)
                self._progress_raw_bytes += delta
        elif text:
            self._progress.update(len(text.encode('utf-8')))

    def __enter__(self) -> 'PolyReader':
        """
        Open the file when entering the context.

        This method is called when the context is entered using the `with` statement.

        Returns
        -------
        PolyReader
            The PolyReader instance with the file opened for reading.
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

        When progress tracking is disabled and the underlying handle is an
        ``io.TextIOBase``, this returns the handle's own C-level iterator directly.
        """
        self._ensure_open()
        if self.show_progress:
            return self._iter_progress()
        if isinstance(self._fh, io.TextIOBase):
            return iter(self._fh)
        return self._iter_decoded()

    def _iter_decoded(self):
        """Yield decoded text lines from a non-text handle (binary stream)."""
        for line in self._fh:
            if isinstance(line, bytes):
                yield line.decode('utf-8')
            else:
                yield line

    def _iter_progress(self):
        """Yield text lines while batching tqdm progress updates to 64 KiB."""
        try:
            fh = self._fh
            progress = self._progress
            is_text = isinstance(fh, io.TextIOBase)
            counting = self._counting_stream

            if progress is None:
                if is_text:
                    yield from fh
                else:
                    for line in fh:
                        yield line.decode('utf-8') if isinstance(line, bytes) else line
                return

            update = progress.update
            if counting is not None:
                if is_text:
                    for line in fh:
                        delta = counting.bytes_read - self._progress_raw_bytes
                        if delta > 0:
                            self._progress_raw_bytes += delta
                            self._progress_pending += delta
                            if self._progress_pending >= 65536:
                                update(self._progress_pending)
                                self._progress_pending = 0
                        yield line
                else:
                    for line in fh:
                        if isinstance(line, bytes):
                            line = line.decode('utf-8')
                        delta = counting.bytes_read - self._progress_raw_bytes
                        if delta > 0:
                            self._progress_raw_bytes += delta
                            self._progress_pending += delta
                            if self._progress_pending >= 65536:
                                update(self._progress_pending)
                                self._progress_pending = 0
                        yield line
            elif is_text:
                for line in fh:
                    self._progress_pending += len(line.encode('utf-8'))
                    if self._progress_pending >= 65536:
                        update(self._progress_pending)
                        self._progress_pending = 0
                    yield line
            else:
                for line in fh:
                    if isinstance(line, bytes):
                        line_bytes = len(line)
                        line = line.decode('utf-8')
                    else:
                        line_bytes = len(line.encode('utf-8'))
                    self._progress_pending += line_bytes
                    if self._progress_pending >= 65536:
                        update(self._progress_pending)
                        self._progress_pending = 0
                    yield line
        finally:
            self._flush_progress()

    def readline(self, size: int = -1) -> str:
        """Read and return one line from the stream, or up to size characters."""
        self._ensure_open()
        line = self._fh.readline() if (size is None or size < 0) else self._fh.readline(size)
        if isinstance(line, bytes):
            line = line.decode('utf-8')
        self._update_progress(line)
        return line

    def read(self, size: int = -1) -> str:
        """Read and return up to size characters, or the entire remaining stream if size < 0."""
        self._ensure_open()
        data = self._fh.read() if (size is None or size < 0) else self._fh.read(size)
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        self._update_progress(data)
        return data

    def readlines(self, hint: int = -1) -> list:
        """Read and return a list of lines from the stream."""
        self._ensure_open()
        lines = []
        total = 0
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
            total += len(line)
            if hint is not None and hint > 0 and total >= hint:
                break
        return lines

    def __next__(self) -> str:
        """
        Provide the next line in the file.

        This method is used to retrieve the next line when iterating over the file.

        Returns
        -------
        str
            The next line in the file.

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

    def _wrap_ftp(self, parsed: ParseResult):
        """
        Wrap self._fh to handle FTP connections and file operations.

        Parameters
        ----------
        parsed : ParseResult
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
        stream = self._make_counted_stream(adapted)

        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            self._fh = io.TextIOWrapper(
                dctx.stream_reader(stream, read_across_frames=True),
                encoding='utf-8',
            )
        elif norm.endswith('.bz2'):
            import bz2
            self._fh = bz2.open(stream, 'rt', encoding='utf-8')
        elif norm.endswith('.xz'):
            import lzma
            self._fh = lzma.open(stream, 'rt', encoding='utf-8')
        elif norm.endswith('.gz'):
            import gzip
            self._fh = gzip.open(stream, 'rt', encoding='utf-8')
        else:
            self._fh = io.TextIOWrapper(stream, encoding='utf-8')

    def _wrap_ssh(self, parsed: ParseResult):
        """
        Wrap self._fh to handle SSH/SFTP connections and file operations.

        This method establishes an SSH connection using paramiko, opens the specified file via SFTP,
        and wraps it with appropriate decompression handlers based on the file extension.

        Parameters
        ----------
        parsed : ParseResult
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
        try:
            self._ssh_client = client
            client.connect(parsed.hostname, **connect_kwargs)
            sftp = client.open_sftp()
            self._sftp_client = sftp
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

            prefetch_fn = getattr(sftp_file, 'prefetch', None)
            if prefetch_fn is not None and file_size is not None:
                try:
                    prefetch_fn(file_size, max_concurrent_requests=64)
                except TypeError:
                    prefetch_fn(file_size)
        except BaseException:
            self.close()
            raise

        stream = self._make_counted_stream(self._sftp_file)
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            self._fh = io.TextIOWrapper(
                dctx.stream_reader(stream, read_across_frames=True),
                encoding='utf-8',
            )
        elif norm.endswith('.bz2'):
            import bz2
            self._fh = bz2.open(stream, 'rt', encoding='utf-8')
        elif norm.endswith('.xz'):
            import lzma
            self._fh = lzma.open(stream, 'rt', encoding='utf-8')
        elif norm.endswith('.gz'):
            import gzip
            self._fh = gzip.open(stream, 'rt', encoding='utf-8')
        else:
            self._fh = io.TextIOWrapper(stream, encoding='utf-8')

    def _wrap_http(self, parsed: Optional[ParseResult] = None):
        """
        Wrap self._fh to handle HTTP/HTTPS connections and file operations.

        This method fetches the specified file via HTTP/HTTPS, wraps the response content
        with appropriate decompression handlers based on the file extension, and sets the file
        handle to the decompressed content. It also stores the file size in self._http_file_size.

        Parameters
        ----------
        parsed : ParseResult or None, optional
            The parsed URL (parsed from self.filename if omitted).

        Returns
        -------
        None
        """
        if parsed is None:
            from urllib.parse import urlparse
            parsed = urlparse(self.filename)
        norm = _normalized_path(self.filename, parsed)
        is_compressed = norm.endswith(('.zst', '.bz2', '.xz', '.gz'))
        get_kwargs = {'stream': True}
        if is_compressed:
            get_kwargs['headers'] = {'Accept-Encoding': 'identity'}
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
            import zstandard as zstd
            stream = self._make_counted_stream(response.raw)
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            self._fh = io.TextIOWrapper(
                dctx.stream_reader(stream, read_across_frames=True),
                encoding='utf-8',
            )
        elif norm.endswith('.bz2'):
            import bz2
            stream = self._make_counted_stream(response.raw)
            self._fh = bz2.open(stream, 'rt', encoding='utf-8')
        elif norm.endswith('.xz'):
            import lzma
            stream = self._make_counted_stream(response.raw)
            self._fh = lzma.open(stream, 'rt', encoding='utf-8')
        elif norm.endswith('.gz'):
            import gzip
            stream = self._make_counted_stream(response.raw)
            self._fh = gzip.open(stream, 'rt', encoding='utf-8')
        else:
            content_encoding = response.headers.get('Content-Encoding')
            if isinstance(content_encoding, str) and content_encoding.strip().lower() not in ('', 'identity'):
                self._http_file_size = None
            try:
                response.raw.decode_content = True
            except AttributeError:
                pass
            stream = self._make_counted_stream(_HttpRawStream(response.raw))
            self._fh = io.TextIOWrapper(stream, encoding='utf-8')

    def _wrap_local(self, parsed: Optional[ParseResult] = None):
        """
        Wrap self._fh to handle local file operations with optional decompression.

        This method opens the specified local file and wraps it with appropriate decompression
        handlers based on the file extension, setting the file handle to the decompressed content.

        Parameters
        ----------
        parsed : ParseResult or None, optional
            The parsed path/URL (optional; omitted for plain local paths).

        Returns
        -------
        None
        """
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            self._raw_fh = open(self.filename, 'rb')
            stream = self._make_counted_stream(self._raw_fh)
            self._fh = io.TextIOWrapper(
                dctx.stream_reader(stream, read_across_frames=True),
                encoding='utf-8',
            )
        elif norm.endswith('.bz2'):
            import bz2
            if self.show_progress:
                self._raw_fh = open(self.filename, 'rb')
                self._fh = bz2.open(self._make_counted_stream(self._raw_fh), 'rt', encoding='utf-8')
            else:
                self._fh = bz2.open(self.filename, 'rt', encoding='utf-8')
        elif norm.endswith('.xz'):
            import lzma
            if self.show_progress:
                self._raw_fh = open(self.filename, 'rb')
                self._fh = lzma.open(self._make_counted_stream(self._raw_fh), 'rt', encoding='utf-8')
            else:
                self._fh = lzma.open(self.filename, 'rt', encoding='utf-8')
        elif norm.endswith('.gz'):
            import gzip
            if self.show_progress:
                self._raw_fh = open(self.filename, 'rb')
                self._fh = gzip.open(self._make_counted_stream(self._raw_fh), 'rt', encoding='utf-8')
            else:
                self._fh = gzip.open(self.filename, 'rt', encoding='utf-8')
        else:
            self._fh = open(self.filename, 'r', encoding='utf-8')

    def open(self) -> 'PolyReader':
        """
        Open the file for reading, decompressing it on the fly if necessary.

        This method determines the file type and source, and opens the file
        with the appropriate handler and decompression method.

        Returns
        -------
        PolyReader
            The PolyReader instance with the file opened for reading.
        """
        if self._fh is not None:
            return self

        self.filename, parsed, scheme = _resolve_target(self.filename)

        if _normalized_path(self.filename, parsed).endswith(UNSUPPORTED_ARCHIVES):
            raise UnsupportedArchiveError(
                f"polyopen does not support reading archive formats directly: {_redact_url(self.filename)}. "
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
                self._progress_raw_bytes = initial_bytes
                self._progress_pending = 0
                total = file_size if (file_size is not None and file_size > 0) else None
                self._progress = tqdm(total=total, initial=initial_bytes, unit='B', unit_scale=True)
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
        except Exception as err:
            flush_err = err
        finally:
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
            self._progress_raw_bytes = 0
            self._progress_pending = 0

        try:
            _close_handles(fh, raw_fh, sftp_file, sftp_client, ssh_client, ftp, response, progress)
        except Exception:
            if flush_err is not None:
                raise flush_err
            raise
        if flush_err is not None:
            raise flush_err


class PolyWriter:
    """
    UTF-8 text stream writer supporting `.gz`, `.bz2`, `.xz`, and `.zst` compression
    over local files (`file://` or plain paths), FTP (write only), and SSH/SFTP (write/append),
    with automatic `.NNN.bu` backup rotation on overwrite.

    Attributes
    ----------
    filename : str
        Normalized path or URL to write.
    timeout : float or None
        Optional network timeout in seconds.
    encoding : str
        Always `'utf-8'`.
    closed : bool
        True when no underlying stream handle is open.

    Methods
    -------
    open(append=False, backup=True, compresslevel=None) -> PolyWriter
        Open the stream for writing or appending.
    write(data) -> int, writelines(lines) -> None, flush() -> None
        Standard text stream write operations.
    readable() -> bool, writable() -> bool, seekable() -> bool
        Standard file-like capability predicates.
    close() -> None
        Idempotently close all acquired stream and transport handles.
    """

    encoding = 'utf-8'

    @property
    def closed(self) -> bool:
        """Return True if the writer is closed."""
        return self._fh is None

    def readable(self) -> bool:
        """Return False (PolyWriter is a write-only stream)."""
        return False

    def writable(self) -> bool:
        """Return True (PolyWriter is a write-only stream)."""
        return True

    def seekable(self) -> bool:
        """Return False (PolyWriter does not support random-access seeking)."""
        return False

    def __repr__(self) -> str:
        state = "closed" if self.closed else "open"
        return f"PolyWriter({_redact_url(self.filename)!r}, state={state!r})"

    def __init__(
        self,
        filename: Union[str, os.PathLike],
        show_progress: bool = False,
        timeout: Optional[float] = None,
    ):
        """
        Initialize PolyWriter with a filename.

        Parameters
        ----------
        filename : str or os.PathLike
            The name of the file to write to.
        show_progress : bool, optional
            Progress tracking on writers is not implemented; passing True raises NotImplementedError.
        timeout : float or None, optional
            Optional network timeout in seconds for FTP and SSH/SFTP connections.
        """
        if show_progress:
            raise NotImplementedError('Progress tracking is currently isolated to reader paths. Write implementations are deferred.')
        self.filename = os.fsdecode(filename)
        self.show_progress = show_progress
        self.timeout = _validate_timeout(timeout)
        self._fh = None
        self._raw_fh = None
        self._ftp = None
        self._ssh_client = None
        self._sftp_client = None
        self._sftp_file = None

    def __enter__(self) -> 'PolyWriter':
        """
        Open the file for writing when entering the context.

        This method is called when the context is entered using the `with` statement.

        Returns
        -------
        PolyWriter
            The PolyWriter instance with the file opened for writing.
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

    def _wrap_ftp(self, parsed: ParseResult, append: bool, backup: bool, compresslevel: Optional[int] = None):
        """
        Wrap self._fh to handle FTP connections and file operations.

        This method establishes an FTP connection using ftputil, opens the specified file for writing,
        and wraps it with appropriate compression handlers based on the file extension.

        Parameters
        ----------
        parsed : ParseResult
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
                "Protocol 'ftp' does not support append mode in polyopen (ftputil does not support FTP append)."
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
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            cctx = zstd.ZstdCompressor() if compresslevel is None else zstd.ZstdCompressor(level=compresslevel)
            self._fh = io.TextIOWrapper(cctx.stream_writer(adapted), encoding='utf-8')
        elif norm.endswith('.bz2'):
            import bz2
            bz2_kwargs = {'mode': 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                bz2_kwargs['compresslevel'] = compresslevel
            self._fh = bz2.open(adapted, **bz2_kwargs)
        elif norm.endswith('.xz'):
            import lzma
            lzma_kwargs = {'mode': 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                lzma_kwargs['preset'] = compresslevel
            self._fh = lzma.open(adapted, **lzma_kwargs)
        elif norm.endswith('.gz'):
            import gzip
            gz_kwargs = {'mode': 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                gz_kwargs['compresslevel'] = compresslevel
            self._fh = gzip.open(adapted, **gz_kwargs)
        else:
            self._fh = io.TextIOWrapper(adapted, encoding='utf-8')

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
            sftp.stat(path)
        except FileNotFoundError:
            return

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

    def _wrap_ssh(self, parsed: ParseResult, append: bool, backup: bool, compresslevel: Optional[int] = None):
        """
        Wrap self._fh to handle SSH/SFTP connections and file operations.

        This method establishes an SSH connection using paramiko, opens the specified file via SFTP,
        and wraps it with appropriate compression handlers based on the file extension.

        Parameters
        ----------
        parsed : ParseResult
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

        try:
            self._ssh_client = client
            client.connect(parsed.hostname, **connect_kwargs)
            sftp = client.open_sftp()
            self._sftp_client = sftp

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

        if norm.endswith('.zst'):
            import zstandard as zstd
            cctx = zstd.ZstdCompressor() if compresslevel is None else zstd.ZstdCompressor(level=compresslevel)
            self._fh = io.TextIOWrapper(cctx.stream_writer(self._sftp_file), encoding='utf-8')
        elif norm.endswith('.bz2'):
            import bz2
            bz2_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                bz2_kwargs['compresslevel'] = compresslevel
            self._fh = bz2.open(self._sftp_file, **bz2_kwargs)
        elif norm.endswith('.xz'):
            import lzma
            lzma_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                lzma_kwargs['preset'] = compresslevel
            self._fh = lzma.open(self._sftp_file, **lzma_kwargs)
        elif norm.endswith('.gz'):
            import gzip
            gz_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                gz_kwargs['compresslevel'] = compresslevel
            self._fh = gzip.open(self._sftp_file, **gz_kwargs)
        else:
            adapted = _BinaryStreamAdapter(self._sftp_file, readable=False, writable=True)
            self._fh = io.TextIOWrapper(adapted, encoding='utf-8')

    def _backup_local_file(self):
        """Atomically rotate an existing local file to the next free .bu slot."""
        src = self.filename
        if not os.path.lexists(src):
            return
        if os.path.isdir(src) and not os.path.islink(src):
            raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), src)

        def _try_move(dst: str) -> str:
            """Return 'moved', 'exists', or 'gone'."""
            try:
                os.link(src, dst)
            except FileExistsError:
                return 'exists'
            except FileNotFoundError:
                return 'gone'
            except (NotImplementedError, AttributeError):
                if os.path.lexists(dst):
                    return 'exists'
                os.rename(src, dst)
                return 'moved'
            except OSError as exc:
                if exc.errno in _LINK_FALLBACK_ERRNOS:
                    if os.path.isdir(src) and not os.path.islink(src):
                        raise IsADirectoryError(errno.EISDIR, os.strerror(errno.EISDIR), src) from exc
                    if os.path.lexists(dst):
                        return 'exists'
                    os.rename(src, dst)
                    return 'moved'
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

    def _wrap_local(self, append: bool, backup: bool, parsed: Optional[ParseResult] = None, compresslevel: Optional[int] = None):
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
        parsed : ParseResult or None, optional
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
        if norm.endswith('.zst'):
            import zstandard as zstd
            cctx = zstd.ZstdCompressor() if compresslevel is None else zstd.ZstdCompressor(level=compresslevel)
            self._raw_fh = open(self.filename, mode)
            self._fh = io.TextIOWrapper(cctx.stream_writer(self._raw_fh), encoding='utf-8')
        elif norm.endswith('.bz2'):
            import bz2
            bz2_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                bz2_kwargs['compresslevel'] = compresslevel
            self._fh = bz2.open(self.filename, **bz2_kwargs)
        elif norm.endswith('.xz'):
            import lzma
            lzma_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                lzma_kwargs['preset'] = compresslevel
            self._fh = lzma.open(self.filename, **lzma_kwargs)
        elif norm.endswith('.gz'):
            import gzip
            gz_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                gz_kwargs['compresslevel'] = compresslevel
            self._fh = gzip.open(self.filename, **gz_kwargs)
        else:
            self._fh = open(self.filename, 'a' if append else 'w', encoding='utf-8')

    def open(self, append: bool = False, backup: bool = True, compresslevel: Optional[int] = None) -> 'PolyWriter':
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
        PolyWriter
            The PolyWriter instance with the file opened for writing.

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
                f"polyopen does not support writing or appending to archive formats safely: {_redact_url(self.filename)}. "
                f"Use Python's zipfile or tarfile module instead."
            )

        if append and backup:
            raise ValueError("Cannot have both append and backup set to True.")

        if scheme in ('http', 'https'):
            raise ReadOnlyProtocolError(f"Protocol '{parsed.scheme}' does not support write/append operations in polyopen.")

        _validate_write_options(norm, compresslevel)

        try:
            if scheme == 'ftp':
                self._wrap_ftp(parsed, append, backup, compresslevel=compresslevel)
            elif scheme in ('sftp', 'ssh'):
                self._wrap_ssh(parsed, append, backup, compresslevel=compresslevel)
            else:
                self._wrap_local(append, backup, parsed, compresslevel=compresslevel)
        except BaseException:
            self.close()
            raise

        if type(self).write is PolyWriter.write:
            self.write = self._fh.write

        return self

    def write(self, data):
        """
        Write data to the file.

        Parameters
        ----------
        data : str
            The data to write to the file.

        Returns
        -------
        int
            The number of characters written.
        """
        if self._fh is None:
            raise ValueError("I/O operation on closed file.")
        return self._fh.write(data)

    def writelines(self, lines) -> None:
        """Write an iterable of strings to the file."""
        if self._fh is None:
            raise ValueError("I/O operation on closed file.")
        self._fh.writelines(lines)

    def flush(self) -> None:
        """Flush buffered write data to the underlying stream."""
        if self._fh is None:
            raise ValueError("I/O operation on closed file.")
        self._fh.flush()

    def close(self):
        """
        Close the file.

        This method closes the file handle and any associated resources idempotently,
        resetting all handle attributes even if one handle fails to close.

        Returns
        -------
        None
        """
        self.__dict__.pop('write', None)
        fh = self._fh
        raw_fh = self._raw_fh
        sftp_file = self._sftp_file
        sftp_client = self._sftp_client
        ssh_client = self._ssh_client
        ftp = self._ftp

        self._fh = None
        self._raw_fh = None
        self._sftp_file = None
        self._sftp_client = None
        self._ssh_client = None
        self._ftp = None

        _close_handles(fh, raw_fh, sftp_file, sftp_client, ssh_client, ftp)


def polyopen(
    filename: Union[str, os.PathLike],
    mode: str = 'r',
    backup: Optional[bool] = None,
    show_progress: bool = False,
    compresslevel: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Union[PolyReader, PolyWriter]:
    """
    Open a local or remote UTF-8 text stream with transparent compression.

    Supports plain text, `.gz`, `.bz2`, `.xz`, and `.zst` over local paths (`file://` or implicit),
    `http://`/`https://` (read-only), `ftp://` (read/write), and `ssh://`/`sftp://` (read/write/append).

    Note: All modes operate on UTF-8 text (`str`). The `'rb'`, `'wb'`, and `'ab'` modes are accepted
    as text-mode aliases for compatibility and do NOT return or accept raw `bytes`.

    Parameters
    ----------
    filename : str or os.PathLike
        Path or URL to open (extension matching is case-insensitive and ignores URL query strings).
    mode : str, optional
        One of `'r'`, `'rt'`, `'rb'` (read, default `'r'`), `'w'`, `'wt'`, `'wb'` (write/truncate),
        or `'a'`, `'at'`, `'ab'` (append). All modes decode/encode UTF-8 text.
    backup : bool or None, optional
        Rotate an existing target to `.001.bu`..`.999.bu` (or nanosecond timestamp fallback) before
        overwriting. Defaults to `True` in write modes and `False` in append modes; passing
        `backup=True` with an append mode raises `ValueError`.
    show_progress : bool, optional
        Display a `tqdm` progress bar tracking bytes read (read modes only; passing `True` in
        write/append modes raises `NotImplementedError`). Default is `False`.
    compresslevel : int or None, optional
        Optional compression level/preset forwarded to `.zst` (default 3), `.xz` (default 6),
        `.bz2` (default 9), and `.gz` (default 9) writers; ignored in read modes.
    timeout : float or None, optional
        Optional network timeout in seconds for HTTP, FTP, and SSH/SFTP connections.

    Returns
    -------
    PolyReader or PolyWriter
        An opened reader or writer context manager.

    Raises
    ------
    UnsupportedArchiveError
        If the target matches a container archive extension (e.g., `.zip`, `.tar`, `.tar.gz`, `.tar.xz`).
    ReadOnlyProtocolError
        If writing or appending to `http://` or `https://`.
    UnsupportedProtocolError
        If invoking an unsupported scheme (e.g., `s3://`, `gs://`, `az://`, `hdfs://`) or FTP append.
    ValueError
        If `mode` is unsupported, `backup=True` is combined with append, or `timeout` is non-positive.
    NotImplementedError
        If `show_progress=True` is passed with write or append modes.

    Examples
    --------
    >>> with polyopen("data.gz", "w", compresslevel=6) as f:
    ...     f.write("hello\\n")
    >>> with polyopen("data.gz", "r") as f:
    ...     print(f.read())
    """
    filename = os.fsdecode(filename)
    if show_progress and mode in ('w', 'wt', 'wb', 'a', 'at', 'ab'):
        raise NotImplementedError('Progress tracking is currently isolated to reader paths. Write implementations are deferred.')
    if mode in ('r', 'rt', 'rb'):
        return PolyReader(filename, show_progress=show_progress, timeout=timeout).open()
    elif mode in ('w', 'wt', 'wb'):
        effective_backup = True if backup is None else backup
        return PolyWriter(filename, show_progress=show_progress, timeout=timeout).open(
            append=False,
            backup=effective_backup,
            compresslevel=compresslevel,
        )
    elif mode in ('a', 'at', 'ab'):
        effective_backup = False if backup is None else backup
        return PolyWriter(filename, show_progress=show_progress, timeout=timeout).open(
            append=True,
            backup=effective_backup,
            compresslevel=compresslevel,
        )
    else:
        raise ValueError(
            f"Unsupported mode: '{mode}'. Supported modes are 'r', 'rt', 'rb', 'w', 'wt', 'wb', 'a', 'at', 'ab' (all UTF-8 text)."
        )


def main():
    """
    CLI entry point for `polyopen` (`python -m polyopen` or `polyopen`).

    Supports streaming reads (`--read`) with line/byte throughput reporting and single-pass
    multi-target writes (`--write`) across plain text, `.gz`, `.bz2`, `.xz`, and `.zst`
    files over local paths, HTTP/HTTPS, FTP, and SSH/SFTP.
    """
    def format_rate(rate: float, unit: str = "") -> str:
        """Format rate with standard units."""
        if not rate:
            return f"0{unit}/s"
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
        prog="polyopen",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Read or broadcast UTF-8 text streams across plain, .gz, .bz2, .xz, and .zst files "
            "over local paths, HTTP/HTTPS, FTP, and SSH/SFTP."
        ),
        epilog=(
            "Examples:\n"
            "  python -m polyopen --read data.txt.gz https://example.com/log.zst\n"
            "  python -m polyopen --write source.gz copy.txt backup.bz2\n"
            "  python -m polyopen --write -i source.gz copy.txt sftp://user@host/dest.zst"
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

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--write',
        '-w',
        action='store_true',
        help='Read one source stream and broadcast every line to one or more destinations.',
    )
    group.add_argument(
        '--read',
        '-r',
        action='store_true',
        help='Read one or more files sequentially and report line/byte throughput.',
    )

    args = parser.parse_args()

    if args.read:
        if args.input_file is not None:
            parser.error("--input-file/-i is only valid with --write")
        if len(args.files) < 1:
            parser.error("Read mode requires at least one file (e.g., 'python -m polyopen -r path/to/file.gz')")
        for input_file in args.files:
            print(f"TESTING reading {_redact_url(input_file)}")
            with PolyReader(input_file) as reader:
                line_count = 0
                t0 = time.monotonic()
                bytes_count = 0
                for line in reader:
                    line_count += 1
                    bytes_count += len(line.encode('utf-8'))
                pinfo("FINISHED Reading.", _redact_url(reader.filename), line_count, bytes_count, time.monotonic() - t0)
    else:
        if args.input_file is not None:
            if len(args.files) < 1:
                parser.error("Write mode with --input-file/-i requires at least one destination file")
            source_file = args.input_file
            output_files = args.files
        else:
            if len(args.files) < 2:
                parser.error("Write mode requires at least one source file and one destination file (e.g., 'python -m polyopen -w src dest')")
            source_file = args.files[0]
            output_files = args.files[1:]

        # Pre-validate destination schemes and archive guards before rotating/truncating earlier outputs.
        for output_file in output_files:
            resolved_out, parsed_out, scheme_out = _resolve_target(output_file)
            if _normalized_path(resolved_out, parsed_out).endswith(UNSUPPORTED_ARCHIVES):
                raise UnsupportedArchiveError(
                    f"polyopen does not support writing or appending to archive formats safely: {_redact_url(output_file)}. "
                    f"Use Python's zipfile or tarfile module instead."
                )
            if scheme_out in ('http', 'https'):
                raise ReadOnlyProtocolError(f"Protocol '{parsed_out.scheme}' does not support write/append operations in polyopen.")

        import contextlib
        print(f"Will Read from {_redact_url(source_file)}")
        line_count = 0
        bytes_count = 0
        dest_names = []
        t0 = time.monotonic()
        with contextlib.ExitStack() as stack:
            reader = stack.enter_context(PolyReader(source_file))
            writers = []
            for output_file in output_files:
                writer = stack.enter_context(PolyWriter(output_file))
                print(f"Writing {_redact_url(writer.filename)}")
                writers.append(writer)
                dest_names.append(_redact_url(writer.filename))
            write_fns = [w.write for w in writers]
            for line in reader:
                line_count += 1
                bytes_count += len(line.encode('utf-8'))
                for write_fn in write_fns:
                    write_fn(line)
        elapsed = time.monotonic() - t0
        for filename in dest_names:
            pinfo("FINISHED Writing.", filename, line_count, bytes_count, elapsed)


if __name__ == "__main__":
    main()

