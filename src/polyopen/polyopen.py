#!/usr/bin/env python3
"""
polyfile - A Python module for versatile file handling.

This module provides utility classes and functions for reading and writing files of various formats
(plain text, gzip, bz2, zst) and from various sources (local files, HTTP/HTTPS URLs, FTP, SSH, and SFTP servers).

Classes
-------
- `FTPSessionWrapper`: A wrapper around `ftplib.FTP` for more flexible FTP session handling.
- `PolyReader`: A class designed for reading files line-by-line with support for various
   compression formats and remote sources.
- `PolyWriter`: A class designed for writing files line-by-line with support for various
   compression formats and remote destinations.

Usage
-----
To read from a local gzipped file:
    >>> with PolyReader("path/to/localfile.gz") as reader:
    >>>     for line in reader:
    >>>         process(line)

To read from a remote HTTP source:
    >>> with PolyReader("http://example.com/data.txt") as reader:
    >>>     for line in reader:
    >>>         process(line)

To read from a remote SSH server with zstd compression:
    >>> with PolyReader("ssh://username:password@hostname/path/to/remote.zst") as reader:
    >>>     for line in reader:
    >>>         process(line)

To write to a local bz2 compressed file:
    >>> with PolyWriter("path/to/outputfile.bz2") as writer:
    >>>     writer.write("Some data to write")

To write to a remote SFTP server:
    >>> with PolyWriter("sftp://username:password@hostname/path/to/outputfile.txt") as writer:
    >>>     writer.write("Some data to write")

To write to a local file with zstd compression:
    >>> with PolyWriter("path/to/outputfile.zst") as writer:
    >>>     writer.write("Some data to write")

Dependencies
------------
- `paramiko`: For SSH/SFTP support.
- `zstandard`: For zst file support.
- `requests`: For HTTP/HTTPS support.
- `ftputil`: For enhanced FTP utilities.
- `tqdm`: For progress bars.

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
    """Base exception for polyopen."""

class UnsupportedArchiveError(PolyopenError):
    """Exception raised when an explicitly unsupported archive format is passed."""

class ReadOnlyProtocolError(PolyopenError):
    """Exception raised when attempting to write/append to a read-only stream."""

class UnsupportedProtocolError(PolyopenError):
    """Exception raised when an explicitly unsupported protocol is invoked."""

UNSUPPORTED_ARCHIVES = ('.zip', '.tar', '.tgz', '.rar', '.7z', '.gz.tar', '.tar.gz', '.tar.bz2', '.tar.zst')

import errno
import io
import os
import time
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from urllib.parse import ParseResult

_NETWORK_SCHEMES = frozenset(('http', 'https', 'ftp', 'ssh', 'sftp', 's3', 'gs', 'az', 'hdfs'))
_LINK_FALLBACK_ERRNOS = frozenset(
    code
    for name in ('EOPNOTSUPP', 'ENOTSUP', 'ENOSYS', 'EPERM', 'EXDEV', 'EMLINK')
    for code in (getattr(errno, name, None),)
    if code is not None
)


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
        if num < 1024.0:
            return f"{num:3.1f}{unit}"
        num /= 1024.0
    return f"{num:3.1f}YB"


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
        close_fn = getattr(handle, 'close', None)
        if close_fn is None:
            continue
        try:
            close_fn()
        except Exception as err:
            if first_err is None:
                first_err = err
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


_http_session_local = None


def _get_http_session():
    """Return a thread-local requests.Session with cookie persistence disabled."""
    global _http_session_local
    local = _http_session_local
    if local is None:
        import threading
        local = threading.local()
        _http_session_local = local
    session = getattr(local, 'session', None)
    if session is None:
        import http.cookiejar
        import requests
        session = requests.Session()
        session.cookies.set_policy(http.cookiejar.DefaultCookiePolicy(allowed_domains=[]))
        local.session = session
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

            def __init__(self, host: str, userid: str, password: str, port: Optional[int] = 21):
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
                """
                if port is None:
                    port = 21
                # Initialize the parent ftplib.FTP class
                ftplib.FTP.__init__(self)
                # Connect to the specified FTP server on the given port
                self.connect(host, port)
                # Log in to the FTP server with the provided username and password
                self.login(userid, password)

        _FTP_SESSION_WRAPPER_CLASS = FTPSessionWrapper
    return _FTP_SESSION_WRAPPER_CLASS


def __getattr__(name: str):
    """Lazily resolve optional attributes such as FTPSessionWrapper (PEP 562)."""
    if name == 'FTPSessionWrapper':
        return _ftp_session_wrapper_class()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class PolyReader:
    """
    A class used to read files line by line, with support for gzip, bz2, xz, and zst compression formats,
    as well as remote files over HTTP, HTTPS, FTP, SSH, and SFTP.

    Attributes
    ----------
    filename : str
        The name of the file to read.
    show_progress : bool
        Whether to show a progress bar.

    Methods
    -------
    open() -> 'PolyReader':
        Opens the file for reading, decompressing it on the fly if necessary.
    close():
        Closes the file.
    """

    def __init__(self, filename: Union[str, os.PathLike], show_progress: bool = False):
        """
        Initialize PolyReader with a filename.

        Parameters
        ----------
        filename : str or os.PathLike
            The name of the file to read.
        show_progress : bool
            Whether to show a progress bar.
        """
        self.filename = os.fsdecode(filename)
        self.show_progress = show_progress
        self._fh = None
        self._raw_fh = None
        self._ssh_client = None
        self._sftp_client = None
        self._sftp_file = None
        self._sftp_file_size = None
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
                pending = self._progress_pending
                raw_bytes = self._progress_raw_bytes
                if is_text:
                    for line in fh:
                        delta = counting.bytes_read - raw_bytes
                        if delta > 0:
                            raw_bytes += delta
                            pending += delta
                            if pending >= 65536:
                                update(pending)
                                pending = 0
                            self._progress_raw_bytes = raw_bytes
                            self._progress_pending = pending
                        yield line
                else:
                    for line in fh:
                        if isinstance(line, bytes):
                            line = line.decode('utf-8')
                        delta = counting.bytes_read - raw_bytes
                        if delta > 0:
                            raw_bytes += delta
                            pending += delta
                            if pending >= 65536:
                                update(pending)
                                pending = 0
                            self._progress_raw_bytes = raw_bytes
                            self._progress_pending = pending
                        yield line
            elif is_text:
                line_len = len
                pending = self._progress_pending
                for line in fh:
                    pending += line_len(line)
                    if pending >= 65536:
                        update(pending)
                        pending = 0
                    self._progress_pending = pending
                    yield line
            else:
                line_len = len
                pending = self._progress_pending
                for line in fh:
                    if isinstance(line, bytes):
                        line = line.decode('utf-8')
                    pending += line_len(line)
                    if pending >= 65536:
                        update(pending)
                        pending = 0
                    self._progress_pending = pending
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
        self._ftp = ftputil.FTPHost(
            parsed.hostname,
            parsed.username,
            parsed.password,
            port=port,
            session_factory=_ftp_session_wrapper_class(),
        )
        self._ftp_path = parsed.path
        raw_fh = self._ftp.open(parsed.path, 'rb')
        self._raw_fh = raw_fh
        stream = self._make_counted_stream(self._raw_fh)

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
        try:
            client.connect(
                parsed.hostname,
                port=port,
                username=parsed.username,
                password=parsed.password,
            )
            self._ssh_client = client
            sftp = client.open_sftp()
            self._sftp_client = sftp
            sftp_file = sftp.file(parsed.path, 'rb')
            self._sftp_file = sftp_file

            file_size = None
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
            if prefetch_fn is not None:
                try:
                    prefetch_fn(file_size, max_concurrent_requests=64)
                except TypeError:
                    prefetch_fn(file_size)
        except Exception:
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
        response = _get_http_session().get(self.filename, stream=True)
        self._response = response
        try:
            response.raise_for_status()
        except Exception:
            self.close()
            raise

        content_length = response.headers.get('Content-Length')
        try:
            self._http_file_size = int(content_length) if content_length is not None else None
        except (TypeError, ValueError):
            self._http_file_size = None

        norm = _normalized_path(self.filename, parsed)
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

        if ':' in self.filename:
            from urllib.parse import urlparse
            parsed = urlparse(self.filename)
            if parsed.scheme == 'file':
                from urllib.request import url2pathname
                self.filename = url2pathname(parsed.path)
                parsed = urlparse(self.filename)
            scheme = parsed.scheme.lower()
        else:
            parsed = None
            scheme = ''

        if _normalized_path(self.filename, parsed).endswith(UNSUPPORTED_ARCHIVES):
            raise UnsupportedArchiveError(
                f"polyopen does not support reading archive formats directly: {self.filename}. "
                f"Use the standard library (e.g. zipfile) instead."
            )

        try:
            if scheme == 'ftp':
                self._wrap_ftp(parsed)
            elif scheme in ('sftp', 'ssh'):
                self._wrap_ssh(parsed)
            elif scheme in ('http', 'https'):
                self._wrap_http(parsed)
            elif scheme in ('s3', 'gs', 'az', 'hdfs'):
                raise UnsupportedProtocolError(
                    f"polyopen does not support cloud object stores like '{parsed.scheme}://'. "
                    f"We heavily recommend using the 'smart_open' library or dedicated SDKs (like boto3) for this architecture."
                )
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
        except Exception:
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
            return self._sftp_file.stat().st_size
        if self._ftp is not None:
            return self._ftp.path.getsize(self._ftp_path)
        if self._response is not None:
            return self._http_file_size
        return os.path.getsize(self.filename)

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
    A class used to write files line by line, with support for gzip, bz2, xz, and zst compression formats,
    as well as remote files over FTP, SSH, and SFTP.

    Attributes
    ----------
    filename : str
        The name of the file to write to.

    Methods
    -------
    open(append: bool = False, backup: bool = True) -> 'PolyWriter':
        Opens the file for writing, compressing it on the fly if necessary.
    write(data):
        Writes data to the file.
    close():
        Closes the file.
    """

    def __init__(self, filename: Union[str, os.PathLike], show_progress: bool = False):
        """
        Initialize PolyWriter with a filename.

        Parameters
        ----------
        filename : str or os.PathLike
            The name of the file to write to.
        show_progress : bool, optional
            Progress tracking on writers is not implemented; passing True raises NotImplementedError.
        """
        if show_progress:
            raise NotImplementedError('Progress tracking is currently isolated to reader paths. Write implementations are deferred.')
        self.filename = os.fsdecode(filename)
        self.show_progress = show_progress
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
        import ftputil
        port = 21 if parsed.port is None else parsed.port
        self._ftp = ftputil.FTPHost(
            parsed.hostname,
            parsed.username,
            parsed.password,
            port=port,
            session_factory=_ftp_session_wrapper_class(),
        )
        self.remote_filename = parsed.path

        if backup and self._ftp.path.exists(parsed.path):
            first_candidate = f"{parsed.path}.001.bu"
            if not self._ftp.path.exists(first_candidate):
                backup_filename = first_candidate
            else:
                import posixpath
                parent = posixpath.dirname(parsed.path) or "."
                basename = posixpath.basename(parsed.path)
                entries = self._ftp.listdir(parent)
                backup_filename = _select_backup_filename(
                    parsed.path, basename, entries, reserved_slots=(1,)
                )
            self._ftp.rename(parsed.path, backup_filename)

        mode = 'ab' if append else 'wb'
        raw_fh = self._ftp.open(parsed.path, mode)
        self._raw_fh = raw_fh
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            cctx = zstd.ZstdCompressor() if compresslevel is None else zstd.ZstdCompressor(level=compresslevel)
            self._fh = io.TextIOWrapper(cctx.stream_writer(self._raw_fh), encoding='utf-8')
        elif norm.endswith('.bz2'):
            import bz2
            bz2_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                bz2_kwargs['compresslevel'] = compresslevel
            self._fh = bz2.open(self._raw_fh, **bz2_kwargs)
        elif norm.endswith('.xz'):
            import lzma
            lzma_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                lzma_kwargs['preset'] = compresslevel
            self._fh = lzma.open(self._raw_fh, **lzma_kwargs)
        elif norm.endswith('.gz'):
            import gzip
            gz_kwargs = {'mode': 'at' if append else 'wt', 'encoding': 'utf-8'}
            if compresslevel is not None:
                gz_kwargs['compresslevel'] = compresslevel
            self._fh = gzip.open(self._raw_fh, **gz_kwargs)
        else:
            self._fh = io.TextIOWrapper(self._raw_fh, encoding='utf-8')

    def _ssh_backup_if(self, sftp, path):
        """
        Backup the specified file on the SSH/SFTP server if it exists.

        Checks `<path>.001.bu` first in O(1); if it already exists, lists the parent
        directory once and selects the lowest free 001..999 slot (or collision-safe
        timestamp fallback when all 999 slots are occupied).

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
            sftp.rename(path, first_candidate)
            return

        import posixpath
        parent = posixpath.dirname(path) or "."
        basename = posixpath.basename(path)
        entries = sftp.listdir(parent)
        backup_filename = _select_backup_filename(
            path, basename, entries, reserved_slots=(1,)
        )
        sftp.rename(path, backup_filename)

    def _wrap_ssh(self, parsed: ParseResult, append: bool, backup: bool, compresslevel: Optional[int] = None):  # noqa: C901
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
        norm = _normalized_path(self.filename, parsed)
        is_compressed = norm.endswith(('.zst', '.bz2', '.xz', '.gz'))
        sftp_mode = ('ab' if append else 'wb') if is_compressed else ('a' if append else 'w')

        try:
            client.connect(
                parsed.hostname,
                port=port,
                username=parsed.username,
                password=parsed.password,
            )
            self._ssh_client = client
            sftp = client.open_sftp()
            self._sftp_client = sftp

            if backup:
                self._ssh_backup_if(sftp, parsed.path)

            sftp_file = sftp.file(parsed.path, sftp_mode)
            self._sftp_file = sftp_file

            set_pipelined_fn = getattr(sftp_file, 'set_pipelined', None)
            if set_pipelined_fn is not None:
                set_pipelined_fn(True)
        except Exception:
            self.close()
            raise

        self._fh = sftp_file
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

    def _backup_local_file(self):
        """Atomically rotate an existing local file to the next free .bu slot."""
        src = self.filename
        if not os.path.exists(src):
            return

        def _try_move(dst: str) -> str:
            """Return 'moved', 'exists', or 'gone'."""
            try:
                os.link(src, dst)
            except FileExistsError:
                return 'exists'
            except FileNotFoundError:
                return 'gone'
            except (NotImplementedError, AttributeError):
                os.rename(src, dst)
                return 'moved'
            except OSError as exc:
                if exc.errno in _LINK_FALLBACK_ERRNOS:
                    os.rename(src, dst)
                    return 'moved'
                raise
            try:
                os.unlink(src)
            except FileNotFoundError:
                pass
            return 'moved'

        first_candidate = f"{src}.001.bu"
        if not os.path.exists(first_candidate):
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

        if ':' in self.filename:
            from urllib.parse import urlparse
            parsed = urlparse(self.filename)
            if parsed.scheme == 'file':
                from urllib.request import url2pathname
                self.filename = url2pathname(parsed.path)
                parsed = urlparse(self.filename)
            scheme = parsed.scheme.lower()
        else:
            parsed = None
            scheme = ''

        if _normalized_path(self.filename, parsed).endswith(UNSUPPORTED_ARCHIVES):
            raise UnsupportedArchiveError(
                f"polyopen does not support writing or appending to archive formats safely: {self.filename}. "
                f"Use the standard library (e.g. zipfile) to mutate archives."
            )

        if append and backup:
            raise ValueError("Cannot have both append and backup set to True.")

        if scheme in ('http', 'https'):
            raise ReadOnlyProtocolError(f"Protocol '{parsed.scheme}' does not support write/append operations in polyopen.")

        if scheme in ('s3', 'gs', 'az', 'hdfs'):
            raise UnsupportedProtocolError(
                f"polyopen does not support explicitly pushing to cloud object stores like '{parsed.scheme}://'. "
                f"We heavily recommend using the 'smart_open' library or dedicated SDKs (like boto3) for unified cloud I/O."
            )

        try:
            if scheme == 'ftp':
                self._wrap_ftp(parsed, append, backup, compresslevel=compresslevel)
            elif scheme in ('sftp', 'ssh'):
                self._wrap_ssh(parsed, append, backup, compresslevel=compresslevel)
            else:
                self._wrap_local(append, backup, parsed, compresslevel=compresslevel)
        except Exception:
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
):
    """
    Open a file for reading or writing, determining the appropriate stream and compression 
    automatically based on the file extension and protocol scheme.

    This function mimics Python's built-in `open()`, returning a `PolyReader` or `PolyWriter`
    context manager.
    
    Parameters
    ----------
    filename : str or os.PathLike
        The path or URL to open.
    mode : str, optional
        The mode in which the file is opened. 'r' for reading (default), 'w' for writing, 
        and 'a' for appending.
    backup : bool or None, optional
        If True, creates a backup of the existing file before overwriting.
        Defaults to True in write modes and False in append modes; passing backup=True
        with an append mode raises ValueError.
    show_progress : bool, optional
        If True, displays a progress bar (read mode only). Default is False.
    compresslevel : int or None, optional
        Optional compression level/preset forwarded to write/append modes (ignored in read
        modes). When None, each compression library uses its own default.
        
    Returns
    -------
    PolyReader or PolyWriter
        The appropriate reader or writer context manager object.
    """
    filename = os.fsdecode(filename)
    if show_progress and mode in ('w', 'wt', 'wb', 'a', 'at', 'ab'):
        raise NotImplementedError('Progress tracking is currently isolated to reader paths. Write implementations are deferred.')
    if mode in ('r', 'rt', 'rb'):
        return PolyReader(filename, show_progress=show_progress).open()
    elif mode in ('w', 'wt', 'wb'):
        effective_backup = True if backup is None else backup
        return PolyWriter(filename, show_progress=show_progress).open(
            append=False,
            backup=effective_backup,
            compresslevel=compresslevel,
        )
    elif mode in ('a', 'at', 'ab'):
        effective_backup = False if backup is None else backup
        return PolyWriter(filename, show_progress=show_progress).open(
            append=True,
            backup=effective_backup,
            compresslevel=compresslevel,
        )
    else:
        raise ValueError(f"Unsupported mode: '{mode}'. Supported modes are 'r', 'w', 'a'.")


def main():
    """
    Main function, mostly for testing.

    This function serves as the entry point for the script, providing functionality to read from and write to files.
    It supports various compression formats (plain text, gzipped, bzip2, zstandard) and remote sources (HTTP, HTTPS, FTP, SSH, SFTP).
    """
    def format_rate(rate: float, unit: str = "") -> str:
        """Format rate with standard units."""
        if not rate:
            return f"0{unit}/s"
        return f"{format_bytes(rate)}/s"

    def pinfo(preface: str, filename: str, line_count: int, bytes_count: int, seconds: float):
        """
        Print information about the file processing.

        Parameters
        ----------
        preface : str
            A preface string for the output message.
        filename : str
            The name of the file being processed.
        line_count : int
            The number of lines processed.
        bytes_count : int
            The number of bytes processed.
        seconds : float
            The total time taken for processing.
        """
        rate = line_count / seconds if seconds > 0 else 0
        byterate = bytes_count / seconds if seconds > 0 else 0
        print(
            f"{preface} {filename}. "
            f"{line_count:,d} lines / {format_bytes(bytes_count)} "
            f"at {rate:,.1f}/s ({format_rate(byterate)}). "
            f"Total time: {seconds:0.2f} Secs."
        )

    # Set up argument parser for command-line interface
    import argparse
    parser = argparse.ArgumentParser(description='Test reading of plain text, gzipped, bzip2, or zstandard compressed files locally and remotely.')

    # Add argument for input files to process
    parser.add_argument('files', type=str, nargs='*', default=[], help='The input file(s) to process', metavar="file")
    parser.add_argument('--input-file', '-i', type=str, default=None, help='Source input file for --write mode')

    # Add mutually exclusive group for read/write modes
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--write', '-w', action='store_true', help='Write mode')
    group.add_argument('--read', '-r', action='store_true', help='Read mode')

    # Parse the command-line arguments
    args = parser.parse_args()

    # If read mode is specified
    if args.read:
        if args.input_file is not None:
            parser.error("--input-file/-i is only valid with --write")
        for input_file in args.files:
            print(f"TESTING reading {input_file}")
            with PolyReader(input_file) as reader:
                line_count = 0
                t0 = time.monotonic()
                bytes_count = 0
                for line in reader:
                    line_count += 1
                    bytes_count += len(line.encode('utf-8'))
                # Print final reading statistics
                pinfo("FINISHED Reading.", reader.filename, line_count, bytes_count, time.monotonic() - t0)
    else:
        # If write mode is specified
        if args.input_file is not None:
            if len(args.files) < 1:
                parser.error("Write mode with --input-file/-i requires at least one destination file")
            source_file = args.input_file
            output_files = args.files
        else:
            if len(args.files) < 2:
                parser.error("Write mode requires at least one source file and one destination file (e.g., 'python -m polyopen.polyopen -w src dest')")
            source_file = args.files[0]
            output_files = args.files[1:]

        import contextlib
        print(f"Will Read from {source_file}")
        line_count = 0
        bytes_count = 0
        dest_names = []
        t0 = time.monotonic()
        with contextlib.ExitStack() as stack:
            reader = stack.enter_context(PolyReader(source_file))
            writers = []
            for output_file in output_files:
                writer = stack.enter_context(PolyWriter(output_file))
                print(f"Writing {writer.filename}")
                writers.append(writer)
                dest_names.append(writer.filename)
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

