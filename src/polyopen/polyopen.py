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

import argparse
import bz2
import errno
import ftplib
import gzip
import io
import lzma
import os
import time
from typing import Optional
from urllib.parse import ParseResult, urlparse

_NETWORK_SCHEMES = frozenset(('http', 'https', 'ftp', 'ssh', 'sftp', 's3', 'gs', 'az', 'hdfs'))
_LINK_FALLBACK_ERRNOS = frozenset(
    code
    for name in ('EOPNOTSUPP', 'ENOTSUP', 'ENOSYS', 'EPERM', 'EXDEV', 'EMLINK')
    for code in (getattr(errno, name, None),)
    if code is not None
)


def _normalized_path(filename: str, parsed: ParseResult) -> str:
    """Return lowercase path component for network URLs and lowercase filename for local paths."""
    if parsed.scheme.lower() in _NETWORK_SCHEMES:
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

    def __init__(self, filename: str, show_progress: bool = False):
        """
        Initialize PolyReader with a filename.

        Parameters
        ----------
        filename : str
            The name of the file to read.
        show_progress : bool
            Whether to show a progress bar.
        """
        self.filename = filename
        self.show_progress = show_progress
        self._fh = None
        self._raw_fh = None
        self._ssh_client = None
        self._sftp_client = None
        self._sftp_file = None
        self._ftp = None
        self._ftp_path = None
        self._response = None
        self._progress = None
        self._request_iterator = False
        self._http_file_size = None

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

    def __iter__(self) -> 'PolyReader':
        """
        Make the PolyReader object iterable.

        This method allows the PolyReader to be used as an iterator.

        Returns
        -------
        PolyReader
            The PolyReader instance itself.
        """
        return self

    def __next__(self):
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
        if self._request_iterator:
            line = next(self._fh)
        else:
            line = self._fh.readline()

        if self.show_progress and self._progress is not None:
            self._progress.update(len(line))

        if not line:
            # End of file
            raise StopIteration
        if isinstance(line, bytes):
            return line.decode()
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
            session_factory=FTPSessionWrapper,
        )
        self._ftp_path = parsed.path
        raw_fh = self._ftp.open(parsed.path, 'rb')
        self._raw_fh = raw_fh

        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            self._fh = io.TextIOWrapper(
                dctx.stream_reader(self._raw_fh, read_across_frames=True),
                encoding='utf-8',
            )
        elif norm.endswith('.bz2'):
            self._fh = bz2.open(self._raw_fh, 'rt')
        elif norm.endswith('.xz'):
            self._fh = lzma.open(self._raw_fh, 'rt')
        elif norm.endswith('.gz'):
            self._fh = gzip.open(self._raw_fh, 'rt')
        else:
            self._fh = io.TextIOWrapper(self._raw_fh, encoding='utf-8')

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
        except Exception:
            self.close()
            raise

        self._fh = sftp_file
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            self._fh = io.TextIOWrapper(
                dctx.stream_reader(self._sftp_file, read_across_frames=True),
                encoding='utf-8',
            )
        elif norm.endswith('.bz2'):
            self._fh = bz2.BZ2File(self._sftp_file)
        elif norm.endswith('.xz'):
            self._fh = lzma.open(self._sftp_file, 'rt')
        elif norm.endswith('.gz'):
            self._fh = gzip.GzipFile(fileobj=self._sftp_file)

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
        import requests
        if parsed is None:
            parsed = urlparse(self.filename)
        response = requests.get(self.filename, stream=True)
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
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            self._fh = io.TextIOWrapper(
                dctx.stream_reader(response.raw, read_across_frames=True),
                encoding='utf-8',
            )
        elif norm.endswith('.bz2'):
            self._fh = bz2.open(response.raw, 'rt')
        elif norm.endswith('.xz'):
            self._fh = lzma.open(response.raw, 'rt')
        elif norm.endswith('.gz'):
            self._fh = gzip.open(response.raw, 'rt')
        else:
            self._fh = response.iter_lines()
            self._request_iterator = True

    def _wrap_local(self, parsed: Optional[ParseResult] = None):
        """
        Wrap self._fh to handle local file operations with optional decompression.

        This method opens the specified local file and wraps it with appropriate decompression
        handlers based on the file extension, setting the file handle to the decompressed content.

        Parameters
        ----------
        parsed : ParseResult or None, optional
            The parsed path/URL (parsed from self.filename if omitted).

        Returns
        -------
        None
        """
        if parsed is None:
            parsed = urlparse(self.filename)
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            self._raw_fh = open(self.filename, 'rb')
            self._fh = io.TextIOWrapper(
                dctx.stream_reader(self._raw_fh, read_across_frames=True),
                encoding='utf-8',
            )
        elif norm.endswith('.bz2'):
            self._fh = bz2.open(self.filename, 'rt')
        elif norm.endswith('.xz'):
            self._fh = lzma.open(self.filename, 'rt')
        elif norm.endswith('.gz'):
            self._fh = gzip.open(self.filename, 'rt')
        else:
            self._fh = open(self.filename, 'r')

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

        parsed = urlparse(self.filename)
        if parsed.scheme == 'file':
            from urllib.request import url2pathname
            self.filename = url2pathname(parsed.path)
            parsed = urlparse(self.filename)

        if _normalized_path(self.filename, parsed).endswith(UNSUPPORTED_ARCHIVES):
            raise UnsupportedArchiveError(
                f"polyopen does not support reading archive formats directly: {self.filename}. "
                f"Use the standard library (e.g. zipfile) instead."
            )

        try:
            scheme = parsed.scheme.lower()
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
                if file_size is not None:
                    self._progress = tqdm(total=file_size, unit='B', unit_scale=True)
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
        self._sftp_client = None
        self._ssh_client = None
        self._ftp = None
        self._ftp_path = None
        self._response = None
        self._http_file_size = None
        self._progress = None
        self._request_iterator = False

        _close_handles(fh, raw_fh, sftp_file, sftp_client, ssh_client, ftp, response, progress)


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

    def __init__(self, filename: str):
        """
        Initialize PolyWriter with a filename.

        Parameters
        ----------
        filename : str
            The name of the file to write to.
        """
        self.filename = filename
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

    def _wrap_ftp(self, parsed: ParseResult, append: bool, backup: bool):
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
            session_factory=FTPSessionWrapper,
        )
        self.remote_filename = parsed.path

        if backup and self._ftp.path.exists(parsed.path):
            backup_filename = None
            for num in range(1, 1000):
                candidate = f"{parsed.path}.{num:03d}.bu"
                if not self._ftp.path.exists(candidate):
                    backup_filename = candidate
                    break
            if backup_filename is None:
                ts_ns = time.time_ns()
                for _ in range(10000):
                    candidate = f"{parsed.path}.{ts_ns}.bu"
                    if not self._ftp.path.exists(candidate):
                        backup_filename = candidate
                        break
                    ts_ns += 1
                else:
                    raise FileExistsError(f"Could not allocate unique backup name for {parsed.path}")
            self._ftp.rename(parsed.path, backup_filename)

        mode = 'ab' if append else 'wb'
        raw_fh = self._ftp.open(parsed.path, mode)
        norm = _normalized_path(self.filename, parsed)
        if norm.endswith('.zst'):
            import zstandard as zstd
            self._raw_fh = raw_fh
            dctx = zstd.ZstdCompressor(level=22)
            self._fh = io.TextIOWrapper(dctx.stream_writer(self._raw_fh), encoding='utf-8')
        elif norm.endswith('.bz2'):
            self._raw_fh = raw_fh
            self._fh = bz2.open(self._raw_fh, compresslevel=9, mode='at' if append else 'wt')
        elif norm.endswith('.xz'):
            self._raw_fh = raw_fh
            self._fh = lzma.open(self._raw_fh, preset=9, mode='at' if append else 'wt')
        elif norm.endswith('.gz'):
            self._raw_fh = raw_fh
            self._fh = gzip.open(self._raw_fh, compresslevel=9, mode='at' if append else 'wt')
        else:
            self._fh = raw_fh

    def _ssh_backup_if(self, sftp, path):
        """
        Backup the specified file on the SSH/SFTP server if it exists.

        This method creates a backup of the file if it exists by renaming it
        with a .bu extension and an incrementing number (capped at 999 with nanosecond fallback).

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

        backup_filename = None
        for num in range(1, 1000):
            candidate = f"{path}.{num:03d}.bu"
            try:
                sftp.stat(candidate)
            except FileNotFoundError:
                backup_filename = candidate
                break

        if backup_filename is None:
            ts_ns = time.time_ns()
            for _ in range(10000):
                candidate = f"{path}.{ts_ns}.bu"
                try:
                    sftp.stat(candidate)
                except FileNotFoundError:
                    backup_filename = candidate
                    break
                ts_ns += 1
            else:
                raise FileExistsError(f"Could not allocate unique backup name for {path}")

        sftp.rename(path, backup_filename)

    def _wrap_ssh(self, parsed: ParseResult, append: bool, backup: bool):  # noqa: C901
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
        except Exception:
            self.close()
            raise

        self._fh = sftp_file
        if norm.endswith('.zst'):
            import zstandard as zstd
            dctx = zstd.ZstdCompressor(level=22)
            self._fh = io.TextIOWrapper(dctx.stream_writer(self._sftp_file), encoding='utf-8')
        elif norm.endswith('.bz2'):
            self._fh = bz2.open(self._sftp_file, compresslevel=9, mode='at' if append else 'wt')
        elif norm.endswith('.xz'):
            self._fh = lzma.open(self._sftp_file, preset=9, mode='at' if append else 'wt')
        elif norm.endswith('.gz'):
            self._fh = gzip.open(self._sftp_file, compresslevel=9, mode='at' if append else 'wt')

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

        for num in range(1, 1000):
            candidate = f"{src}.{num:03d}.bu"
            if os.path.exists(candidate):
                continue
            outcome = _try_move(candidate)
            if outcome in ('moved', 'gone'):
                return

        ts_ns = time.time_ns()
        for _ in range(10000):
            candidate = f"{src}.{ts_ns}.bu"
            if os.path.exists(candidate):
                ts_ns += 1
                continue
            outcome = _try_move(candidate)
            if outcome in ('moved', 'gone'):
                return
            ts_ns += 1
        raise FileExistsError(f"Could not allocate unique backup name for {src}")

    def _wrap_local(self, append: bool, backup: bool, parsed: Optional[ParseResult] = None):
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

        Returns
        -------
        None
        """
        if backup:
            self._backup_local_file()

        if parsed is None:
            parsed = urlparse(self.filename)
        norm = _normalized_path(self.filename, parsed)
        mode = 'ab' if append else 'wb'
        if norm.endswith('.zst'):
            import zstandard as zstd
            cctx = zstd.ZstdCompressor()
            self._raw_fh = open(self.filename, mode)
            self._fh = io.TextIOWrapper(cctx.stream_writer(self._raw_fh), encoding='utf-8')
        elif norm.endswith('.bz2'):
            self._fh = bz2.open(self.filename, 'at' if append else 'wt')
        elif norm.endswith('.xz'):
            self._fh = lzma.open(self.filename, 'at' if append else 'wt')
        elif norm.endswith('.gz'):
            self._fh = gzip.open(self.filename, 'at' if append else 'wt')
        else:
            self._fh = open(self.filename, 'a' if append else 'w')

    def open(self, append: bool = False, backup: bool = True) -> 'PolyWriter':
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

        parsed = urlparse(self.filename)
        if parsed.scheme == 'file':
            from urllib.request import url2pathname
            self.filename = url2pathname(parsed.path)
            parsed = urlparse(self.filename)

        if _normalized_path(self.filename, parsed).endswith(UNSUPPORTED_ARCHIVES):
            raise UnsupportedArchiveError(
                f"polyopen does not support writing or appending to archive formats safely: {self.filename}. "
                f"Use the standard library (e.g. zipfile) to mutate archives."
            )

        if append and backup:
            raise ValueError("Cannot have both append and backup set to True.")

        scheme = parsed.scheme.lower()
        if scheme in ('http', 'https'):
            raise ReadOnlyProtocolError(f"Protocol '{parsed.scheme}' does not support write/append operations in polyopen.")

        if scheme in ('s3', 'gs', 'az', 'hdfs'):
            raise UnsupportedProtocolError(
                f"polyopen does not support explicitly pushing to cloud object stores like '{parsed.scheme}://'. "
                f"We heavily recommend using the 'smart_open' library or dedicated SDKs (like boto3) for unified cloud I/O."
            )

        try:
            if scheme == 'ftp':
                self._wrap_ftp(parsed, append, backup)
            elif scheme in ('sftp', 'ssh'):
                self._wrap_ssh(parsed, append, backup)
            else:
                self._wrap_local(append, backup, parsed)
        except Exception:
            self.close()
            raise

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
        None
        """
        self._fh.write(data)

    def close(self):
        """
        Close the file.

        This method closes the file handle and any associated resources idempotently,
        resetting all handle attributes even if one handle fails to close.

        Returns
        -------
        None
        """
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


def polyopen(filename: str, mode: str = 'r', backup: Optional[bool] = None, show_progress: bool = False):
    """
    Open a file for reading or writing, determining the appropriate stream and compression 
    automatically based on the file extension and protocol scheme.

    This function mimics Python's built-in `open()`, returning a `PolyReader` or `PolyWriter`
    context manager.
    
    Parameters
    ----------
    filename : str
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
        
    Returns
    -------
    PolyReader or PolyWriter
        The appropriate reader or writer context manager object.
    """
    if show_progress and mode in ('w', 'wt', 'wb', 'a', 'at', 'ab'):
        raise NotImplementedError('Progress tracking is currently isolated to reader paths. Write implementations are deferred.')
    if mode in ('r', 'rt', 'rb'):
        return PolyReader(filename, show_progress=show_progress).open()
    elif mode in ('w', 'wt', 'wb'):
        effective_backup = True if backup is None else backup
        return PolyWriter(filename).open(append=False, backup=effective_backup)
    elif mode in ('a', 'at', 'ab'):
        effective_backup = False if backup is None else backup
        return PolyWriter(filename).open(append=True, backup=effective_backup)
    else:
        raise ValueError(f"Unsupported mode: '{mode}'. Supported modes are 'r', 'w', 'a'.")


def main():
    """
    Main function, mostly for testing.

    This function serves as the entry point for the script, providing functionality to read from and write to files.
    It supports various compression formats (plain text, gzipped, bzip2, zstandard) and remote sources (HTTP, HTTPS, FTP, SSH, SFTP).
    """
    import time

    def format_bytes(num: float) -> str:
        """Format bytes to human readable sizes."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
            if num < 1024.0:
                return f"{num:3.1f}{unit}"
            num /= 1024.0
        return f"{num:.1f}Y"

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
        pass  # for auto-indentation

    # Set up argument parser for command-line interface
    parser = argparse.ArgumentParser(description='Test reading of plain text, gzipped, bzip2, or zstandard compressed files locally and remotely.')

    # Add argument for input files to process
    parser.add_argument('files', type=str, nargs='*', default=[], help='The input file(s) to process', metavar="file")

    # Add mutually exclusive group for read/write modes
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--write', '-w', action='store_true', help='Write mode')
    group.add_argument('--read', '-r', action='store_true', help='Read mode')

    # Parse the command-line arguments
    args = parser.parse_args()

    # If read mode is specified
    if args.read:
        for input_file in args.files:
            print(f"TESTING reading {input_file}")
            with PolyReader(input_file) as reader:
                line_count = 0
                t0 = time.monotonic()
                bytes_count = 0
                for line in reader:
                    line_count += 1
                    bytes_count += len(line)
                    pass  # for auto-indentation
                # Print final reading statistics
                pinfo("FINISHED Reading.", reader.filename, line_count, bytes_count, time.monotonic() - t0)
                pass  # for auto-indentation
            pass  # for auto-indentation
        pass  # for auto-indentation
    else:
        # If write mode is specified
        if len(args.files) < 2:
            parser.error("Write mode requires at least one source file and one destination file (e.g., 'python -m polyopen.polyopen -w src dest')")
            exit(1)
            pass # for auto-indentation
            
        source_file = args.files[0]
        output_files = args.files[1:]
        
        print(f"Will Read from {source_file}")
        for output_file in output_files:
            with PolyReader(source_file) as reader:
                with PolyWriter(output_file) as writer:
                    print(f"Writing {writer.filename}")
                    line_count = 0
                    t0 = time.monotonic()
                    bytes_count = 0
                    for line in reader:
                        line_count += 1
                        bytes_count += len(line)
                        writer.write(line)
                        pass  # for auto-indentation
                    # Print final writing statistics
                    pinfo("FINISHED Writing.", writer.filename, line_count, bytes_count, time.monotonic() - t0)
                    pass  # for auto-indentation
                pass  # for auto-indentation
            pass  # for auto-indentation
        pass  # for auto-indentation
    pass  # for auto-indentation


if __name__ == "__main__":
    main()

