import bz2
import functools
import gzip
import http.cookiejar
import http.server
import io
import lzma
import threading
from urllib.parse import urlparse
from unittest.mock import patch, MagicMock
import pytest
import requests
import zstandard as zstd
from polyopen import polyopen, PolyReader, PolyWriter, UnsupportedArchiveError
from polyopen.polyopen import _get_http_session, _normalized_path

@patch('paramiko.SSHClient')
def test_ssh_routing(mock_ssh):
    """
    Assert polyopen intercepts ssh:// streams and spins up the paramount infrastructure
    without triggering local file bindings.
    """
    mock_instance = MagicMock()
    mock_ssh.return_value = mock_instance
    
    # We mock open_sftp to intercept and succeed
    mock_sftp = MagicMock()
    mock_instance.open_sftp.return_value = mock_sftp

    with polyopen("ssh://user:pass@host/file.txt", 'w', backup=False) as writer:
        writer.write("stub")
        
    assert mock_ssh.called
    assert mock_instance.connect.called
    assert mock_instance.open_sftp.called

@patch('requests.Session.get')
def test_http_routing(mock_get):
    """
    Assert polyopen cleanly traps http:// streams, reads via response.raw with preserved newlines,
    and explicitly closes the HTTP response.
    """
    mock_response = MagicMock()
    mock_response.headers = {'Content-Length': '100'}
    mock_response.raw = io.BytesIO(b"line 1\nline 2\n")
    mock_get.return_value = mock_response
    
    lines = []
    with polyopen("https://example.com/data.txt", 'r') as reader:
        for line in reader:
             lines.append(line)
             
    assert mock_get.called
    assert lines == ["line 1\n", "line 2\n"]
    assert mock_response.close.called


@patch('ftputil.FTPHost')
def test_ftp_read_default_port_progress_and_close(mock_host):
    """BUG-01: FTP reading (plain and .gz), default port 21, path-only getsize, and handle closure."""
    host = MagicMock()
    mock_host.return_value = host

    raw_plain = io.BytesIO("hello ftp café\n".encode("utf-8"))
    host.open.return_value = raw_plain
    with polyopen("ftp://user:pass@host/dir/file.txt", "r") as reader:
        assert list(reader) == ["hello ftp café\n"]
    assert mock_host.call_args.kwargs["port"] == 21
    assert raw_plain.closed
    assert host.close.called

    gz_bytes = gzip.compress("compressed ftp\n".encode("utf-8"))
    raw_gz = io.BytesIO(gz_bytes)
    host.reset_mock()
    host.open.return_value = raw_gz
    host.path.getsize.return_value = len(gz_bytes)
    with polyopen("ftp://user:pass@host/dir/file.txt.gz", "r", show_progress=True) as reader:
        assert list(reader) == ["compressed ftp\n"]
    assert mock_host.call_args.kwargs["port"] == 21
    host.path.getsize.assert_called_once_with("/dir/file.txt.gz")
    assert raw_gz.closed
    assert host.close.called


@patch('ftputil.FTPHost')
def test_ftp_write_default_port_compressed_close_and_999_cap(mock_host):
    """BUG-01 & BUG-05: FTP write defaults port 21, closes _raw_fh/_ftp, and caps sequential backups at 999."""
    host = MagicMock()
    mock_host.return_value = host

    existing = {"/dir/out.gz"} | {f"/dir/out.gz.{i:03d}.bu" for i in range(1, 1000)}
    host.path.exists.side_effect = lambda p: p in existing
    host.listdir.return_value = ["out.gz"] + [f"out.gz.{i:03d}.bu" for i in range(1, 1000)]

    buf = io.BytesIO()
    host.open.return_value = buf
    with patch("time.time_ns", return_value=1700000000000000123):
        with polyopen("ftp://user:pass@host/dir/out.gz", "w", backup=True) as writer:
            writer.write("ftp data\n")

    assert mock_host.call_args.kwargs["port"] == 21
    host.rename.assert_called_once_with("/dir/out.gz", "/dir/out.gz.1700000000000000123.bu")
    assert buf.closed
    assert host.close.called


@patch('paramiko.SSHClient')
def test_sftp_missing_file_raises_without_exit_or_password_leak(mock_ssh, capsys):
    """BUG-02: Missing SFTP file on reader and writer raises FileNotFoundError without SystemExit or credential leak."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp
    sftp.file.side_effect = FileNotFoundError("missing")

    with pytest.raises(FileNotFoundError):
        PolyReader("ssh://alice:supersecret@host:2222/nope.txt").open()

    captured = capsys.readouterr()
    assert "supersecret" not in captured.out
    assert "supersecret" not in captured.err
    assert client.load_system_host_keys.called
    assert client.connect.call_args.kwargs["port"] == 2222
    assert sftp.close.called
    assert client.close.called

    client.reset_mock()
    sftp.reset_mock()
    sftp.file.side_effect = FileNotFoundError("missing")
    with pytest.raises(FileNotFoundError):
        PolyWriter("sftp://alice:supersecret@host/nope.txt").open(append=False, backup=False)

    captured = capsys.readouterr()
    assert "supersecret" not in captured.out
    assert "supersecret" not in captured.err
    assert client.load_system_host_keys.called
    assert client.connect.call_args.kwargs["port"] == 22
    assert sftp.close.called
    assert client.close.called


@patch('paramiko.SSHClient')
def test_sftp_read_port_stat_and_utf8_decompression(mock_ssh):
    """BUG-02 & BUG-06: SFTP stat().st_size with no path arg and UTF-8 str reads across plain, .gz, and .bz2."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    class FakeSFTPFile(io.BytesIO):
        def __init__(self, data):
            super().__init__(data)
            self.stat_calls = []

        def stat(self):
            self.stat_calls.append(())
            st = MagicMock()
            st.st_size = len(self.getvalue())
            return st

    plain_bytes = "café 日本語\n".encode("utf-8")
    f_plain = FakeSFTPFile(plain_bytes)
    sftp.file.return_value = f_plain
    with PolyReader("sftp://u:p@host:2222/a.txt", show_progress=True) as r:
        assert list(r) == ["café 日本語\n"]
    assert client.connect.call_args.kwargs["port"] == 2222
    assert f_plain.stat_calls == [()]

    f_gz = FakeSFTPFile(gzip.compress(plain_bytes))
    sftp.file.return_value = f_gz
    with PolyReader("sftp://u:p@host/a.txt.gz") as r:
        assert r.read() == "café 日本語\n"
    assert client.connect.call_args.kwargs["port"] == 22

    f_bz2 = FakeSFTPFile(bz2.compress(plain_bytes))
    sftp.file.return_value = f_bz2
    with PolyReader("sftp://u:p@host/a.txt.bz2") as r:
        assert r.read() == "café 日本語\n"


@patch('paramiko.SSHClient')
def test_sftp_prefetch_guard_when_stat_fails_or_size_none(mock_ssh):
    """E-03: Skip SFTP prefetch when stat() raises OSError or st_size is None instead of crashing."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    class ParamikoLikeSFTPFile(io.BytesIO):
        def __init__(self, data, *, stat_raises=False, st_size_none=False):
            super().__init__(data)
            self.stat_raises = stat_raises
            self.st_size_none = st_size_none
            self.prefetch_calls = []

        def stat(self):
            if self.stat_raises:
                raise OSError("FSTAT denied")
            st = MagicMock()
            st.st_size = None if self.st_size_none else len(self.getvalue())
            return st

        def prefetch(self, file_size=None, max_concurrent_requests=None):
            if file_size is None:
                file_size = self.stat().st_size
            if file_size is None:
                raise TypeError("'<' not supported between instances of 'int' and 'NoneType'")
            self.prefetch_calls.append((file_size, max_concurrent_requests))

    f_err = ParamikoLikeSFTPFile(b"ok\n", stat_raises=True)
    sftp.file.return_value = f_err
    with PolyReader("sftp://u:p@host/a.txt") as r:
        assert r.read() == "ok\n"
    assert f_err.prefetch_calls == []

    f_none = ParamikoLikeSFTPFile(b"ok\n", st_size_none=True)
    sftp.file.return_value = f_none
    with PolyReader("sftp://u:p@host/a.txt") as r:
        assert r.read() == "ok\n"
    assert f_none.prefetch_calls == []

    f_ok = ParamikoLikeSFTPFile(b"ok\n")
    sftp.file.return_value = f_ok
    with PolyReader("sftp://u:p@host/a.txt") as r:
        assert r.read() == "ok\n"
    assert f_ok.prefetch_calls == [(3, 64)]


@patch('requests.Session.get')
def test_http_errors_query_extensions_unknown_size_and_file_methods(mock_get):
    """BUG-03, BUG-06, BUG-07: HTTP 404 close+raise, query strings, unknown Content-Length total=None, and read methods."""
    resp404 = MagicMock()
    resp404.raise_for_status.side_effect = requests.HTTPError("404")
    mock_get.return_value = resp404
    with pytest.raises(requests.HTTPError):
        PolyReader("https://example.com/missing.txt").open()
    assert resp404.close.called

    gz_data = gzip.compress("query gz\n".encode("utf-8"))
    resp_gz = MagicMock()
    resp_gz.headers = {"Content-Length": str(len(gz_data))}
    resp_gz.raw = io.BytesIO(gz_data)
    mock_get.return_value = resp_gz
    with PolyReader("https://example.com/data.txt.gz?token=1") as r:
        assert list(r) == ["query gz\n"]

    with pytest.raises(UnsupportedArchiveError):
        PolyReader("https://example.com/archive.zip?dl=1").open()

    for headers in ({}, {"Content-Length": "invalid"}):
        resp_unknown = MagicMock()
        resp_unknown.headers = headers
        resp_unknown.raw = io.BytesIO(b"line1\nline2\nline3\n")
        mock_get.return_value = resp_unknown
        with patch("os.path.getsize", side_effect=AssertionError("os.path.getsize must not be called")):
            with PolyReader("https://example.com/stream.txt", show_progress=True) as r:
                assert r._get_file_size() is None
                assert r._progress is not None
                assert r._progress.total is None
                assert r.readline() == "line1\n"
                assert r.read(2) == "li"
                assert r.readlines() == ["ne2\n", "line3\n"]

    assert _normalized_path("C:/tmp/DATA.GZ", urlparse("C:/tmp/DATA.GZ")) == "c:/tmp/data.gz"


class _InspectableBytesIO(io.BytesIO):
    """BytesIO buffer that retains written bytes after close() for post-close decompression checks."""

    def __init__(self):
        super().__init__()
        self.saved_bytes = b""
        self.close_count = 0
        self.pipelined_calls = []

    def set_pipelined(self, pipelined=True):
        self.pipelined_calls.append(pipelined)

    def close(self):
        self.close_count += 1
        if not self.closed:
            self.saved_bytes = self.getvalue()
        super().close()


def _decompress_bytes(raw: bytes, ext: str) -> str:
    if ext == ".zst":
        with zstd.ZstdDecompressor().stream_reader(io.BytesIO(raw), read_across_frames=True) as sr:
            return sr.read().decode("utf-8")
    if ext == ".gz":
        return gzip.decompress(raw).decode("utf-8")
    if ext == ".bz2":
        return bz2.decompress(raw).decode("utf-8")
    if ext == ".xz":
        return lzma.decompress(raw).decode("utf-8")
    raise ValueError(f"Unexpected extension: {ext}")


@pytest.mark.parametrize("protocol", ["sftp", "ftp"])
@pytest.mark.parametrize("ext", [".zst", ".gz", ".bz2", ".xz"])
@pytest.mark.parametrize("compresslevel", [None, 2])
def test_remote_compresslevel_default_and_explicit(protocol, ext, compresslevel):
    """p1f3dc E-01(a): Default (omitting None kwargs) and explicit compresslevel forwarding on SFTP and FTP."""
    buf = _InspectableBytesIO()
    payload = f"remote {protocol} {ext} level={compresslevel} café\n"
    url = f"{protocol}://user:pass@host/dir/out{ext}"

    if protocol == "sftp":
        transport_patch = patch("paramiko.SSHClient")
    else:
        transport_patch = patch("ftputil.FTPHost")

    if ext == ".zst":
        comp_patch = patch("zstandard.ZstdCompressor", wraps=zstd.ZstdCompressor)
    elif ext == ".gz":
        comp_patch = patch("gzip.open", wraps=gzip.open)
    elif ext == ".bz2":
        comp_patch = patch("bz2.open", wraps=bz2.open)
    else:
        comp_patch = patch("lzma.open", wraps=lzma.open)

    with transport_patch as mock_transport, comp_patch as mock_comp:
        if protocol == "sftp":
            client = MagicMock()
            sftp = MagicMock()
            mock_transport.return_value = client
            client.open_sftp.return_value = sftp
            sftp.file.return_value = buf
        else:
            host = MagicMock()
            mock_transport.return_value = host
            host.open.return_value = buf

        with polyopen(url, "w", backup=False, compresslevel=compresslevel) as writer:
            writer.write(payload)

        assert mock_comp.called
        call_args, call_kwargs = mock_comp.call_args
        if compresslevel is None:
            if ext == ".zst":
                assert call_args == ()
                assert call_kwargs == {}
            elif ext in (".gz", ".bz2"):
                assert "compresslevel" not in call_kwargs
            else:
                assert "preset" not in call_kwargs
        else:
            if ext == ".zst":
                assert call_kwargs == {"level": 2}
            elif ext in (".gz", ".bz2"):
                assert call_kwargs["compresslevel"] == 2
            else:
                assert call_kwargs["preset"] == 2

    assert buf.closed
    assert _decompress_bytes(buf.saved_bytes, ext) == payload


@patch("paramiko.SSHClient")
def test_sftp_backup_rotation_fast_path_500_posix_and_999_cap(mock_ssh):
    """p1f3dc E-01(b): SFTP .001.bu fast-path, single listdir for 500 backups, POSIX split, and 999-cap fallback."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    # 1. .001.bu fast-path without listdir
    existing_fast = {"/nested/dir/file.txt"}

    def stat_fast(path):
        if path in existing_fast:
            return MagicMock()
        raise FileNotFoundError(path)

    sftp.stat.side_effect = stat_fast
    sftp.file.return_value = _InspectableBytesIO()
    with PolyWriter("sftp://u:p@host/nested/dir/file.txt").open(backup=True) as w:
        w.write("new\n")
    sftp.listdir.assert_not_called()
    sftp.rename.assert_called_once_with("/nested/dir/file.txt", "/nested/dir/file.txt.001.bu")

    # 2. Single listdir with 500 prior backups and POSIX parent splitting (/nested/dir, /, and relative '.')
    for remote_path, expected_parent, basename in (
        ("/nested/dir/file.txt", "/nested/dir", "file.txt"),
        ("/rootfile.txt", "/", "rootfile.txt"),
        ("bare.txt", ".", "bare.txt"),
    ):
        sftp.reset_mock()
        sftp.stat.side_effect = lambda p: MagicMock()
        sftp.listdir.return_value = [basename] + [f"{basename}.{i:03d}.bu" for i in range(1, 501)]
        writer = PolyWriter("sftp://u:p@host/placeholder")
        writer._ssh_backup_if(sftp, remote_path)
        sftp.listdir.assert_called_once_with(expected_parent)
        sftp.rename.assert_called_once_with(remote_path, f"{remote_path}.501.bu")

    # 3. 999-cap collision-safe timestamp fallback
    sftp.reset_mock()
    sftp.stat.side_effect = lambda p: MagicMock()
    sftp.listdir.return_value = (
        ["cap.txt"]
        + [f"cap.txt.{i:03d}.bu" for i in range(1, 1000)]
        + ["cap.txt.1700000000000000123.bu"]
    )
    sftp.file.return_value = _InspectableBytesIO()
    with patch("time.time_ns", return_value=1700000000000000123):
        with PolyWriter("sftp://u:p@host/sub/cap.txt").open(backup=True) as w:
            w.write("after cap\n")
    sftp.listdir.assert_called_once_with("/sub")
    sftp.rename.assert_called_once_with("/sub/cap.txt", "/sub/cap.txt.1700000000000000124.bu")


@patch("ftputil.FTPHost")
def test_ftp_backup_rotation_fast_path_500_posix_and_999_cap(mock_host):
    """p1f3dc E-01(b): FTP .001.bu fast-path, single listdir for 500 backups, POSIX split, and 999-cap fallback."""
    host = MagicMock()
    mock_host.return_value = host

    # 1. .001.bu fast-path without listdir
    host.path.exists.side_effect = lambda p: p == "/nested/dir/file.txt"
    host.open.return_value = _InspectableBytesIO()
    with PolyWriter("ftp://u:p@host/nested/dir/file.txt").open(backup=True) as w:
        w.write("new\n")
    host.listdir.assert_not_called()
    host.rename.assert_called_once_with("/nested/dir/file.txt", "/nested/dir/file.txt.001.bu")

    # 2. Single listdir with 500 prior backups and POSIX parent splitting
    for remote_path, expected_parent, basename in (
        ("/nested/dir/file.txt", "/nested/dir", "file.txt"),
        ("/rootfile.txt", "/", "rootfile.txt"),
    ):
        host.reset_mock()
        host.path.exists.side_effect = None
        host.path.exists.return_value = True
        host.listdir.return_value = [basename] + [f"{basename}.{i:03d}.bu" for i in range(1, 501)]
        host.open.return_value = _InspectableBytesIO()
        with PolyWriter(f"ftp://u:p@host{remote_path}").open(backup=True) as w:
            w.write("next\n")
        host.listdir.assert_called_once_with(expected_parent)
        host.rename.assert_called_once_with(remote_path, f"{remote_path}.501.bu")

    # 3. 999-cap collision-safe timestamp fallback (existing timestamp collides -> increments by 1)
    host.reset_mock()
    host.path.exists.side_effect = None
    host.path.exists.return_value = True
    host.listdir.return_value = (
        ["cap.txt"]
        + [f"cap.txt.{i:03d}.bu" for i in range(1, 1000)]
        + ["cap.txt.1700000000000000500.bu"]
    )
    host.open.return_value = _InspectableBytesIO()
    with patch("time.time_ns", return_value=1700000000000000500):
        with PolyWriter("ftp://u:p@host/dir/cap.txt").open(backup=True) as w:
            w.write("after cap\n")
    host.listdir.assert_called_once_with("/dir")
    host.rename.assert_called_once_with("/dir/cap.txt", "/dir/cap.txt.1700000000000000501.bu")


@patch("paramiko.SSHClient")
def test_sftp_read_prefetch_fallback_cached_size_and_idempotent_close(mock_ssh):
    """p1f3dc E-01(c): SFTP bounded prefetch, <3.3 TypeError fallback, cached _get_file_size, and idempotent/error close."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    class BoundedSFTPFile(io.BytesIO):
        def __init__(self, data):
            super().__init__(data)
            self.stat_count = 0
            self.prefetch_calls = []
            self.close_count = 0

        def stat(self):
            self.stat_count += 1
            st = MagicMock()
            st.st_size = len(self.getvalue())
            return st

        def prefetch(self, file_size=None, max_concurrent_requests=None):
            self.prefetch_calls.append((file_size, max_concurrent_requests))

        def close(self):
            self.close_count += 1
            super().close()

    # 1. Bounded prefetch + cached _get_file_size (no duplicate stat RTT) + idempotent close
    f1 = BoundedSFTPFile(b"row1\nrow2\n")
    sftp.file.return_value = f1
    reader = PolyReader("sftp://u:p@host/data.txt", show_progress=True).open()
    assert f1.prefetch_calls == [(10, 64)]
    assert reader._get_file_size() == 10
    assert f1.stat_count == 1
    assert list(reader) == ["row1\n", "row2\n"]
    reader.close()
    first_close_count = f1.close_count
    assert first_close_count >= 1
    assert f1.closed
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1
    reader.close()  # idempotent: no additional close calls
    assert f1.close_count == first_close_count
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1

    # 2. Paramiko <3.3 TypeError fallback to single-arg prefetch(file_size)
    class LegacySFTPFile(io.BytesIO):
        def __init__(self, data):
            super().__init__(data)
            self.prefetch_calls = []

        def stat(self):
            st = MagicMock()
            st.st_size = len(self.getvalue())
            return st

        def prefetch(self, file_size=None):
            self.prefetch_calls.append(file_size)

    client.reset_mock()
    sftp.reset_mock()
    f_legacy = LegacySFTPFile(b"legacy\n")
    sftp.file.return_value = f_legacy
    with PolyReader("sftp://u:p@host/legacy.txt") as r:
        assert r.read() == "legacy\n"
    assert f_legacy.prefetch_calls == [7]

    # 3. Close error path still closes _sftp_file, _sftp_client, and _ssh_client and remains idempotent
    client.reset_mock()
    sftp.reset_mock()
    f_err = BoundedSFTPFile(b"err\n")
    sftp.file.return_value = f_err
    r_err = PolyReader("sftp://u:p@host/err.txt").open()
    r_err._fh.detach()
    failing_fh = MagicMock()
    failing_fh.close.side_effect = OSError("fh close failed")
    r_err._fh = failing_fh
    with pytest.raises(OSError, match="fh close failed"):
        r_err.close()
    assert failing_fh.close.call_count == 1
    assert f_err.close_count == 1
    assert f_err.closed
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1
    r_err.close()  # second close is a no-op
    assert f_err.close_count == 1
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1


@patch("paramiko.SSHClient")
@pytest.mark.parametrize("ext", [".zst", ".gz", ".bz2", ".xz"])
@pytest.mark.parametrize("append,expected_mode", [(False, "wb"), (True, "ab")])
def test_sftp_compressed_writes_binary_mode_and_pipelined(mock_ssh, ext, append, expected_mode):
    """p1f3dc E-01(c): Compressed SSH writes open in binary mode ('wb'/'ab') and call set_pipelined(True)."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp
    buf = _InspectableBytesIO()
    sftp.file.return_value = buf

    with PolyWriter(f"sftp://u:p@host/dir/out{ext}").open(append=append, backup=False) as writer:
        writer.write("pipelined line\n")

    sftp.file.assert_called_once_with(f"/dir/out{ext}", expected_mode)
    assert buf.pipelined_calls == [True]
    assert buf.closed
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1
    assert _decompress_bytes(buf.saved_bytes, ext) == "pipelined line\n"


@patch("paramiko.SSHClient")
def test_sftp_writer_close_idempotent_and_error_paths(mock_ssh):
    """p1f3dc E-01(c): PolyWriter.close() idempotently closes all SFTP/SSH handles including on error paths."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    # 1. Normal idempotent close
    sftp_file = MagicMock()
    sftp.file.return_value = sftp_file
    writer = PolyWriter("sftp://u:p@host/plain.txt").open(backup=False)
    sftp.file.assert_called_once_with("/plain.txt", "wb")
    sftp_file.set_pipelined.assert_called_once_with(True)
    writer.close()
    writer.close()
    assert sftp_file.close.call_count == 1
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1

    # 2. Error during _fh.close() still closes _sftp_file, _sftp_client, and _ssh_client
    client.reset_mock()
    sftp.reset_mock()
    buf = _InspectableBytesIO()
    sftp.file.return_value = buf
    writer_err = PolyWriter("sftp://u:p@host/out.gz").open(backup=False)
    writer_err._fh.close()  # closes gzip wrapper cleanly without closing underlying buf
    assert buf.close_count == 0
    failing_fh = MagicMock()
    failing_fh.close.side_effect = OSError("writer fh boom")
    writer_err._fh = failing_fh
    with pytest.raises(OSError, match="writer fh boom"):
        writer_err.close()
    assert failing_fh.close.call_count == 1
    assert buf.close_count == 1
    assert buf.closed
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1
    writer_err.close()  # idempotent after error
    assert buf.close_count == 1
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1

    # 3. Error during open (set_pipelined raises) cleans up acquired handles
    client.reset_mock()
    sftp.reset_mock()
    bad_file = MagicMock()
    bad_file.set_pipelined.side_effect = RuntimeError("pipeline error")
    sftp.file.return_value = bad_file
    with pytest.raises(RuntimeError, match="pipeline error"):
        PolyWriter("sftp://u:p@host/bad.txt").open(backup=False)
    assert bad_file.close.call_count == 1
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1


def test_http_session_thread_local_reuse_and_cookie_disabled():
    """p1f3dc E-01(d): Per-thread _get_http_session() reuse and disabled cookie persistence."""
    main_s1 = _get_http_session()
    main_s2 = _get_http_session()
    assert isinstance(main_s1, requests.Session)
    assert main_s1 is main_s2

    policy = main_s1.cookies.get_policy()
    assert isinstance(policy, http.cookiejar.DefaultCookiePolicy)
    assert list(policy.allowed_domains()) == []

    worker_sessions = []

    def worker():
        s1 = _get_http_session()
        s2 = _get_http_session()
        assert s1 is s2
        worker_sessions.append(s1)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert len(worker_sessions) == 2
    assert worker_sessions[0] is not worker_sessions[1]
    assert worker_sessions[0] is not main_s1
    assert worker_sessions[1] is not main_s1

    # Observable cookie rejection test against a local ephemeral HTTP server
    received_cookie_headers = []

    class CookieHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            received_cookie_headers.append(self.headers.get("Cookie"))
            body = b"hello cookie-free\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Set-Cookie", "session_token=secret123; Path=/")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: A002
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CookieHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}/data.txt"
    try:
        with PolyReader(url) as r1:
            assert list(r1) == ["hello cookie-free\n"]
        assert len(_get_http_session().cookies) == 0
        with PolyReader(url) as r2:
            assert list(r2) == ["hello cookie-free\n"]
        assert len(_get_http_session().cookies) == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert received_cookie_headers == [None, None]


@patch("requests.Session.get")
def test_http_early_iteration_exit_closes_response(mock_get):
    """p1f3dc E-01(d): Early iteration exit on HTTP stream closes the response and raw handle."""
    raw = io.BytesIO(b"line1\nline2\nline3\nline4\n")
    mock_response = MagicMock()
    mock_response.headers = {"Content-Length": "24"}
    mock_response.raw = raw
    mock_get.return_value = mock_response

    seen = []
    with PolyReader("https://example.com/early.txt") as reader:
        for line in reader:
            seen.append(line)
            break

    assert seen == ["line1\n"]
    assert raw.closed
    mock_response.close.assert_called_once()


class _TrackedHttpRaw(io.BytesIO):
    """BytesIO with a writable decode_content attribute mimicking urllib3 HTTPResponse."""

    def __init__(self, data: bytes):
        super().__init__(data)
        self.decode_content = False


@patch("requests.Session.get")
@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_http_decode_content_only_on_uncompressed(mock_get, ext):
    """p1f3dc E-01(d): decode_content = True is set strictly on uncompressed HTTP responses."""
    text = "http stream line 1\nhttp stream line 2\n"
    raw_bytes = text.encode("utf-8")
    if ext == ".gz":
        payload = gzip.compress(raw_bytes)
    elif ext == ".bz2":
        payload = bz2.compress(raw_bytes)
    elif ext == ".xz":
        payload = lzma.compress(raw_bytes)
    elif ext == ".zst":
        payload = zstd.ZstdCompressor().compress(raw_bytes)
    else:
        payload = raw_bytes

    tracked_raw = _TrackedHttpRaw(payload)
    mock_response = MagicMock()
    mock_response.headers = {"Content-Length": str(len(payload))}
    mock_response.raw = tracked_raw
    mock_get.return_value = mock_response

    with PolyReader(f"https://example.com/file{ext or '.txt'}") as reader:
        assert list(reader) == ["http stream line 1\n", "http stream line 2\n"]

    if ext == "":
        assert tracked_raw.decode_content is True
    else:
        assert tracked_raw.decode_content is False
    mock_response.close.assert_called_once()


class _StrictFTPFileProxy:
    """Mimics real ftputil.file.FTPFile: lacks readable/writable/seekable/read1/readinto."""

    def __init__(self, initial: bytes = b""):
        self._buf = io.BytesIO(initial)
        self.saved_bytes = b""
        self.closed = False

    def read(self, size=-1):
        return self._buf.read() if (size is None or size < 0) else self._buf.read(size)

    def write(self, data):
        self._buf.write(data)
        return None  # real FTPFile.write delegates to underlying file/socket

    def flush(self):
        self._buf.flush()

    def close(self):
        if not self.closed:
            self.closed = True
            self.saved_bytes = self._buf.getvalue()
            self._buf.close()


@patch("ftputil.FTPHost")
@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_ftp_real_ftpfile_contract_reads_writes_and_append_rejected(mock_host, ext):
    """S2-B1: Real ftputil.file.FTPFile (without readable/seekable/writable) works across all codecs; FTP append rejected."""
    from polyopen import UnsupportedProtocolError

    host = MagicMock()
    mock_host.return_value = host
    text = f"ftp real contract {ext} café\nline 2\n"

    # Write with strict FTPFile proxy
    write_proxy = _StrictFTPFileProxy()
    host.open.return_value = write_proxy
    with polyopen(f"ftp://user:pass@host:2121/dir/out{ext or '.txt'}", "w", backup=False, timeout=5.0) as w:
        n = w.write(text)
        assert n == len(text)
    assert write_proxy.closed
    assert mock_host.call_args.kwargs["port"] == 2121
    assert mock_host.call_args.kwargs["timeout"] == 5.0
    if ext:
        assert _decompress_bytes(write_proxy.saved_bytes, ext) == text
    else:
        assert write_proxy.saved_bytes.decode("utf-8") == text

    # Read back with strict FTPFile proxy (both show_progress=False and True)
    for show_progress in (False, True):
        read_proxy = _StrictFTPFileProxy(write_proxy.saved_bytes)
        host.open.return_value = read_proxy
        host.path.getsize.side_effect = OSError("SIZE unsupported")
        with polyopen(f"ftp://user:pass@host/dir/out{ext or '.txt'}", "r", show_progress=show_progress) as r:
            assert r.read() == text
            if show_progress:
                assert r._progress is not None and r._progress.total is None
        assert read_proxy.closed

    # FTP append fails fast BEFORE connecting
    mock_host.reset_mock()
    with pytest.raises(UnsupportedProtocolError, match="append"):
        polyopen("ftp://user:pass@host/dir/out.txt", "a")
    assert not mock_host.called


def test_ftp_session_wrapper_port_timeout_and_login_failure_closes_socket(monkeypatch):
    """S2-MEM1 & S3-T1: FTPSessionWrapper forwards port/timeout and closes socket if login() raises."""
    import ftplib
    from polyopen import FTPSessionWrapper

    calls = []
    closed = []

    def fake_connect(self, host, port=0, timeout=None):
        calls.append(("connect", host, port, timeout))

    def fake_login(self, user="", passwd=""):
        calls.append(("login", user, passwd))

    def fake_close(self):
        closed.append(True)

    monkeypatch.setattr(ftplib.FTP, "connect", fake_connect)
    monkeypatch.setattr(ftplib.FTP, "login", fake_login)
    monkeypatch.setattr(ftplib.FTP, "close", fake_close)

    FTPSessionWrapper("ftp.example", "alice", "pw", port=None, timeout=7.5)
    assert calls == [("connect", "ftp.example", 21, 7.5), ("login", "alice", "pw")]

    calls.clear()
    closed.clear()
    monkeypatch.setattr(ftplib.FTP, "login", lambda self, u="", p="": (_ for _ in ()).throw(ftplib.error_perm("530")))
    with pytest.raises(ftplib.error_perm):
        FTPSessionWrapper("ftp.example", "alice", "badpw", port=2121)
    assert closed == [True]


@patch("paramiko.SSHClient")
def test_ssh_connect_leak_fix_percent_decode_timeout_uncompressed_write_int_and_stat_fail_progress(mock_ssh):
    """S2-MEM1, S2-B3, S2-B5, S2-B6, S2-S2, S2-LIVE3: SSH lifecycle, percent-decoding, timeout, write()->int, progress stat-fail, and backup retry."""
    client = MagicMock()
    mock_ssh.return_value = client

    # 1. Failed connect() closes SSHClient (prevents leaked Paramiko Transport thread)
    client.connect.side_effect = OSError("auth failed")
    with pytest.raises(OSError, match="auth failed"):
        PolyReader("sftp://alice:pw@host/a.txt", timeout=4.0).open()
    assert client.close.call_count == 1
    assert client.connect.call_args.kwargs["timeout"] == 4.0
    assert client.connect.call_args.kwargs["banner_timeout"] == 4.0
    assert client.connect.call_args.kwargs["auth_timeout"] == 4.0

    # 2. Missing hostname raises ValueError before connect
    client.reset_mock()
    client.connect.side_effect = None
    with pytest.raises(ValueError, match="Missing hostname"):
        PolyReader("sftp:///no_host.txt").open()
    assert not client.connect.called

    # 3. Percent-decoded credentials/path + uncompressed SFTP write() returns int even when raw write returns None
    sftp = MagicMock()
    client.open_sftp.return_value = sftp
    raw_write = _StrictFTPFileProxy()
    sftp.file.return_value = raw_write
    with polyopen("sftp://user%40org:p%3As%2Fw@host/my%20dir/out.txt", "w", backup=False, timeout=9.0) as w:
        written = w.write("café\n")
        assert written == 5
    assert client.connect.call_args.kwargs["username"] == "user@org"
    assert client.connect.call_args.kwargs["password"] == "p:s/w"
    assert client.connect.call_args.kwargs["timeout"] == 9.0
    sftp.file.assert_called_once_with("/my dir/out.txt", "wb")
    assert raw_write.saved_bytes == "café\n".encode("utf-8")

    # 4. SFTP show_progress=True when stat() raises OSError degrades to total=None instead of crashing
    class StatFailSFTPFile(io.BytesIO):
        def stat(self):
            raise OSError("FSTAT unsupported")

    sftp.file.return_value = StatFailSFTPFile(b"ok\n")
    with PolyReader("sftp://u:p@host/data.txt", show_progress=True) as r:
        assert r._progress is not None
        assert r._progress.total is None
        assert r.read() == "ok\n"

    # 5. SFTP backup rename collision retries next free slot; missing source is a no-op
    sftp.reset_mock()
    sftp.stat.side_effect = FileNotFoundError("no file")
    PolyWriter("sftp://u:p@host/missing.txt")._ssh_backup_if(sftp, "/missing.txt")
    assert not sftp.rename.called

    sftp.reset_mock()
    existing = {"/dir/x.txt"}

    def stat_fn(p):
        if p in existing:
            return MagicMock()
        raise FileNotFoundError(p)

    rename_calls = []

    def rename_fn(src, dst):
        rename_calls.append((src, dst))
        if dst.endswith(".001.bu"):
            existing.add(dst)
            raise OSError("collision on 001")

    sftp.stat.side_effect = stat_fn
    sftp.listdir.return_value = ["x.txt", "x.txt.001.bu"]
    sftp.rename.side_effect = rename_fn
    PolyWriter("sftp://u:p@host/dir/x.txt")._ssh_backup_if(sftp, "/dir/x.txt")
    assert rename_calls == [("/dir/x.txt", "/dir/x.txt.001.bu"), ("/dir/x.txt", "/dir/x.txt.002.bu")]


@patch("requests.Session.get")
def test_http_accept_encoding_content_encoding_timeout_and_raw_fallbacks(mock_get):
    """S2-B7, S2-S2, S3-T1: Compressed HTTP sends Accept-Encoding: identity; Content-Encoding nulls total; raw fallbacks."""
    gz_bytes = gzip.compress("http gz café\n".encode("utf-8"))
    resp_gz = MagicMock()
    resp_gz.headers = {"Content-Length": str(len(gz_bytes))}
    resp_gz.raw = io.BytesIO(gz_bytes)
    mock_get.return_value = resp_gz

    with PolyReader("https://example.com/archive.txt.gz", show_progress=True, timeout=6.0) as r:
        assert r.read() == "http gz café\n"
    assert mock_get.call_args.kwargs["headers"] == {"Accept-Encoding": "identity"}
    assert mock_get.call_args.kwargs["timeout"] == 6.0

    # Plain HTTP with Content-Encoding: gzip nulls _http_file_size and works with minimal raw stream (no read1/readinto)
    class MinimalRaw:
        def __init__(self, data: bytes):
            self._buf = io.BytesIO(data)
            self.decode_content = False
            self.closed = False

        def read(self, size=-1):
            return self._buf.read() if (size is None or size < 0) else self._buf.read(size)

        def close(self):
            self.closed = True
            self._buf.close()

    minimal = MinimalRaw(b"plain line\n")
    resp_enc = MagicMock()
    resp_enc.headers = {"Content-Length": "5", "Content-Encoding": "gzip"}
    resp_enc.raw = minimal
    mock_get.return_value = resp_enc

    with PolyReader("https://example.com/plain.txt", show_progress=True) as r:
        assert r._get_file_size() is None
        assert r._progress is not None and r._progress.total is None
        assert r.read() == "plain line\n"
    assert minimal.decode_content is True
    assert minimal.closed


def test_http_real_server_gzip_stream():
    """S3-T1: Real local ThreadingHTTPServer serving a .gz file exercises real urllib3.HTTPResponse."""
    payload_text = "server gz line 1\nserver gz line 2\n"
    gz_body = gzip.compress(payload_text.encode("utf-8"))
    seen_accept = []

    class GzHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen_accept.append(self.headers.get("Accept-Encoding"))
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(gz_body)))
            self.end_headers()
            self.wfile.write(gz_body)

        def log_message(self, format, *args):  # noqa: A002
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), GzHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with PolyReader(f"http://127.0.0.1:{port}/data.txt.gz", show_progress=True, timeout=5.0) as r:
            assert r.read() == payload_text
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    assert seen_accept == ["identity"]


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_remote_binary_mode_http_ftp_and_sftp(ext):
    """Binary modes (rb/wb/ab) stream exact non-UTF-8 bytes across HTTP, FTP, and SFTP for all codecs."""
    from polyopen import UnsupportedProtocolError

    part1 = [b"\x00\xff\xfeheader\n", b"\x80\x81body1\n"]
    part2 = b"\xde\xad\xbe\xefbody2\n"
    full_bytes = b"".join(part1) + part2

    def _compress(raw: bytes) -> bytes:
        if ext == ".gz":
            return gzip.compress(raw)
        if ext == ".bz2":
            return bz2.compress(raw)
        if ext == ".xz":
            return lzma.compress(raw)
        if ext == ".zst":
            return zstd.ZstdCompressor().compress(raw)
        return raw

    # 1. HTTP binary read ('rb') with show_progress=True
    wire = _compress(full_bytes)
    with patch("requests.Session.get") as mock_get:
        resp = MagicMock()
        resp.headers = {"Content-Length": str(len(wire))}
        resp.raw = io.BytesIO(wire)
        mock_get.return_value = resp
        with polyopen(f"https://example.com/blob{ext or '.bin'}", "rb", show_progress=True) as r:
            assert r.encoding is None
            pbar = r._progress
            assert pbar is not None and pbar.total == len(wire)
            assert list(r) == part1 + [part2]
        assert pbar.n == len(wire)

    # 2. FTP binary write ('wb'), read ('rb'), and append ('ab') rejection
    with patch("ftputil.FTPHost") as mock_ftp:
        host = MagicMock()
        mock_ftp.return_value = host
        write_proxy = _StrictFTPFileProxy()
        host.open.return_value = write_proxy
        with polyopen(f"ftp://user:pass@host/dir/blob{ext or '.bin'}", "wb", backup=False) as w:
            assert w.encoding is None
            assert w.write(part1[0]) == len(part1[0])
            w.writelines([part1[1], part2])
            with pytest.raises(TypeError):
                w.write("str rejected on ftp binary writer")
        assert write_proxy.closed

        read_proxy = _StrictFTPFileProxy(write_proxy.saved_bytes)
        host.open.return_value = read_proxy
        host.path.getsize.return_value = len(write_proxy.saved_bytes)
        with polyopen(f"ftp://user:pass@host/dir/blob{ext or '.bin'}", "rb", show_progress=True) as r:
            assert r.encoding is None
            pbar = r._progress
            assert list(r) == part1 + [part2]
        assert pbar.n == len(write_proxy.saved_bytes)
        assert read_proxy.closed

        mock_ftp.reset_mock()
        with pytest.raises(UnsupportedProtocolError, match="append"):
            polyopen(f"ftp://user:pass@host/dir/blob{ext or '.bin'}", "ab")
        assert not mock_ftp.called

    # 3. SFTP binary write ('wb'), append ('ab'), and read ('rb') using minimal SFTPFile (no readinto)
    with patch("paramiko.SSHClient") as mock_ssh:
        client = MagicMock()
        sftp = MagicMock()
        mock_ssh.return_value = client
        client.open_sftp.return_value = sftp

        # Write part1
        w1_proxy = _StrictFTPFileProxy()
        sftp.file.return_value = w1_proxy
        with polyopen(f"sftp://u:p@host/dir/blob{ext or '.bin'}", "wb", backup=False) as w:
            assert w.encoding is None
            w.writelines(part1)
            with pytest.raises(TypeError):
                w.write("str rejected on sftp binary writer")

        # Append part2 (multi-member stream for compressed formats, raw bytes for plain)
        w2_proxy = _StrictFTPFileProxy()
        sftp.file.return_value = w2_proxy
        with polyopen(f"sftp://u:p@host/dir/blob{ext or '.bin'}", "ab") as w:
            assert w.encoding is None
            assert w.write(part2) == len(part2)

        combined_wire = w1_proxy.saved_bytes + w2_proxy.saved_bytes
        r_proxy = _StrictFTPFileProxy(combined_wire)
        sftp.file.return_value = r_proxy
        with polyopen(f"sftp://u:p@host/dir/blob{ext or '.bin'}", "rb", show_progress=True) as r:
            assert r.encoding is None
            assert list(r) == part1 + [part2]
        assert r_proxy.closed


def test_percent_encoded_remote_gz_uses_gzip_codec():
    """iev841 E-01: Percent-encoded .gz in SFTP and HTTPS URLs selects gzip codec and identity Accept-Encoding."""
    payload = "percent-encoded gzip line 1\nline 2\n"

    with patch("paramiko.SSHClient") as mock_ssh:
        client = MagicMock()
        sftp = MagicMock()
        mock_ssh.return_value = client
        client.open_sftp.return_value = sftp

        write_buf = _InspectableBytesIO()
        sftp.file.return_value = write_buf
        with polyopen("sftp://u@h/dir/data%2Egz", "w", backup=False) as w:
            w.write(payload)
        sftp.file.assert_called_once_with("/dir/data.gz", "wb")
        assert write_buf.closed
        assert gzip.decompress(write_buf.saved_bytes).decode("utf-8") == payload

        sftp.reset_mock()
        read_buf = io.BytesIO(write_buf.saved_bytes)
        sftp.file.return_value = read_buf
        with polyopen("sftp://u@h/dir/data%2Egz", "r") as r:
            assert r.read() == payload
        sftp.file.assert_called_once_with("/dir/data.gz", "rb")

    gz_wire = gzip.compress(payload.encode("utf-8"))
    with patch("requests.Session.get") as mock_get:
        resp = MagicMock()
        resp.headers = {"Content-Length": str(len(gz_wire))}
        resp.raw = io.BytesIO(gz_wire)
        mock_get.return_value = resp
        with polyopen("https://h/dir/data%2Egz", "r") as r:
            assert r.read() == payload
        assert mock_get.call_args.kwargs["headers"] == {"Accept-Encoding": "identity"}
        assert resp.close.called


@patch("paramiko.SSHClient")
def test_sftp_backup_rejects_remote_directory_and_closes_handles(mock_ssh):
    """iev841 E-02: SFTP backup=True raises IsADirectoryError on existing remote directories without renaming."""
    import stat

    for opener in (
        lambda url: polyopen(url, "w", backup=True),
        lambda url: PolyWriter(url).open(backup=True),
    ):
        client = MagicMock()
        sftp = MagicMock()
        mock_ssh.return_value = client
        client.open_sftp.return_value = sftp

        dir_stat = MagicMock()
        dir_stat.st_mode = stat.S_IFDIR | 0o755
        sftp.stat.return_value = dir_stat

        with pytest.raises(IsADirectoryError):
            opener("sftp://u:p@host/dir/existing_dir")

        sftp.rename.assert_not_called()
        sftp.file.assert_not_called()
        assert sftp.close.call_count == 1
        assert client.close.call_count == 1


@patch("ftputil.FTPHost")
def test_ftp_backup_rejects_remote_directory_and_closes_handles(mock_host):
    """iev841 E-02: FTP backup=True raises IsADirectoryError on existing remote directories without renaming."""
    for opener in (
        lambda url: polyopen(url, "w", backup=True),
        lambda url: PolyWriter(url).open(backup=True),
    ):
        host = MagicMock()
        mock_host.return_value = host
        host.path.exists.return_value = True
        host.path.isdir.return_value = True

        with pytest.raises(IsADirectoryError):
            opener("ftp://u:p@host/dir/existing_dir")

        host.rename.assert_not_called()
        host.open.assert_not_called()
        assert host.close.call_count == 1
