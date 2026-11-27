import bz2
import functools
import gzip
import http.cookiejar
import http.server
import io
import lzma
import os
import threading
from urllib.parse import urlparse
from unittest.mock import patch, MagicMock
import pytest
import requests
import zstandard as zstd
from openas import openas, OpenAsReader, OpenAsWriter, UnsupportedArchiveError
from openas.openas import _get_http_session, _normalized_path

def test_ssh_routing():
    """
    was3ig E-02: Enforce Paramiko RejectPolicy contract for both OpenAsReader and OpenAsWriter
    (load_system_host_keys called, set_missing_host_key_policy never called) and verify real
    paramiko.SSHClient defaults to RejectPolicy.
    """
    import paramiko

    real_client = paramiko.SSHClient()
    try:
        assert isinstance(real_client._policy, paramiko.RejectPolicy)
    finally:
        real_client.close()

    with patch('paramiko.SSHClient') as mock_ssh:
        # 1. OpenAsReader._wrap_ssh
        reader_client = MagicMock()
        reader_sftp = MagicMock()
        reader_client.open_sftp.return_value = reader_sftp
        reader_sftp.file.return_value = io.BytesIO(b"hello ssh\n")
        mock_ssh.return_value = reader_client

        with openas("ssh://user:pass@host/file.txt", 'r') as reader:
            assert reader.read() == "hello ssh\n"

        reader_client.load_system_host_keys.assert_called_once()
        reader_client.set_missing_host_key_policy.assert_not_called()
        reader_client.connect.assert_called_once()
        reader_client.open_sftp.assert_called_once()
        reader_sftp.close.assert_called_once()
        reader_client.close.assert_called_once()

        # 2. OpenAsWriter._wrap_ssh
        writer_client = MagicMock()
        writer_sftp = MagicMock()
        writer_buf = _InspectableBytesIO()
        writer_client.open_sftp.return_value = writer_sftp
        writer_sftp.file.return_value = writer_buf
        mock_ssh.return_value = writer_client

        with openas("ssh://user:pass@host/file.txt", 'w', backup=False) as writer:
            writer.write("stub\n")

        writer_client.load_system_host_keys.assert_called_once()
        writer_client.set_missing_host_key_policy.assert_not_called()
        writer_client.connect.assert_called_once()
        writer_client.open_sftp.assert_called_once()
        assert writer_buf.saved_bytes == f"stub{os.linesep}".encode("utf-8")
        assert writer_buf.closed
        writer_sftp.close.assert_called_once()
        writer_client.close.assert_called_once()


@patch('paramiko.SSHClient')
def test_sftp_backup_five_collision_retry_exhaustion_closes_handles(mock_ssh):
    """was3ig E-02: Exhausting the 5-retry SFTP backup collision loop propagates OSError and closes handles."""
    import stat

    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    file_stat = MagicMock()
    file_stat.st_mode = stat.S_IFREG | 0o644
    sftp.stat.side_effect = lambda p: file_stat
    sftp.listdir.return_value = ["file.txt", "file.txt.001.bu"]

    rename_targets = []

    def collide_always(src, dst):
        rename_targets.append(dst)
        raise OSError(f"collision #{len(rename_targets)} on {dst}")

    sftp.rename.side_effect = collide_always

    writer = OpenAsWriter("sftp://u:p@host/dir/file.txt")
    with pytest.raises(OSError, match=r"collision #5 on /dir/file\.txt\.006\.bu"):
        writer.open(backup=True)

    assert rename_targets == [f"/dir/file.txt.{i:03d}.bu" for i in range(2, 7)]
    sftp.file.assert_not_called()
    assert writer.closed is True
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1

@patch('requests.Session.get')
def test_http_routing(mock_get):
    """
    Assert openas cleanly traps http:// streams, reads via response.raw with preserved newlines,
    and explicitly closes the HTTP response.
    """
    mock_response = MagicMock()
    mock_response.headers = {'Content-Length': '100'}
    mock_response.raw = io.BytesIO(b"line 1\nline 2\n")
    mock_get.return_value = mock_response
    
    lines = []
    with openas("https://example.com/data.txt", 'r') as reader:
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
    with openas("ftp://user:pass@host/dir/file.txt", "r") as reader:
        assert list(reader) == ["hello ftp café\n"]
    assert mock_host.call_args.kwargs["port"] == 21
    assert raw_plain.closed
    assert host.close.called

    gz_bytes = gzip.compress("compressed ftp\n".encode("utf-8"))
    raw_gz = io.BytesIO(gz_bytes)
    host.reset_mock()
    host.open.return_value = raw_gz
    host.path.getsize.return_value = len(gz_bytes)
    with openas("ftp://user:pass@host/dir/file.txt.gz", "r", show_progress=True) as reader:
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
        with openas("ftp://user:pass@host/dir/out.gz", "w", backup=True) as writer:
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
        OpenAsReader("ssh://alice:supersecret@host:2222/nope.txt").open()

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
        OpenAsWriter("sftp://alice:supersecret@host/nope.txt").open(append=False, backup=False)

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
    with OpenAsReader("sftp://u:p@host:2222/a.txt", show_progress=True) as r:
        assert list(r) == ["café 日本語\n"]
    assert client.connect.call_args.kwargs["port"] == 2222
    assert f_plain.stat_calls == [()]

    f_gz = FakeSFTPFile(gzip.compress(plain_bytes))
    sftp.file.return_value = f_gz
    with OpenAsReader("sftp://u:p@host/a.txt.gz") as r:
        assert r.read() == "café 日本語\n"
    assert client.connect.call_args.kwargs["port"] == 22

    f_bz2 = FakeSFTPFile(bz2.compress(plain_bytes))
    sftp.file.return_value = f_bz2
    with OpenAsReader("sftp://u:p@host/a.txt.bz2") as r:
        assert r.read() == "café 日本語\n"


@patch('paramiko.SSHClient')
def test_sftp_prefetch_guard_when_stat_fails_or_size_none(mock_ssh):
    """a224p3 E-01: Never call unbounded prefetch(); fall back to sequential read() on stat error, None, <=0, or no readv."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    class ParamikoLikeSFTPFile(io.BytesIO):
        MAX_REQUEST_SIZE = 32768

        def __init__(self, data, *, stat_raises=False, st_size_override="auto", has_readv=True):
            super().__init__(data)
            self.stat_raises = stat_raises
            self.st_size_override = st_size_override
            self.prefetch_calls = []
            self.readv_calls = []
            if not has_readv:
                self.readv = None

        def stat(self):
            if self.stat_raises:
                raise OSError("FSTAT denied")
            st = MagicMock()
            if self.st_size_override == "auto":
                st.st_size = len(self.getvalue())
            else:
                st.st_size = self.st_size_override
            return st

        def prefetch(self, file_size=None, max_concurrent_requests=None):
            self.prefetch_calls.append((file_size, max_concurrent_requests))
            raise AssertionError("unbounded prefetch() must not be called")

        def readv(self, chunks, max_concurrent_prefetch_requests=None):
            self.readv_calls.append((list(chunks), max_concurrent_prefetch_requests))
            buf = self.getvalue()
            for offset, length in chunks:
                yield buf[offset:offset + length]

    f_err = ParamikoLikeSFTPFile(b"ok\n", stat_raises=True)
    sftp.file.return_value = f_err
    with OpenAsReader("sftp://u:p@host/a.txt") as r:
        assert r.read() == "ok\n"
    assert f_err.prefetch_calls == []
    assert f_err.readv_calls == []

    f_none = ParamikoLikeSFTPFile(b"ok\n", st_size_override=None)
    sftp.file.return_value = f_none
    with OpenAsReader("sftp://u:p@host/a.txt") as r:
        assert r.read() == "ok\n"
    assert f_none.prefetch_calls == []
    assert f_none.readv_calls == []

    f_zero = ParamikoLikeSFTPFile(b"", st_size_override=0)
    sftp.file.return_value = f_zero
    with OpenAsReader("sftp://u:p@host/empty.txt") as r:
        assert r.read() == ""
    assert f_zero.prefetch_calls == []
    assert f_zero.readv_calls == []

    f_neg = ParamikoLikeSFTPFile(b"neg\n", st_size_override=-1)
    sftp.file.return_value = f_neg
    with OpenAsReader("sftp://u:p@host/neg.txt") as r:
        assert r.read() == "neg\n"
    assert f_neg.prefetch_calls == []
    assert f_neg.readv_calls == []

    f_no_readv = ParamikoLikeSFTPFile(b"seq\n", has_readv=False)
    sftp.file.return_value = f_no_readv
    with OpenAsReader("sftp://u:p@host/seq.txt") as r:
        assert r.read() == "seq\n"
    assert f_no_readv.prefetch_calls == []

    f_ok = ParamikoLikeSFTPFile(b"ok\n")
    sftp.file.return_value = f_ok
    with OpenAsReader("sftp://u:p@host/a.txt") as r:
        assert r.read() == "ok\n"
    assert f_ok.prefetch_calls == []
    assert f_ok.readv_calls == [([(0, 3)], 64)]


@patch('requests.Session.get')
def test_http_errors_query_extensions_unknown_size_and_file_methods(mock_get):
    """BUG-03, BUG-06, BUG-07: HTTP 404 close+raise, query strings, unknown Content-Length total=None, and read methods."""
    resp404 = MagicMock()
    resp404.raise_for_status.side_effect = requests.HTTPError("404")
    mock_get.return_value = resp404
    with pytest.raises(requests.HTTPError):
        OpenAsReader("https://example.com/missing.txt").open()
    assert resp404.close.called

    gz_data = gzip.compress("query gz\n".encode("utf-8"))
    resp_gz = MagicMock()
    resp_gz.headers = {"Content-Length": str(len(gz_data))}
    resp_gz.raw = io.BytesIO(gz_data)
    mock_get.return_value = resp_gz
    with OpenAsReader("https://example.com/data.txt.gz?token=1") as r:
        assert list(r) == ["query gz\n"]

    with pytest.raises(UnsupportedArchiveError):
        OpenAsReader("https://example.com/archive.zip?dl=1").open()

    for headers in ({}, {"Content-Length": "invalid"}):
        resp_unknown = MagicMock()
        resp_unknown.headers = headers
        resp_unknown.raw = io.BytesIO(b"line1\nline2\nline3\n")
        mock_get.return_value = resp_unknown
        with patch("os.path.getsize", side_effect=AssertionError("os.path.getsize must not be called")):
            with OpenAsReader("https://example.com/stream.txt", show_progress=True) as r:
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

        with openas(url, "w", backup=False, compresslevel=compresslevel) as writer:
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
    assert _decompress_bytes(buf.saved_bytes, ext) == payload.replace("\n", os.linesep)


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
    with OpenAsWriter("sftp://u:p@host/nested/dir/file.txt").open(backup=True) as w:
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
        writer = OpenAsWriter("sftp://u:p@host/placeholder")
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
        with OpenAsWriter("sftp://u:p@host/sub/cap.txt").open(backup=True) as w:
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
    with OpenAsWriter("ftp://u:p@host/nested/dir/file.txt").open(backup=True) as w:
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
        with OpenAsWriter(f"ftp://u:p@host{remote_path}").open(backup=True) as w:
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
        with OpenAsWriter("ftp://u:p@host/dir/cap.txt").open(backup=True) as w:
            w.write("after cap\n")
    host.listdir.assert_called_once_with("/dir")
    host.rename.assert_called_once_with("/dir/cap.txt", "/dir/cap.txt.1700000000000000501.bu")


@patch("paramiko.SSHClient")
def test_sftp_read_prefetch_fallback_cached_size_and_idempotent_close(mock_ssh):
    """a224p3 E-01: Bounded SFTP readv(), <3.3 TypeError fallback, cached _get_file_size, and idempotent/error close."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    class BoundedSFTPFile(io.BytesIO):
        MAX_REQUEST_SIZE = 32768

        def __init__(self, data):
            super().__init__(data)
            self.stat_count = 0
            self.prefetch_calls = []
            self.readv_calls = []
            self.close_count = 0

        def stat(self):
            self.stat_count += 1
            st = MagicMock()
            st.st_size = len(self.getvalue())
            return st

        def prefetch(self, file_size=None, max_concurrent_requests=None):
            self.prefetch_calls.append((file_size, max_concurrent_requests))
            raise AssertionError("unbounded prefetch() must not be called")

        def readv(self, chunks, max_concurrent_prefetch_requests=None):
            self.readv_calls.append((list(chunks), max_concurrent_prefetch_requests))
            buf = self.getvalue()
            for offset, length in chunks:
                yield buf[offset:offset + length]

        def close(self):
            self.close_count += 1
            super().close()

    # 1. Bounded readv() + cached _get_file_size (no duplicate stat RTT) + idempotent close
    f1 = BoundedSFTPFile(b"row1\nrow2\n")
    sftp.file.return_value = f1
    reader = OpenAsReader("sftp://u:p@host/data.txt", show_progress=True).open()
    assert f1.prefetch_calls == []
    assert reader._get_file_size() == 10
    assert f1.stat_count == 1
    assert list(reader) == ["row1\n", "row2\n"]
    assert f1.readv_calls == [([(0, 10)], 64)]
    assert reader._progress is not None
    assert reader._progress.n == 10
    reader.close()
    assert f1.close_count == 1
    assert f1.closed
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1
    reader.close()  # idempotent: no additional close calls
    assert f1.close_count == 1
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1

    # 2. Paramiko <3.3 TypeError fallback to single-arg readv(chunks)
    class LegacySFTPFile(io.BytesIO):
        MAX_REQUEST_SIZE = 32768

        def __init__(self, data):
            super().__init__(data)
            self.readv_calls = []

        def stat(self):
            st = MagicMock()
            st.st_size = len(self.getvalue())
            return st

        def readv(self, chunks):
            self.readv_calls.append(list(chunks))
            buf = self.getvalue()
            for offset, length in chunks:
                yield buf[offset:offset + length]

    client.reset_mock()
    sftp.reset_mock()
    f_legacy = LegacySFTPFile(b"legacy\n")
    sftp.file.return_value = f_legacy
    with OpenAsReader("sftp://u:p@host/legacy.txt") as r:
        assert r.read() == "legacy\n"
    assert f_legacy.readv_calls == [[(0, 7)]]

    # 3. Close error path still closes _raw_fh, _sftp_file, _sftp_client, and _ssh_client and remains idempotent
    client.reset_mock()
    sftp.reset_mock()
    f_err = BoundedSFTPFile(b"err\n")
    sftp.file.return_value = f_err
    r_err = OpenAsReader("sftp://u:p@host/err.txt").open()
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

    with OpenAsWriter(f"sftp://u:p@host/dir/out{ext}").open(append=append, backup=False) as writer:
        writer.write("pipelined line\n")

    sftp.file.assert_called_once_with(f"/dir/out{ext}", expected_mode)
    assert buf.pipelined_calls == [True]
    assert buf.closed
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1
    assert _decompress_bytes(buf.saved_bytes, ext) == f"pipelined line{os.linesep}"


@patch("paramiko.SSHClient")
def test_sftp_writer_close_idempotent_and_error_paths(mock_ssh):
    """p1f3dc E-01(c): OpenAsWriter.close() idempotently closes all SFTP/SSH handles including on error paths."""
    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    # 1. Normal idempotent close
    sftp_file = MagicMock()
    sftp.file.return_value = sftp_file
    writer = OpenAsWriter("sftp://u:p@host/plain.txt").open(backup=False)
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
    writer_err = OpenAsWriter("sftp://u:p@host/out.gz").open(backup=False)
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
        OpenAsWriter("sftp://u:p@host/bad.txt").open(backup=False)
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
        with OpenAsReader(url) as r1:
            assert list(r1) == ["hello cookie-free\n"]
        assert len(_get_http_session().cookies) == 0
        with OpenAsReader(url) as r2:
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
    with OpenAsReader("https://example.com/early.txt") as reader:
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

    with OpenAsReader(f"https://example.com/file{ext or '.txt'}") as reader:
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
    from openas import UnsupportedProtocolError

    host = MagicMock()
    mock_host.return_value = host
    text = f"ftp real contract {ext} café\nline 2\n"

    # Write with strict FTPFile proxy
    write_proxy = _StrictFTPFileProxy()
    host.open.return_value = write_proxy
    with openas(f"ftp://user:pass@host:2121/dir/out{ext or '.txt'}", "w", backup=False, timeout=5.0) as w:
        n = w.write(text)
        assert n == len(text)
    assert write_proxy.closed
    assert mock_host.call_args.kwargs["port"] == 2121
    assert mock_host.call_args.kwargs["timeout"] == 5.0
    expected_wire_text = text.replace("\n", os.linesep)
    if ext:
        assert _decompress_bytes(write_proxy.saved_bytes, ext) == expected_wire_text
    else:
        assert write_proxy.saved_bytes.decode("utf-8") == expected_wire_text

    # Read back with strict FTPFile proxy (both show_progress=False and True)
    for show_progress in (False, True):
        read_proxy = _StrictFTPFileProxy(write_proxy.saved_bytes)
        host.open.return_value = read_proxy
        host.path.getsize.side_effect = OSError("SIZE unsupported")
        with openas(f"ftp://user:pass@host/dir/out{ext or '.txt'}", "r", show_progress=show_progress) as r:
            assert r.read() == text
            if show_progress:
                assert r._progress is not None and r._progress.total is None
        assert read_proxy.closed

    # FTP append fails fast BEFORE connecting
    mock_host.reset_mock()
    with pytest.raises(UnsupportedProtocolError, match="append"):
        openas("ftp://user:pass@host/dir/out.txt", "a")
    assert not mock_host.called


def test_ftp_session_wrapper_port_timeout_and_login_failure_closes_socket(monkeypatch):
    """S2-MEM1 & S3-T1: FTPSessionWrapper forwards port/timeout and closes socket if login() raises."""
    import ftplib
    from openas.openas import FTPSessionWrapper

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
        OpenAsReader("sftp://alice:pw@host/a.txt", timeout=4.0).open()
    assert client.close.call_count == 1
    assert client.connect.call_args.kwargs["timeout"] == 4.0
    assert client.connect.call_args.kwargs["banner_timeout"] == 4.0
    assert client.connect.call_args.kwargs["auth_timeout"] == 4.0

    # 2. Missing hostname raises ValueError before connect
    client.reset_mock()
    client.connect.side_effect = None
    with pytest.raises(ValueError, match="Missing hostname"):
        OpenAsReader("sftp:///no_host.txt").open()
    assert not client.connect.called

    # 3. Percent-decoded credentials/path + uncompressed SFTP write() returns int even when raw write returns None
    sftp = MagicMock()
    client.open_sftp.return_value = sftp
    raw_write = _StrictFTPFileProxy()
    sftp.file.return_value = raw_write
    with openas("sftp://user%40org:p%3As%2Fw@host/my%20dir/out.txt", "w", backup=False, timeout=9.0) as w:
        written = w.write("café\n")
        assert written == 5
    assert client.connect.call_args.kwargs["username"] == "user@org"
    assert client.connect.call_args.kwargs["password"] == "p:s/w"
    assert client.connect.call_args.kwargs["timeout"] == 9.0
    sftp.file.assert_called_once_with("/my dir/out.txt", "wb")
    assert raw_write.saved_bytes == f"café{os.linesep}".encode("utf-8")

    # 4. SFTP show_progress=True when stat() raises OSError degrades to total=None instead of crashing
    class StatFailSFTPFile(io.BytesIO):
        def stat(self):
            raise OSError("FSTAT unsupported")

    sftp.file.return_value = StatFailSFTPFile(b"ok\n")
    with OpenAsReader("sftp://u:p@host/data.txt", show_progress=True) as r:
        assert r._progress is not None
        assert r._progress.total is None
        assert r.read() == "ok\n"

    # 5. SFTP backup rename collision retries next free slot; missing source is a no-op
    sftp.reset_mock()
    sftp.stat.side_effect = FileNotFoundError("no file")
    OpenAsWriter("sftp://u:p@host/missing.txt")._ssh_backup_if(sftp, "/missing.txt")
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
    OpenAsWriter("sftp://u:p@host/dir/x.txt")._ssh_backup_if(sftp, "/dir/x.txt")
    assert rename_calls == [("/dir/x.txt", "/dir/x.txt.001.bu"), ("/dir/x.txt", "/dir/x.txt.002.bu")]


@patch("requests.Session.get")
def test_http_accept_encoding_content_encoding_timeout_and_raw_fallbacks(mock_get):
    """S2-B7, S2-S2, S3-T1: Compressed HTTP sends Accept-Encoding: identity; Content-Encoding nulls total; raw fallbacks."""
    gz_bytes = gzip.compress("http gz café\n".encode("utf-8"))
    resp_gz = MagicMock()
    resp_gz.headers = {"Content-Length": str(len(gz_bytes))}
    resp_gz.raw = io.BytesIO(gz_bytes)
    mock_get.return_value = resp_gz

    with OpenAsReader("https://example.com/archive.txt.gz", show_progress=True, timeout=6.0) as r:
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

    with OpenAsReader("https://example.com/plain.txt", show_progress=True) as r:
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
        with OpenAsReader(f"http://127.0.0.1:{port}/data.txt.gz", show_progress=True, timeout=5.0) as r:
            assert r.read() == payload_text
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    assert seen_accept == ["identity"]


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_remote_binary_mode_http_ftp_and_sftp(ext):
    """Binary modes (rb/wb/ab) stream exact non-UTF-8 bytes across HTTP, FTP, and SFTP for all codecs."""
    from openas import UnsupportedProtocolError

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
        with openas(f"https://example.com/blob{ext or '.bin'}", "rb", show_progress=True) as r:
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
        with openas(f"ftp://user:pass@host/dir/blob{ext or '.bin'}", "wb", backup=False) as w:
            assert w.encoding is None
            assert w.write(part1[0]) == len(part1[0])
            w.writelines([part1[1], part2])
            with pytest.raises(TypeError):
                w.write("str rejected on ftp binary writer")
        assert write_proxy.closed

        read_proxy = _StrictFTPFileProxy(write_proxy.saved_bytes)
        host.open.return_value = read_proxy
        host.path.getsize.return_value = len(write_proxy.saved_bytes)
        with openas(f"ftp://user:pass@host/dir/blob{ext or '.bin'}", "rb", show_progress=True) as r:
            assert r.encoding is None
            pbar = r._progress
            assert list(r) == part1 + [part2]
        assert pbar.n == len(write_proxy.saved_bytes)
        assert read_proxy.closed

        mock_ftp.reset_mock()
        with pytest.raises(UnsupportedProtocolError, match="append"):
            openas(f"ftp://user:pass@host/dir/blob{ext or '.bin'}", "ab")
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
        with openas(f"sftp://u:p@host/dir/blob{ext or '.bin'}", "wb", backup=False) as w:
            assert w.encoding is None
            w.writelines(part1)
            with pytest.raises(TypeError):
                w.write("str rejected on sftp binary writer")

        # Append part2 (multi-member stream for compressed formats, raw bytes for plain)
        w2_proxy = _StrictFTPFileProxy()
        sftp.file.return_value = w2_proxy
        with openas(f"sftp://u:p@host/dir/blob{ext or '.bin'}", "ab") as w:
            assert w.encoding is None
            assert w.write(part2) == len(part2)

        combined_wire = w1_proxy.saved_bytes + w2_proxy.saved_bytes
        r_proxy = _StrictFTPFileProxy(combined_wire)
        sftp.file.return_value = r_proxy
        with openas(f"sftp://u:p@host/dir/blob{ext or '.bin'}", "rb", show_progress=True) as r:
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
        with openas("sftp://u@h/dir/data%2Egz", "w", backup=False) as w:
            w.write(payload)
        sftp.file.assert_called_once_with("/dir/data.gz", "wb")
        assert write_buf.closed
        assert gzip.decompress(write_buf.saved_bytes).decode("utf-8") == payload.replace("\n", os.linesep)

        sftp.reset_mock()
        read_buf = io.BytesIO(write_buf.saved_bytes)
        sftp.file.return_value = read_buf
        with openas("sftp://u@h/dir/data%2Egz", "r") as r:
            assert r.read() == payload
        sftp.file.assert_called_once_with("/dir/data.gz", "rb")

    gz_wire = gzip.compress(payload.encode("utf-8"))
    with patch("requests.Session.get") as mock_get:
        resp = MagicMock()
        resp.headers = {"Content-Length": str(len(gz_wire))}
        resp.raw = io.BytesIO(gz_wire)
        mock_get.return_value = resp
        with openas("https://h/dir/data%2Egz", "r") as r:
            assert r.read() == payload
        assert mock_get.call_args.kwargs["headers"] == {"Accept-Encoding": "identity"}
        assert resp.close.called


@patch("paramiko.SSHClient")
def test_sftp_backup_rejects_remote_directory_and_closes_handles(mock_ssh):
    """iev841 E-02: SFTP backup=True raises IsADirectoryError on existing remote directories without renaming."""
    import stat

    for opener in (
        lambda url: openas(url, "w", backup=True),
        lambda url: OpenAsWriter(url).open(backup=True),
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
        lambda url: openas(url, "w", backup=True),
        lambda url: OpenAsWriter(url).open(backup=True),
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


@patch("paramiko.SSHClient")
def test_sftp_bounded_readv_multi_window_early_break_truncation_and_compressed_teardown(mock_ssh):
    """a224p3 E-01: Bounded 4 MiB readv windows, sub-chunking, early break, truncation, and compressed close."""
    from openas.openas import _BoundedSFTPReadStream

    window_bytes = 4 * 1024 * 1024
    max_req = 32768

    class WindowedSFTPFile:
        MAX_REQUEST_SIZE = max_req

        def __init__(self, payload: bytes, reported_size=None):
            self._payload = payload
            self._reported_size = len(payload) if reported_size is None else reported_size
            self.closed = False
            self.close_count = 0
            self.prefetch_calls = 0
            self.readv_windows = []
            self.gen_closed_count = 0

        def stat(self):
            st = MagicMock()
            st.st_size = self._reported_size
            return st

        def prefetch(self, *args, **kwargs):
            self.prefetch_calls += 1
            raise AssertionError("unbounded prefetch() must not be called")

        def readv(self, chunks, max_concurrent_prefetch_requests=None):
            chunk_list = list(chunks)
            self.readv_windows.append((chunk_list, max_concurrent_prefetch_requests))

            def _gen():
                try:
                    for offset, length in chunk_list:
                        yield self._payload[offset:offset + length]
                finally:
                    self.gen_closed_count += 1

            return _gen()

        def close(self):
            self.close_count += 1
            self.closed = True

    client = MagicMock()
    sftp = MagicMock()
    mock_ssh.return_value = client
    client.open_sftp.return_value = sftp

    # 1. Multi-window (>4 MiB) SFTP file: bounded 4 MiB windows split into <= MAX_REQUEST_SIZE sub-chunks
    line = (b"A" * 63) + b"\n"
    total_lines = (window_bytes // len(line)) + 1024  # ~4.06 MiB across 2 windows
    payload = line * total_lines
    assert len(payload) > window_bytes

    f_multi = WindowedSFTPFile(payload)
    sftp.file.return_value = f_multi
    with OpenAsReader("sftp://u:p@host/multi.bin", binary=True, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert pbar.total == len(payload)
        got = b"".join(r)
        assert pbar.n == len(payload)
    assert got == payload
    assert f_multi.prefetch_calls == 0
    assert len(f_multi.readv_windows) == 2
    for chunks, conc in f_multi.readv_windows:
        assert conc == 64
        assert sum(length for _, length in chunks) <= window_bytes
        assert all(0 < length <= max_req for _, length in chunks)
    assert f_multi.readv_windows[0][0][0] == (0, max_req)
    assert f_multi.readv_windows[1][0][0][0] == window_bytes
    assert f_multi.closed and f_multi.close_count == 1
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1

    # 2. Early break after reading 1 line inside window 1 never requests window 2 and closes generator + handles
    client.reset_mock()
    sftp.reset_mock()
    f_early = WindowedSFTPFile(payload)
    sftp.file.return_value = f_early
    seen = []
    with OpenAsReader("sftp://u:p@host/multi.txt") as r:
        for row in r:
            seen.append(row)
            break
    assert seen == [("A" * 63) + "\n"]
    assert len(f_early.readv_windows) == 1
    assert f_early.gen_closed_count == 1
    assert f_early.closed and f_early.close_count == 1
    assert sftp.close.call_count == 1
    assert client.close.call_count == 1

    # 3. Truncated remote file stops cleanly at early EOF without requesting window 2
    client.reset_mock()
    sftp.reset_mock()
    truncated_payload = b"short-line-1\nshort-line-2\n"
    f_trunc = WindowedSFTPFile(truncated_payload, reported_size=window_bytes * 2)
    sftp.file.return_value = f_trunc
    with OpenAsReader("sftp://u:p@host/trunc.txt") as r:
        assert list(r) == ["short-line-1\n", "short-line-2\n"]
    assert len(f_trunc.readv_windows) == 1
    assert f_trunc.closed and f_trunc.close_count == 1

    # 4. Compressed SFTP streams (.gz, .bz2, .xz, .zst) deterministically close _BoundedSFTPReadStream and sftp_file
    sample_text = "sftp compressed line 1\nsftp café line 2\n"
    sample_bytes = sample_text.encode("utf-8")
    compressed_cases = [
        (".gz", gzip.compress(sample_bytes)),
        (".bz2", bz2.compress(sample_bytes)),
        (".xz", lzma.compress(sample_bytes)),
        (".zst", zstd.ZstdCompressor().compress(sample_bytes)),
    ]
    for ext, wire in compressed_cases:
        for binary in (False, True):
            client.reset_mock()
            sftp.reset_mock()
            f_comp = WindowedSFTPFile(wire)
            sftp.file.return_value = f_comp
            with OpenAsReader(f"sftp://u:p@host/data{ext}", binary=binary, show_progress=True) as r:
                raw_stream = r._raw_fh
                assert isinstance(raw_stream, _BoundedSFTPReadStream)
                pbar = r._progress
                assert pbar is not None and pbar.total == len(wire)
                got_data = list(r)
                assert pbar.n == len(wire)
            expected = [b"sftp compressed line 1\n", "sftp café line 2\n".encode("utf-8")] if binary else [
                "sftp compressed line 1\n",
                "sftp café line 2\n",
            ]
            assert got_data == expected
            assert raw_stream.closed
            assert f_comp.closed and f_comp.close_count == 1
            assert sftp.close.call_count == 1
            assert client.close.call_count == 1

    # 5. Closed _BoundedSFTPReadStream raises ValueError on read/read1/readinto
    f_closed = WindowedSFTPFile(b"abcdef")
    stream = _BoundedSFTPReadStream(f_closed, 6)
    assert stream.read1(2) == b"ab"
    buf = bytearray(2)
    assert stream.readinto(buf) == 2
    assert bytes(buf) == b"cd"
    stream.close()
    assert stream.closed
    assert f_closed.close_count == 1
    for op in (lambda: stream.read(), lambda: stream.read1(), lambda: stream.readinto(bytearray(1))):
        with pytest.raises(ValueError):
            op()


def test_http_real_server_all_formats_text_and_binary():
    """was3ig E-01: Real loopback ThreadingHTTPServer verifies all 5 formats in 'r' and 'rb' modes."""
    text_payload = "server line 1 café\nserver line 2 日本語\n"
    text_bytes = text_payload.encode("utf-8")
    bin_payload = b"\x00\xff\xfe\x80bin-line-1\n\xde\xad\xbe\xefbin-line-2\n"

    def _encode(raw: bytes, ext: str) -> bytes:
        if ext == ".gz":
            return gzip.compress(raw)
        if ext == ".bz2":
            return bz2.compress(raw)
        if ext == ".xz":
            return lzma.compress(raw)
        if ext == ".zst":
            return zstd.ZstdCompressor().compress(raw)
        return raw

    routes = {}
    for ext in (".txt", ".gz", ".bz2", ".xz", ".zst"):
        routes[f"/text{ext}"] = _encode(text_bytes, ext)
        routes[f"/bin{ext}"] = _encode(bin_payload, ext)

    seen_accept = {}

    class MultiFormatHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            seen_accept[self.path] = self.headers.get("Accept-Encoding")
            body = routes[self.path]
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: A002
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), MultiFormatHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        for ext in (".txt", ".gz", ".bz2", ".xz", ".zst"):
            text_url = f"http://127.0.0.1:{port}/text{ext}"
            with openas(text_url, "r", show_progress=True, timeout=5.0) as r:
                pbar = r._progress
                assert pbar is not None
                assert pbar.total == len(routes[f"/text{ext}"])
                assert r.read() == text_payload
            assert r.closed is True
            assert r._response is None
            assert pbar.n == len(routes[f"/text{ext}"])

            bin_url = f"http://127.0.0.1:{port}/bin{ext}"
            with openas(bin_url, "rb", show_progress=True, timeout=5.0) as r:
                pbar = r._progress
                assert pbar is not None
                assert pbar.total == len(routes[f"/bin{ext}"])
                assert r.read() == bin_payload
            assert r.closed is True
            assert r._response is None
            assert pbar.n == len(routes[f"/bin{ext}"])

            if ext == ".txt":
                assert seen_accept[f"/text{ext}"] != "identity"
            else:
                assert seen_accept[f"/text{ext}"] == "identity"
                assert seen_accept[f"/bin{ext}"] == "identity"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_http_real_server_http11_chunked_gzip_encoding_and_404():
    """was3ig E-01: HTTP/1.1 handler tests chunked transfer, Content-Encoding: gzip, and 404 cleanup."""
    chunked_parts = ["chunk-one café\n".encode("utf-8"), b"chunk-two\n", b"chunk-three\n"]
    chunked_full = b"".join(chunked_parts)
    encoded_plain = "gzip-encoded line 1 café\ngzip-encoded line 2\n"
    encoded_gz = gzip.compress(encoded_plain.encode("utf-8"))

    class Http11Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            if self.path == "/chunked.txt":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Transfer-Encoding", "chunked")
                self.send_header("Connection", "close")
                self.end_headers()
                for part in chunked_parts:
                    self.wfile.write(f"{len(part):x}\r\n".encode("ascii") + part + b"\r\n")
                self.wfile.write(b"0\r\n\r\n")
                self.close_connection = True
            elif self.path == "/encoded.txt":
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(encoded_gz)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(encoded_gz)
                self.close_connection = True
            else:
                body = b"not found"
                self.send_response(404)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)
                self.close_connection = True

        def log_message(self, format, *args):  # noqa: A002
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Http11Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        # 1. Transfer-Encoding: chunked (total=None) in both 'r' and 'rb'
        for mode, expected in (("r", chunked_full.decode("utf-8")), ("rb", chunked_full)):
            with openas(f"http://127.0.0.1:{port}/chunked.txt", mode, show_progress=True, timeout=5.0) as r:
                pbar = r._progress
                assert pbar is not None
                assert pbar.total is None
                assert r.read() == expected
            assert r.closed is True
            assert r._response is None
            assert pbar.n == len(chunked_full)

        # 2. Content-Encoding: gzip on plain .txt (decode_content=True, total=None) in 'r' and 'rb'
        for mode, expected in (("r", encoded_plain), ("rb", encoded_plain.encode("utf-8"))):
            with openas(f"http://127.0.0.1:{port}/encoded.txt", mode, show_progress=True, timeout=5.0) as r:
                pbar = r._progress
                assert r._response is not None
                assert r._response.raw.decode_content is True
                assert pbar is not None
                assert pbar.total is None
                assert r.read() == expected
            assert r.closed is True
            assert r._response is None

        # 3. HTTP 404 cleanup: HTTPError raised, reader closed, _response None, response.close() called
        closed_statuses = []
        orig_resp_close = requests.Response.close

        def spy_resp_close(self):
            closed_statuses.append(self.status_code)
            return orig_resp_close(self)

        with patch.object(requests.Response, "close", spy_resp_close):
            reader_404 = OpenAsReader(f"http://127.0.0.1:{port}/missing.txt", timeout=5.0)
            with pytest.raises(requests.HTTPError):
                reader_404.open()
            assert reader_404.closed is True
            assert reader_404._response is None
            assert 404 in closed_statuses
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_close_registered_http_sessions_isolated_and_exception_safe(monkeypatch):
    """was3ig E-01: _close_registered_http_sessions closes all registered sessions despite close() errors without touching live session."""
    import sys

    openas_mod = sys.modules["openas.openas"]

    live_session = openas_mod._get_http_session()

    class FakeSession:
        def __init__(self, *, should_raise: bool = False):
            self.should_raise = should_raise
            self.close_calls = 0
            self.closed = False

        def close(self):
            self.close_calls += 1
            if self.should_raise:
                raise RuntimeError("session close failure")
            self.closed = True

    s1 = FakeSession()
    s2 = FakeSession(should_raise=True)
    s3 = FakeSession()

    monkeypatch.setattr(openas_mod, "_http_sessions_registry", [s1, s2, s3])
    openas_mod._close_registered_http_sessions()

    assert s1.closed is True and s1.close_calls == 1
    assert s2.close_calls == 1 and s2.closed is False
    assert s3.closed is True and s3.close_calls == 1
    assert openas_mod._http_sessions_registry == []
    assert openas_mod._get_http_session() is live_session


def test_keyboard_interrupt_mid_open_cleans_up_remote_handles():
    """was3ig E-03: KeyboardInterrupt (BaseException) mid-open closes acquired HTTP, FTP, and SFTP handles."""
    # 1. OpenAsReader HTTP: KeyboardInterrupt in raise_for_status() and in post-wrap _get_file_size()
    with patch("requests.Session.get") as mock_get:
        resp1 = MagicMock()
        resp1.raise_for_status.side_effect = KeyboardInterrupt("ctrl-c status")
        mock_get.return_value = resp1
        r_http1 = OpenAsReader("https://example.com/a.txt")
        with pytest.raises(KeyboardInterrupt, match="ctrl-c status"):
            r_http1.open()
        assert resp1.close.call_count == 1
        assert r_http1.closed is True
        assert r_http1._response is None

        resp2 = MagicMock()
        resp2.headers = {"Content-Length": "5"}
        resp2.raw = io.BytesIO(b"data\n")
        mock_get.return_value = resp2
        r_http2 = OpenAsReader("https://example.com/b.txt", show_progress=True)
        with patch.object(r_http2, "_get_file_size", side_effect=KeyboardInterrupt("ctrl-c progress")):
            with pytest.raises(KeyboardInterrupt, match="ctrl-c progress"):
                r_http2.open()
        assert resp2.raw.closed
        assert resp2.close.call_count == 1
        assert r_http2.closed is True
        assert r_http2._response is None

    # 2. OpenAsReader FTP: KeyboardInterrupt after FTPHost and FTPFile are acquired
    with patch("ftputil.FTPHost") as mock_ftp:
        host = MagicMock()
        raw_ftp = _StrictFTPFileProxy(b"ftp-data\n")
        host.open.return_value = raw_ftp
        mock_ftp.return_value = host
        r_ftp = OpenAsReader("ftp://u:p@host/dir/file.txt", show_progress=True)
        with patch.object(r_ftp, "_get_file_size", side_effect=KeyboardInterrupt("ctrl-c ftp reader")):
            with pytest.raises(KeyboardInterrupt, match="ctrl-c ftp reader"):
                r_ftp.open()
        assert raw_ftp.closed is True
        assert host.close.call_count == 1
        assert r_ftp.closed is True
        assert r_ftp._ftp is None
        assert r_ftp._raw_fh is None

    # 3. OpenAsReader SFTP: KeyboardInterrupt after client, sftp, and sftp_file are acquired
    with patch("paramiko.SSHClient") as mock_ssh:
        client = MagicMock()
        sftp = MagicMock()
        sftp_file = MagicMock()
        sftp_file.stat.side_effect = KeyboardInterrupt("ctrl-c sftp reader")
        mock_ssh.return_value = client
        client.open_sftp.return_value = sftp
        sftp.file.return_value = sftp_file
        r_sftp = OpenAsReader("sftp://u:p@host/dir/file.txt")
        with pytest.raises(KeyboardInterrupt, match="ctrl-c sftp reader"):
            r_sftp.open()
        assert sftp_file.close.call_count == 1
        assert sftp.close.call_count == 1
        assert client.close.call_count == 1
        assert r_sftp.closed is True
        assert r_sftp._sftp_file is None
        assert r_sftp._sftp_client is None
        assert r_sftp._ssh_client is None

    # 4. OpenAsWriter FTP: KeyboardInterrupt after FTPHost and FTPFile are acquired
    with patch("ftputil.FTPHost") as mock_ftp:
        host = MagicMock()
        raw_ftp = _StrictFTPFileProxy()
        host.open.return_value = raw_ftp
        mock_ftp.return_value = host
        w_ftp = OpenAsWriter("ftp://u:p@host/dir/out.gz")
        with patch("gzip.open", side_effect=KeyboardInterrupt("ctrl-c ftp writer")):
            with pytest.raises(KeyboardInterrupt, match="ctrl-c ftp writer"):
                w_ftp.open(backup=False)
        assert raw_ftp.closed is True
        assert host.close.call_count == 1
        assert w_ftp.closed is True
        assert w_ftp._ftp is None
        assert w_ftp._raw_fh is None

    # 5. OpenAsWriter SFTP: KeyboardInterrupt after client, sftp, and sftp_file are acquired
    with patch("paramiko.SSHClient") as mock_ssh:
        client = MagicMock()
        sftp = MagicMock()
        sftp_file = MagicMock()
        sftp_file.set_pipelined.side_effect = KeyboardInterrupt("ctrl-c sftp writer")
        mock_ssh.return_value = client
        client.open_sftp.return_value = sftp
        sftp.file.return_value = sftp_file
        w_sftp = OpenAsWriter("sftp://u:p@host/dir/out.txt")
        with pytest.raises(KeyboardInterrupt, match="ctrl-c sftp writer"):
            w_sftp.open(backup=False)
        assert sftp_file.close.call_count == 1
        assert sftp.close.call_count == 1
        assert client.close.call_count == 1
        assert w_sftp.closed is True
        assert w_sftp._sftp_file is None
        assert w_sftp._sftp_client is None
        assert w_sftp._ssh_client is None


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_remote_truncated_zst_raises_and_closes_handles_across_http_ftp_and_sftp(mode):
    """hxile6: Truncated .zst streams raise EOFError and close handles cleanly across HTTP, FTP, and SFTP."""
    sample = ("remote truncated zst payload line 0123456789abcdef\n" * 200).encode("utf-8")
    full_wire = zstd.ZstdCompressor(write_checksum=True).compress(sample)
    trunc_wire = full_wire[: max(10, len(full_wire) // 2)]

    # 1. HTTP via real local ThreadingHTTPServer
    class TruncZstHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(trunc_wire)))
            self.end_headers()
            self.wfile.write(trunc_wire)

        def log_message(self, format, *args):  # noqa: A002
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), TruncZstHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        reader = OpenAsReader(f"http://127.0.0.1:{port}/trunc.zst", show_progress=True, binary=(mode == "rb"))
        with pytest.raises((EOFError, zstd.ZstdError)):
            with reader:
                reader.read()
        assert reader.closed is True
        assert reader._response is None
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    # 2. FTP via strict FTPFile proxy
    with patch("ftputil.FTPHost") as mock_ftp:
        host = MagicMock()
        read_proxy = _StrictFTPFileProxy(trunc_wire)
        host.open.return_value = read_proxy
        host.path.getsize.return_value = len(trunc_wire)
        mock_ftp.return_value = host
        reader_ftp = OpenAsReader("ftp://u:p@host/dir/trunc.zst", show_progress=True, binary=(mode == "rb"))
        with pytest.raises((EOFError, zstd.ZstdError)):
            with reader_ftp:
                reader_ftp.read()
        assert read_proxy.closed is True
        assert host.close.call_count == 1
        assert reader_ftp.closed is True

    # 3. SFTP via both sequential fallback and readv bounded stream
    class _ReadvSFTPFile(io.BytesIO):
        def __init__(self, data: bytes):
            super().__init__(data)
            self._data = data

        def stat(self):
            st = MagicMock()
            st.st_size = len(self._data)
            return st

        def readv(self, chunks, max_concurrent_prefetch_requests=None):
            for offset, length in chunks:
                yield self._data[offset : offset + length]

    with patch("paramiko.SSHClient") as mock_ssh:
        for make_file in (lambda: _StrictFTPFileProxy(trunc_wire), lambda: _ReadvSFTPFile(trunc_wire)):
            client = MagicMock()
            sftp = MagicMock()
            sftp_file = make_file()
            mock_ssh.return_value = client
            client.open_sftp.return_value = sftp
            sftp.file.return_value = sftp_file

            reader_sftp = OpenAsReader("sftp://u:p@host/dir/trunc.zst", show_progress=True, binary=(mode == "rb"))
            with pytest.raises((EOFError, zstd.ZstdError)):
                with reader_sftp:
                    reader_sftp.read()
            assert sftp_file.closed is True
            assert sftp.close.call_count == 1
            assert client.close.call_count == 1
            assert reader_sftp.closed is True


def test_semicolon_url_paths_preserved_across_sftp_and_http_and_empty_http_path_allowed():
    """kfvf7i E-01: Semicolons stay part of URL paths (urlsplit) and empty HTTP/HTTPS paths remain allowed."""
    payload = "semicolon path line 1\nline 2\n"
    gz_wire = gzip.compress(payload.encode("utf-8"))

    # 1. SFTP write & read with semicolon in filename uses full path and gzip codec
    with patch("paramiko.SSHClient") as mock_ssh:
        client = MagicMock()
        sftp = MagicMock()
        mock_ssh.return_value = client
        client.open_sftp.return_value = sftp

        write_buf = _InspectableBytesIO()
        sftp.file.return_value = write_buf
        with openas("sftp://user@host/dir/a;b.gz", "w", backup=False) as w:
            w.write(payload)
        sftp.file.assert_called_once_with("/dir/a;b.gz", "wb")
        assert gzip.decompress(write_buf.saved_bytes).decode("utf-8") == payload.replace("\n", os.linesep)

        sftp.reset_mock()
        sftp.file.return_value = io.BytesIO(write_buf.saved_bytes)
        with openas("sftp://user@host/dir/a;b.gz", "r") as r:
            assert r.read() == payload
        sftp.file.assert_called_once_with("/dir/a;b.gz", "rb")

    # 2. HTTPS with semicolon in filename selects gzip and sends Accept-Encoding: identity (both open() and _wrap_http(parsed=None))
    with patch("requests.Session.get") as mock_get:
        resp = MagicMock()
        resp.headers = {"Content-Length": str(len(gz_wire))}
        resp.raw = io.BytesIO(gz_wire)
        mock_get.return_value = resp
        with openas("https://example.com/file;v=1.gz", "r") as r:
            assert r.read() == payload
        assert mock_get.call_args.kwargs["headers"] == {"Accept-Encoding": "identity"}

        mock_get.reset_mock()
        resp2 = MagicMock()
        resp2.headers = {"Content-Length": str(len(gz_wire))}
        resp2.raw = io.BytesIO(gz_wire)
        mock_get.return_value = resp2
        direct_reader = OpenAsReader("https://example.com/file;v=1.gz")
        try:
            direct_reader._wrap_http(parsed=None)
            assert direct_reader.read() == payload
        finally:
            direct_reader.close()
        assert mock_get.call_args.kwargs["headers"] == {"Accept-Encoding": "identity"}

    # 3. HTTP and HTTPS empty paths remain allowed
    for url in ("http://example.com", "https://example.com"):
        with patch("requests.Session.get") as mock_get:
            resp = MagicMock()
            resp.headers = {"Content-Length": "3"}
            resp.raw = io.BytesIO(b"ok\n")
            mock_get.return_value = resp
            with openas(url, "r") as r:
                assert r.read() == "ok\n"
            mock_get.assert_called_once_with(url, stream=True)


def test_exited_thread_http_session_reclaimed_and_closed_after_gc():
    """kfvf7i E-03: Exited worker thread's HTTP session is closed and removed from _http_sessions_registry after gc.collect()."""
    import gc
    import sys

    openas_mod = sys.modules["openas.openas"]
    main_session = openas_mod._get_http_session()
    assert any(s is main_session for s in openas_mod._http_sessions_registry)

    closed_count = []
    worker_session_ids = []

    def worker():
        sess = openas_mod._get_http_session()
        orig_close = sess.close

        def spy_close():
            closed_count.append(id(sess))
            return orig_close()

        sess.close = spy_close
        worker_session_ids.append(id(sess))
        assert any(s is sess for s in openas_mod._http_sessions_registry)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=5)
    assert not t.is_alive()

    gc.collect()
    gc.collect()

    assert len(worker_session_ids) == 1
    assert closed_count == worker_session_ids
    assert all(id(s) != worker_session_ids[0] for s in openas_mod._http_sessions_registry)
    assert any(s is main_session for s in openas_mod._http_sessions_registry)


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_remote_text_encoding_errors_and_newline_http_ftp_sftp(ext):
    """ckmclg E-01: HTTP, FTP, and SFTP forward encoding, errors, and newline on reads and writes."""
    latin_text = "café\nnaïve\n"
    latin_lf_bytes = latin_text.encode("latin-1")
    dirty_bytes = b"good-\xff-end\r\n"

    def _encode(raw: bytes) -> bytes:
        if ext == ".gz":
            return gzip.compress(raw)
        if ext == ".bz2":
            return bz2.compress(raw)
        if ext == ".xz":
            return lzma.compress(raw)
        if ext == ".zst":
            return zstd.ZstdCompressor().compress(raw)
        return raw

    # 1. HTTP read with encoding='latin-1', errors='replace', and newline=''
    with patch("requests.Session.get") as mock_get:
        resp_latin = MagicMock()
        resp_latin.headers = {}
        resp_latin.raw = io.BytesIO(_encode(latin_lf_bytes))
        mock_get.return_value = resp_latin
        with openas(f"https://example.com/doc{ext or '.txt'}", "r", encoding="latin-1") as r:
            assert r.encoding == "latin-1"
            assert r.read() == latin_text

        resp_dirty = MagicMock()
        resp_dirty.headers = {}
        resp_dirty.raw = io.BytesIO(_encode(dirty_bytes))
        mock_get.return_value = resp_dirty
        with openas(
            f"https://example.com/dirty{ext or '.txt'}", "r", encoding="utf-8", errors="replace", newline=""
        ) as r:
            assert r.read() == "good-\ufffd-end\r\n"

    # 2. FTP write with encoding='latin-1' + newline='\n' and read back with encoding='latin-1' / errors='replace'
    with patch("ftputil.FTPHost") as mock_ftp:
        host = MagicMock()
        mock_ftp.return_value = host
        write_proxy = _StrictFTPFileProxy()
        host.open.return_value = write_proxy
        with openas(
            f"ftp://u:p@host/dir/out{ext or '.txt'}", "w", backup=False, encoding="latin-1", newline="\n"
        ) as w:
            assert w.encoding == "latin-1"
            w.write(latin_text)
        assert write_proxy.closed
        if ext:
            if ext == ".gz":
                assert gzip.decompress(write_proxy.saved_bytes) == latin_lf_bytes
            elif ext == ".bz2":
                assert bz2.decompress(write_proxy.saved_bytes) == latin_lf_bytes
            elif ext == ".xz":
                assert lzma.decompress(write_proxy.saved_bytes) == latin_lf_bytes
            else:
                assert zstd.ZstdDecompressor().stream_reader(io.BytesIO(write_proxy.saved_bytes)).read() == latin_lf_bytes
        else:
            assert write_proxy.saved_bytes == latin_lf_bytes

        host.open.return_value = _StrictFTPFileProxy(write_proxy.saved_bytes)
        with openas(f"ftp://u:p@host/dir/out{ext or '.txt'}", "r", encoding="latin-1") as r:
            assert r.read() == latin_text

        host.open.return_value = _StrictFTPFileProxy(_encode(dirty_bytes))
        with openas(
            f"ftp://u:p@host/dir/dirty{ext or '.txt'}", "r", encoding="utf-8", errors="replace", newline=""
        ) as r:
            assert r.read() == "good-\ufffd-end\r\n"

    # 3. SFTP write with encoding='latin-1' + newline='\n' and read back
    with patch("paramiko.SSHClient") as mock_ssh:
        client = MagicMock()
        sftp = MagicMock()
        mock_ssh.return_value = client
        client.open_sftp.return_value = sftp

        sftp_write = _InspectableBytesIO()
        sftp.file.return_value = sftp_write
        with openas(
            f"sftp://u@host/dir/out{ext or '.txt'}", "w", backup=False, encoding="latin-1", newline="\n"
        ) as w:
            assert w.encoding == "latin-1"
            w.write(latin_text)
        assert sftp_write.closed

        sftp.file.return_value = io.BytesIO(sftp_write.saved_bytes)
        with openas(f"sftp://u@host/dir/out{ext or '.txt'}", "r", encoding="latin-1", newline="") as r:
            assert r.read() == latin_text

        sftp.file.return_value = io.BytesIO(_encode(dirty_bytes))
        with openas(
            f"sftp://u@host/dir/dirty{ext or '.txt'}", "r", encoding="utf-8", errors="replace", newline=""
        ) as r:
            assert r.read() == "good-\ufffd-end\r\n"


def test_http_custom_headers_accept_encoding_isolation_and_repr():
    """ckmclg E-03: Custom HTTP headers, Accept-Encoding preservation/override, defensive copy, session isolation, and repr."""
    plain_body = b"hello headers\n"
    gz_body = gzip.compress(b"hello gz headers\n")
    recorded = []

    class HeaderInspectHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            recorded.append(
                {
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                    "user-agent": self.headers.get("User-Agent"),
                    "accept-encoding": self.headers.get("Accept-Encoding"),
                    "x-custom": self.headers.get("X-Custom"),
                }
            )
            body = gz_body if self.path.endswith(".gz") else plain_body
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # noqa: A002
            return

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), HeaderInspectHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        # 1. Custom Authorization + User-Agent on compressed URL preserves Accept-Encoding: identity; caller dict not mutated
        caller_headers = {
            "Authorization": "Bearer super-secret-tok",
            "User-Agent": "openas-test/1.0",
        }
        reader = OpenAsReader(
            f"http://127.0.0.1:{port}/data.txt.gz",
            headers=caller_headers,
            timeout=5.0,
        )
        # Mutating caller's dict after __init__ must not affect the request
        caller_headers["X-Custom"] = "mutated-after-init"
        caller_headers["Authorization"] = "Bearer wrong"

        # repr() must never expose header names or values
        rep = repr(reader)
        assert "super-secret-tok" not in rep
        assert "Authorization" not in rep
        assert "openas-test/1.0" not in rep

        with reader:
            assert reader.read() == "hello gz headers\n"
        assert "Accept-Encoding" not in caller_headers
        assert recorded[-1]["authorization"] == "Bearer super-secret-tok"
        assert recorded[-1]["user-agent"] == "openas-test/1.0"
        assert recorded[-1]["accept-encoding"] == "identity"
        assert recorded[-1]["x-custom"] is None

        # 2. Subsequent request without headers on the same thread-local Session must not receive previous headers
        with openas(f"http://127.0.0.1:{port}/plain.txt", "r", timeout=5.0) as r2:
            assert r2.read() == "hello headers\n"
        assert recorded[-1]["authorization"] is None
        assert recorded[-1]["user-agent"] != "openas-test/1.0"

        # 3. Case-insensitive Accept-Encoding override on compressed URL
        with openas(
            f"http://127.0.0.1:{port}/override.txt.gz",
            "r",
            headers={"accept-encoding": "gzip, identity"},
            timeout=5.0,
        ) as r3:
            assert r3.read() == "hello gz headers\n"
        assert recorded[-1]["accept-encoding"] == "gzip, identity"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@patch("paramiko.SSHClient")
def test_ssh_key_filename_forwarding_omission_and_repr(mock_ssh, tmp_path):
    """ckmclg E-04: key_filename (str, Path, sequence) forwards to Paramiko connect(), omitted when None and in repr()."""
    from pathlib import Path

    key1 = tmp_path / "deploy_ed25519"
    key2 = tmp_path / "fallback_rsa"

    cases = [
        (None, None),
        (str(key1), str(key1)),
        (Path(key1), str(key1)),
        ([Path(key1), str(key2)], [str(key1), str(key2)]),
        ((Path(key1),), [str(key1)]),
    ]

    for passed, expected in cases:
        # Reader
        client = MagicMock()
        sftp = MagicMock()
        client.open_sftp.return_value = sftp
        sftp.file.return_value = io.BytesIO(b"ssh ok\n")
        mock_ssh.return_value = client

        reader = OpenAsReader("sftp://user@host/dir/in.txt", key_filename=passed)
        if passed is not None:
            assert str(key1) not in repr(reader)
        with reader:
            assert reader.read() == "ssh ok\n"
        connect_kwargs = client.connect.call_args.kwargs
        if expected is None:
            assert "key_filename" not in connect_kwargs
        else:
            assert connect_kwargs["key_filename"] == expected

        # Writer (via openas)
        client_w = MagicMock()
        sftp_w = MagicMock()
        buf_w = _InspectableBytesIO()
        client_w.open_sftp.return_value = sftp_w
        sftp_w.file.return_value = buf_w
        mock_ssh.return_value = client_w

        with openas("sftp://user@host/dir/out.txt", "w", backup=False, key_filename=passed) as w:
            if passed is not None:
                assert str(key1) not in repr(w)
            w.write("written\n")
        connect_kwargs_w = client_w.connect.call_args.kwargs
        if expected is None:
            assert "key_filename" not in connect_kwargs_w
        else:
            assert connect_kwargs_w["key_filename"] == expected

    # Defensive copy: mutating caller list after __init__ does not alter connect_kwargs
    key_list = [str(key1)]
    client_d = MagicMock()
    sftp_d = MagicMock()
    client_d.open_sftp.return_value = sftp_d
    sftp_d.file.return_value = io.BytesIO(b"defensive\n")
    mock_ssh.return_value = client_d
    reader_d = OpenAsReader("sftp://user@host/dir/in.txt", key_filename=key_list)
    key_list.append(str(key2))
    with reader_d:
        assert reader_d.read() == "defensive\n"
    assert client_d.connect.call_args.kwargs["key_filename"] == [str(key1)]
