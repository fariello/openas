import bz2
import gzip
import io
from urllib.parse import urlparse
from unittest.mock import patch, MagicMock
import pytest
import requests
from polyopen import polyopen, PolyReader, PolyWriter, UnsupportedArchiveError
from polyopen.polyopen import _normalized_path

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
