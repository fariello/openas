import os
import pytest
from openas import openas, UnsupportedArchiveError, ReadOnlyProtocolError, UnsupportedProtocolError

def test_exception_guards_archive(tmp_path):
    # Test Archive Guard Write
    with pytest.raises(UnsupportedArchiveError):
        openas(str(tmp_path / "test.zip"), 'w')
        
    # Test Archive Guard Read
    with pytest.raises(UnsupportedArchiveError):
        openas(str(tmp_path / "test.tar.gz"), 'r')

def test_exception_guards_http():       
    # Test Protocol Guard (HTTP write)
    with pytest.raises(ReadOnlyProtocolError):
        openas("http://example.com/data.txt", 'w')

def test_exception_guards_cloud():
    # Test Unsupported Cloud Protocols explicitly redirect developers
    with pytest.raises(UnsupportedProtocolError) as excinfo:
        openas("s3://bucket/data.csv", 'r')
    assert "boto3" in str(excinfo.value) or "smart_open" in str(excinfo.value)
    
def test_append_backup_conflict(tmp_path):
    from openas import OpenAsWriter
    # openas logic states you cannot append AND backup simultaneously
    with pytest.raises(ValueError):
        OpenAsWriter(str(tmp_path / "data.zst")).open(append=True, backup=True)

def test_writer_progress_exception(tmp_path):
    # Validates explicitly the newly structured parameter guard correctly fails.
    with pytest.raises(NotImplementedError):
        openas(str(tmp_path / "testing.zst"), 'w', show_progress=True)


def test_openas_append_backup_guard(tmp_path):
    """BUG-04: openas(path, 'a', backup=True) raises ValueError; default append skips backup."""
    target = tmp_path / "append_guard.txt"
    with openas(target, "w") as w:
        w.write("first\n")

    with pytest.raises(ValueError):
        openas(target, "a", backup=True)

    with openas(target, "a") as w:
        w.write("second\n")

    assert not os.path.exists(f"{target}.001.bu")
    with openas(target, "r") as r:
        assert r.read() == "first\nsecond\n"


def test_openas_writer_show_progress_signature_guard(tmp_path):
    """BUG-06a: OpenAsWriter accepts show_progress=False and rejects show_progress=True."""
    from openas import OpenAsWriter

    target = tmp_path / "writer_prog.txt"
    with OpenAsWriter(target, show_progress=False) as w:
        w.write("ok\n")
    with pytest.raises(NotImplementedError):
        OpenAsWriter(target, show_progress=True)


@pytest.mark.parametrize(
    "name",
    [
        "a.zip",
        "a.tar",
        "a.tgz",
        "a.rar",
        "a.7z",
        "a.tar.gz",
        "a.tar.bz2",
        "a.tar.xz",
        "a.tar.zst",
        "a.tar.lzma",
        "a.tbz",
        "a.tbz2",
        "a.txz",
        "a.tzst",
        "a.tlz",
        "a.TAR.XZ",
    ],
)
@pytest.mark.parametrize("mode", ["r", "w", "a"])
def test_archive_guards_comprehensive_and_redacted(tmp_path, name, mode):
    """S2-S4 & S2-S1: Extended archive suffixes fail fast across modes, redacting URL credentials."""
    with pytest.raises(UnsupportedArchiveError):
        openas(tmp_path / name, mode)

    with pytest.raises(UnsupportedArchiveError):
        openas((tmp_path / name).as_uri(), mode)

    with pytest.raises(UnsupportedArchiveError) as excinfo:
        openas(f"sftp://alice:topsecret@host/dir/{name}?token=xyz", mode)
    msg = str(excinfo.value)
    assert "topsecret" not in msg
    assert "token=xyz" not in msg
    assert "***" in msg


@pytest.mark.parametrize("scheme", ["http", "https"])
@pytest.mark.parametrize("mode", ["w", "a"])
def test_http_https_readonly_guards(scheme, mode):
    with pytest.raises(ReadOnlyProtocolError):
        openas(f"{scheme}://example.com/data.txt", mode)


@pytest.mark.parametrize("scheme", ["s3", "gs", "az", "hdfs"])
@pytest.mark.parametrize("mode", ["r", "w", "a"])
def test_cloud_schemes_rejected_all_modes(scheme, mode):
    with pytest.raises(UnsupportedProtocolError) as excinfo:
        openas(f"{scheme}://bucket/data.txt", mode)
    assert "smart_open" in str(excinfo.value)


@pytest.mark.parametrize("url", ["ftps://host/a.txt", "scp://host/a.txt", "file://remote.example/a.txt"])
@pytest.mark.parametrize("mode", ["r", "w"])
def test_unknown_schemes_and_remote_file_uri_rejected(url, mode):
    """S2-B4: Unknown scheme:// and non-localhost file:// URIs fail with UnsupportedProtocolError."""
    with pytest.raises(UnsupportedProtocolError):
        openas(url, mode)


@pytest.mark.parametrize("bad_mode", ["x", "r+", "w+", "", "rw"])
def test_invalid_modes_and_append_progress_and_timeout_validation(tmp_path, bad_mode):
    """S3-T1 & S2-S2: Invalid modes, append+progress, and invalid timeout fail clearly."""
    target = tmp_path / "modes.txt"
    with pytest.raises(ValueError, match="Supported modes"):
        openas(target, bad_mode)

    with pytest.raises(NotImplementedError):
        openas(target, "a", show_progress=True)

    for bad_timeout in (0, -1.5, True, "5"):
        with pytest.raises(ValueError, match="timeout"):
            openas(target, "r", timeout=bad_timeout)


@pytest.mark.parametrize("w_mode,a_mode,r_mode", [("w", "a", "r"), ("wt", "at", "rt")])
def test_text_modes_are_utf8_str_and_reject_bytes(tmp_path, w_mode, a_mode, r_mode):
    """Text modes (r/rt, w/wt, a/at) operate on UTF-8 str and reject raw bytes."""
    target = tmp_path / f"text_{w_mode}.txt"
    with openas(target, w_mode, backup=False) as w:
        assert w.encoding == "utf-8"
        w.write("café\n")
        with pytest.raises(TypeError):
            w.write(b"raw bytes")
    with openas(target, a_mode) as w:
        assert w.encoding == "utf-8"
        w.write("more\n")
    with openas(target, r_mode, compresslevel=9) as r:
        assert r.encoding == "utf-8"
        content = r.read()
        assert isinstance(content, str)
        assert content == "café\nmore\n"


def test_binary_modes_are_raw_bytes_and_reject_str(tmp_path):
    """Binary modes (rb, wb, ab) operate on raw bytes (including non-UTF-8) and reject str."""
    target = tmp_path / "raw_binary.bin"
    raw1 = b"\x00\xff\xfe\x80first\n"
    raw2 = b"\xde\xad\xbe\xefsecond\n"
    with openas(target, "wb", backup=False) as w:
        assert w.encoding is None
        assert w.write(raw1) == len(raw1)
        with pytest.raises(TypeError):
            w.write("text not allowed")
    with openas(target, "ab") as w:
        assert w.encoding is None
        assert w.write(raw2) == len(raw2)
    with openas(target, "rb", compresslevel=9) as r:
        assert r.encoding is None
        content = r.read()
        assert isinstance(content, bytes)
        assert content == raw1 + raw2


def test_single_colon_local_paths_and_file_uris(tmp_path, monkeypatch):
    """iev841 E-01: Single-colon local filenames stay local while file:/// and file://localhost/ URIs resolve locally."""
    monkeypatch.chdir(tmp_path)

    if os.name != "nt":
        # Create a distinct plain report.txt to verify file:report.txt does not strip the file: prefix.
        (tmp_path / "report.txt").write_text("plain report\n", encoding="utf-8")

        for name in ("file:report.txt", "http:2026.log", "sftp:notes.txt", "s3:dump.txt"):
            payload = f"content for {name}\n"
            with openas(name, "w", backup=False) as w:
                w.write(payload)
            assert (tmp_path / name).read_text(encoding="utf-8") == payload
            with openas(name, "r") as r:
                assert r.read() == payload

        assert (tmp_path / "report.txt").read_text(encoding="utf-8") == "plain report\n"

        # A path containing '://' later in the path without starting with scheme:// stays local.
        (tmp_path / "http:foo:").mkdir()
        with openas("http:foo://bar.txt", "w", backup=False) as w:
            w.write("nested colon slash\n")
        assert (tmp_path / "http:foo:" / "bar.txt").read_text(encoding="utf-8") == "nested colon slash\n"
        with openas("http:foo://bar.txt", "r") as r:
            assert r.read() == "nested colon slash\n"

    # Valid local file:/// and file://localhost/ URIs continue to resolve to local paths.
    uri_target = tmp_path / "uri_ok.txt"
    with openas(uri_target.as_uri(), "w", backup=False) as w:
        w.write("via file:/// uri\n")
    with openas(uri_target.as_uri(), "r") as r:
        assert r.read() == "via file:/// uri\n"

    localhost_uri = uri_target.as_uri().replace("file:///", "file://localhost/", 1)
    with openas(localhost_uri, "r") as r:
        assert r.read() == "via file:/// uri\n"


@pytest.mark.parametrize(
    "url",
    [
        "sftp://u@h/dir/archive%2Ezip",
        "https://h/dir/archive.tar%2egz",
        "sftp://user@host/dir/a;archive.zip",
        "https://example.com/dir/a;archive.tar.gz",
    ],
)
@pytest.mark.parametrize("mode", ["r", "w", "a"])
def test_percent_encoded_remote_archives_rejected(url, mode):
    """iev841 E-01 & kfvf7i E-01: Percent-encoded and semicolon archive extensions raise UnsupportedArchiveError."""
    with pytest.raises(UnsupportedArchiveError):
        openas(url, mode)


@pytest.mark.parametrize(
    "url,expected_authority",
    [
        ("file://alice:supersecret@remote.example/a.txt", "alice:***@remote.example"),
        ("file://alice:supersecret@/a.txt", "alice:***@"),
        ("file://alice:supersecret@localhost/a.txt", "alice:***@localhost"),
    ],
)
@pytest.mark.parametrize("mode", ["r", "w"])
def test_file_uri_authority_errors_redact_credentials_without_none(url, expected_authority, mode):
    """kfvf7i E-01: Unsupported file:// authorities redact credentials and never print 'None'."""
    with pytest.raises(UnsupportedProtocolError) as excinfo:
        openas(url, mode)
    msg = str(excinfo.value)
    assert "supersecret" not in msg
    assert "'None'" not in msg
    assert expected_authority in msg


@pytest.mark.parametrize(
    "url,expected_redacted",
    [
        ("sftp://user:supersecret:1234/file.txt", "sftp://user:***/file.txt"),
        ("ftp://user:supersecret:1234/file.txt", "ftp://user:***/file.txt"),
        ("sftp://user:supersecret/file.txt", "sftp://user:***/file.txt"),
        ("sftp://[::1]:supersecret/file.txt", "sftp://[::1]:***/file.txt"),
    ],
)
@pytest.mark.parametrize("mode", ["r", "w"])
def test_malformed_port_and_no_at_credential_urls_fail_redacted(url, expected_redacted, mode):
    """kfvf7i E-01: Malformed port/credential URLs without '@' raise redacted ValueError and redact in repr()."""
    from openas import OpenAsReader, OpenAsWriter
    from openas.openas import _redact_url

    assert _redact_url(url) == expected_redacted
    for obj in (OpenAsReader(url), OpenAsWriter(url)):
        rep = repr(obj)
        assert "supersecret" not in rep
        assert expected_redacted in rep

    with pytest.raises(ValueError) as excinfo:
        openas(url, mode)
    msg = str(excinfo.value)
    assert "supersecret" not in msg
    assert expected_redacted in msg
    assert excinfo.value.__suppress_context__ is True


def test_redact_url_preserves_valid_numeric_and_empty_ports_and_masks_non_digits():
    """kfvf7i E-01: _redact_url masks non-digit port suffixes while keeping numeric/empty ports unmasked."""
    from openas.openas import _redact_url

    assert _redact_url("sftp://host:8080/a.txt") == "sftp://host:8080/a.txt"
    assert _redact_url("sftp://host:0/a.txt") == "sftp://host:0/a.txt"
    assert _redact_url("sftp://host:99999/a.txt") == "sftp://host:99999/a.txt"
    assert _redact_url("sftp://host:/a.txt") == "sftp://host:/a.txt"
    assert _redact_url("sftp://[::1]:2222/a.txt") == "sftp://[::1]:2222/a.txt"
    assert _redact_url("sftp://[::1]:/a.txt") == "sftp://[::1]:/a.txt"
    assert _redact_url("sftp://alice:pw@host:badport/a.txt") == "sftp://alice:***@host:***/a.txt"
    assert _redact_url("sftp://alice:pw@[::1]:badport/a.txt?tok=1") == "sftp://alice:***@[::1]:***/a.txt?<redacted>"


@pytest.mark.parametrize(
    "url",
    [
        "sftp://host:0/a.txt",
        "sftp://host:99999/a.txt",
        "ftp://host:0/a.txt",
        "https://example.com:65536/a.txt",
    ],
)
def test_invalid_zero_and_out_of_range_ports_rejected(url):
    """kfvf7i E-01: Port 0 and out-of-range ports fail fast with ValueError."""
    with pytest.raises(ValueError, match="port"):
        openas(url, "r")


@pytest.mark.parametrize(
    "url",
    [
        "file://",
        "file://localhost",
        "ftp://user:supersecret@host",
        "ssh://user:supersecret@host",
        "sftp://user:supersecret@host",
        "sftp://user@host:2222",
    ],
)
@pytest.mark.parametrize("mode", ["r", "w"])
def test_empty_path_rejected_for_file_ftp_ssh_sftp(url, mode):
    """kfvf7i E-01: Empty path on file://, ftp://, ssh://, and sftp:// raises redacted ValueError."""
    with pytest.raises(ValueError) as excinfo:
        openas(url, mode)
    msg = str(excinfo.value)
    assert "supersecret" not in msg


@pytest.mark.parametrize("bad_timeout", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_timeout_and_format_bytes(tmp_path, bad_timeout):
    """kfvf7i E-01: Non-finite timeout raises ValueError; non-finite format_bytes returns '<num>B'."""
    from openas.openas import format_bytes

    target = tmp_path / "timeout_nonfinite.txt"
    with pytest.raises(ValueError, match="timeout"):
        openas(target, "r", timeout=bad_timeout)

    assert format_bytes(bad_timeout) == f"{bad_timeout}B"
