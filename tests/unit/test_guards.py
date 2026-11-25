import os
import pytest
from polyopen import polyopen, UnsupportedArchiveError, ReadOnlyProtocolError, UnsupportedProtocolError

def test_exception_guards_archive(tmp_path):
    # Test Archive Guard Write
    with pytest.raises(UnsupportedArchiveError):
        polyopen(str(tmp_path / "test.zip"), 'w')
        
    # Test Archive Guard Read
    with pytest.raises(UnsupportedArchiveError):
        polyopen(str(tmp_path / "test.tar.gz"), 'r')

def test_exception_guards_http():       
    # Test Protocol Guard (HTTP write)
    with pytest.raises(ReadOnlyProtocolError):
        polyopen("http://example.com/data.txt", 'w')

def test_exception_guards_cloud():
    # Test Unsupported Cloud Protocols explicitly redirect developers
    with pytest.raises(UnsupportedProtocolError) as excinfo:
        polyopen("s3://bucket/data.csv", 'r')
    assert "boto3" in str(excinfo.value) or "smart_open" in str(excinfo.value)
    
def test_append_backup_conflict(tmp_path):
    from polyopen import PolyWriter
    # Polyopen logic states you cannot append AND backup simultaneously
    with pytest.raises(ValueError):
        PolyWriter(str(tmp_path / "data.zst")).open(append=True, backup=True)

def test_writer_progress_exception(tmp_path):
    # Validates explicitly the newly structured parameter guard correctly fails.
    with pytest.raises(NotImplementedError):
        polyopen(str(tmp_path / "testing.zst"), 'w', show_progress=True)


def test_polyopen_append_backup_guard(tmp_path):
    """BUG-04: polyopen(path, 'a', backup=True) raises ValueError; default append skips backup."""
    target = tmp_path / "append_guard.txt"
    with polyopen(target, "w") as w:
        w.write("first\n")

    with pytest.raises(ValueError):
        polyopen(target, "a", backup=True)

    with polyopen(target, "a") as w:
        w.write("second\n")

    assert not os.path.exists(f"{target}.001.bu")
    with polyopen(target, "r") as r:
        assert r.read() == "first\nsecond\n"


def test_polywriter_show_progress_signature_guard(tmp_path):
    """BUG-06a: PolyWriter accepts show_progress=False and rejects show_progress=True."""
    from polyopen import PolyWriter

    target = tmp_path / "writer_prog.txt"
    with PolyWriter(target, show_progress=False) as w:
        w.write("ok\n")
    with pytest.raises(NotImplementedError):
        PolyWriter(target, show_progress=True)


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
        polyopen(tmp_path / name, mode)

    with pytest.raises(UnsupportedArchiveError):
        polyopen(f"file://{(tmp_path / name).as_posix()}", mode)

    with pytest.raises(UnsupportedArchiveError) as excinfo:
        polyopen(f"sftp://alice:topsecret@host/dir/{name}?token=xyz", mode)
    msg = str(excinfo.value)
    assert "topsecret" not in msg
    assert "token=xyz" not in msg
    assert "***" in msg


@pytest.mark.parametrize("scheme", ["http", "https"])
@pytest.mark.parametrize("mode", ["w", "a"])
def test_http_https_readonly_guards(scheme, mode):
    with pytest.raises(ReadOnlyProtocolError):
        polyopen(f"{scheme}://example.com/data.txt", mode)


@pytest.mark.parametrize("scheme", ["s3", "gs", "az", "hdfs"])
@pytest.mark.parametrize("mode", ["r", "w", "a"])
def test_cloud_schemes_rejected_all_modes(scheme, mode):
    with pytest.raises(UnsupportedProtocolError) as excinfo:
        polyopen(f"{scheme}://bucket/data.txt", mode)
    assert "smart_open" in str(excinfo.value)


@pytest.mark.parametrize("url", ["ftps://host/a.txt", "scp://host/a.txt", "file://remote.example/a.txt"])
@pytest.mark.parametrize("mode", ["r", "w"])
def test_unknown_schemes_and_remote_file_uri_rejected(url, mode):
    """S2-B4: Unknown scheme:// and non-localhost file:// URIs fail with UnsupportedProtocolError."""
    with pytest.raises(UnsupportedProtocolError):
        polyopen(url, mode)


@pytest.mark.parametrize("bad_mode", ["x", "r+", "w+", "", "rw"])
def test_invalid_modes_and_append_progress_and_timeout_validation(tmp_path, bad_mode):
    """S3-T1 & S2-S2: Invalid modes, append+progress, and invalid timeout fail clearly."""
    target = tmp_path / "modes.txt"
    with pytest.raises(ValueError, match="Supported modes"):
        polyopen(target, bad_mode)

    with pytest.raises(NotImplementedError):
        polyopen(target, "a", show_progress=True)

    for bad_timeout in (0, -1.5, True, "5"):
        with pytest.raises(ValueError, match="timeout"):
            polyopen(target, "r", timeout=bad_timeout)


@pytest.mark.parametrize("w_mode,a_mode,r_mode", [("wt", "at", "rt"), ("wb", "ab", "rb")])
def test_text_and_binary_alias_modes_are_utf8_text(tmp_path, w_mode, a_mode, r_mode):
    """S2-B2 / S3-T1: Documented v0.1.0 contract — rt/wt/at and rb/wb/ab operate on UTF-8 str."""
    target = tmp_path / f"alias_{w_mode}.txt"
    with polyopen(target, w_mode, backup=False) as w:
        w.write("café\n")
        with pytest.raises(TypeError):
            w.write(b"raw bytes")
    with polyopen(target, a_mode) as w:
        w.write("more\n")
    with polyopen(target, r_mode, compresslevel=9) as r:
        assert r.read() == "café\nmore\n"
