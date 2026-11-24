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
