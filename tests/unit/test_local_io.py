import errno
import os
from unittest.mock import MagicMock, patch
import pytest
import zstandard as zstd
from polyopen import PolyReader, PolyWriter, polyopen

def test_plain_text(tmp_path):
    data = ["hello", "world", "this", "is", "a", "test"]
    filepath = str(tmp_path / "test.txt")

    # Test writing
    with PolyWriter(filepath) as writer:
        for line in data:
            writer.write(line + '\n')
    
    assert os.path.exists(filepath)
    with open(filepath, 'r', encoding='utf-8') as f:
        assert f.read().splitlines() == data

    # Test reading
    read_data = []
    with PolyReader(filepath) as reader:
        for line in reader:
            read_data.append(line.rstrip('\n'))
            
    assert read_data == data

@pytest.mark.parametrize("ext", [".gz", ".bz2", ".zst", ".xz"])
def test_compressed_files(tmp_path, ext):
    data = ["line1 compressed", "line2 compressed", f"some data for {ext}"]
    filepath = str(tmp_path / f"test_data{ext}")

    # Write compressed
    with PolyWriter(filepath) as writer:
        for line in data:
            writer.write(line + '\n')

    assert os.path.exists(filepath)
    
    # Read compressed
    read_data = []
    with PolyReader(filepath) as reader:
        for line in reader:
            read_data.append(line.rstrip('\n'))
            
    assert read_data == data

def test_polyopen_shorthand(tmp_path):
    filepath = str(tmp_path / "shorthand_test.zst")
    data = ["polyopen", "shorthand", "rocks"]
    
    # Test writing
    with polyopen(filepath, 'w') as writer:
        for line in data:
            writer.write(line + '\n')
            
    assert os.path.exists(filepath)
    
    # Test appending
    append_data = ["another", "line"]
    with polyopen(filepath, 'a') as writer:
        for line in append_data:
            writer.write(line + '\n')
            
    read_appended = []
    with polyopen(filepath, 'r') as reader:
        for line in reader:
            read_appended.append(line.rstrip('\n'))
            
    assert read_appended == data + append_data

def test_backup_rotation(tmp_path):
    filepath = str(tmp_path / "backup.txt")
    
    with polyopen(filepath, 'w', backup=True) as w:
        w.write("First.\n")
        
    with polyopen(filepath, 'w', backup=True) as w:
        w.write("Second.\n")
        
    backup_path = f"{filepath}.001.bu"
    assert os.path.exists(backup_path)
    
    with open(backup_path, 'r') as f:
        assert f.read() == "First.\n"

@pytest.mark.parametrize("ext", [".gz", ".bz2", ".zst", ".xz"])
def test_rigorous_line_chunking(tmp_path, ext):
    """
    Rigorously assess how CPython buffers streaming algorithms across native memory constraints.
    Default IO chunks are routinely 131,072 bytes. This suite generates payloads explicitly 
    engineered to collide cleanly across chunk frames.
    """
    filepath = str(tmp_path / f"rigorous_chunk_test{ext}")
    
    # Constructing lines to meet explicit chunking edge logic
    CHUNK_SIZE = 131072
    lines_to_write = [
        "A short line with whitespace. \n", # 1. Less than chunk size
        "A" * int(CHUNK_SIZE * 0.8) + "\n", # Fill up end of chunk 1
        "B" * int(CHUNK_SIZE * 0.5) + " words \n", # 2. Starts in chunk 1, ends in chunk 2
        "C" * int(CHUNK_SIZE * 2.5) + " traversing \n", # 3. Starts in chunk 2, traverses chunk 3, ends in 4
        "Short middle line \n",
        "Another string with random whitespaces \n",
        "More string padding to shift the buffer windows around \n",
        "Small 8th line \n",
        "D" * int(CHUNK_SIZE * 0.3) + " testing \n",
        "E" * 500 + "\n",
        "F" * 200 + "\n",
        "G" * int(CHUNK_SIZE * 1.5) + " final line \n" # 4. Ends file spanning across final chunk
    ]
    
    with PolyWriter(filepath) as writer:
        for line in lines_to_write:
            writer.write(line)
            
    read_data = []
    with PolyReader(filepath) as reader:
        for line in reader:
             read_data.append(line)
             
    # Asserting exact identical retrieval
    assert len(read_data) == 12
    assert read_data == lines_to_write


def test_uppercase_extension_local(tmp_path):
    """BUG-03: Uppercase local extensions decompress and compress cleanly."""
    fp = tmp_path / "DATA.GZ"
    with polyopen(fp, "w") as w:
        w.write("upper gz\n")
    with polyopen(fp, "r") as r:
        assert r.read() == "upper gz\n"


def test_close_reopen_idempotency_and_sibling_cleanup(tmp_path):
    """BUG-04: Reopen after close, idempotent close, and cleanup on failing close or partial open."""
    fp = tmp_path / "lifecycle.txt"
    with polyopen(fp, "w") as w:
        w.write("line1\nline2\n")

    reader = PolyReader(fp).open()
    assert next(reader) == "line1\n"
    reader.close()
    reader.close()  # idempotent
    reader.open()
    assert next(reader) == "line1\n"

    sibling = MagicMock()
    failing = MagicMock()
    failing.close.side_effect = OSError("close failed")
    reader._fh = failing
    reader._ftp = sibling
    with pytest.raises(OSError):
        reader.close()
    assert sibling.close.called
    assert reader._fh is None
    assert reader._ftp is None

    # Partial open failure cleans up acquired handles
    partial_reader = PolyReader(fp, show_progress=True)
    with patch.object(partial_reader, "_get_file_size", side_effect=RuntimeError("stat boom")):
        with pytest.raises(RuntimeError):
            partial_reader.open()
    assert partial_reader._fh is None
    assert partial_reader._raw_fh is None


def test_atomic_backup_race_fallback_post999_and_multiframe_zst(tmp_path):
    """BUG-05: Atomic os.link retry, EOPNOTSUPP/EXDEV rename fallback, post-999 ns retry, and multi-frame .zst."""
    target = tmp_path / "race.txt"
    target.write_text("v1\n", encoding="utf-8")

    real_link = os.link
    calls = {"n": 0}

    def racing_link(src, dst):
        calls["n"] += 1
        if calls["n"] == 1 and dst.endswith(".001.bu"):
            with open(dst, "w", encoding="utf-8") as f:
                f.write("racer\n")
            raise FileExistsError(dst)
        return real_link(src, dst)

    with patch("os.link", side_effect=racing_link):
        with polyopen(target, "w", backup=True) as w:
            w.write("v2\n")
    assert (tmp_path / "race.txt.001.bu").read_text(encoding="utf-8") == "racer\n"
    assert (tmp_path / "race.txt.002.bu").read_text(encoding="utf-8") == "v1\n"

    # Fallback to os.rename on EOPNOTSUPP and EXDEV
    for err_code in (errno.EOPNOTSUPP, errno.EXDEV):
        fb = tmp_path / f"fb_{err_code}.txt"
        fb.write_text("old\n", encoding="utf-8")
        with patch("os.link", side_effect=OSError(err_code, "unsupported")):
            with polyopen(fb, "w", backup=True) as w:
                w.write("new\n")
        assert (tmp_path / f"fb_{err_code}.txt.001.bu").read_text(encoding="utf-8") == "old\n"
        assert fb.read_text(encoding="utf-8") == "new\n"

    # Rapid post-999 nanosecond collision retry preserves both backups
    p999 = tmp_path / "full.txt"
    for i in range(1, 1000):
        (tmp_path / f"full.txt.{i:03d}.bu").touch()
    p999.write_text("first\n", encoding="utf-8")
    with patch("time.time_ns", return_value=1700000000000000000):
        with polyopen(p999, "w", backup=True) as w:
            w.write("second\n")
        with polyopen(p999, "w", backup=True) as w:
            w.write("third\n")
    assert (tmp_path / "full.txt.1700000000000000000.bu").read_text(encoding="utf-8") == "first\n"
    assert (tmp_path / "full.txt.1700000000000000001.bu").read_text(encoding="utf-8") == "second\n"

    # Multi-frame concatenated .zst reads all frames
    zpath = tmp_path / "multi.zst"
    cctx = zstd.ZstdCompressor()
    zpath.write_bytes(cctx.compress(b"frame1\n") + cctx.compress(b"frame2\n"))
    with PolyReader(zpath) as r:
        assert list(r) == ["frame1\n", "frame2\n"]


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_pathlike_utf8_and_file_object_methods(tmp_path, ext):
    """BUG-06: PathLike[str]/PathLike[bytes], UTF-8 round-trip, read/readline/readlines/flush, and closed ValueError."""
    fp = tmp_path / f"utf8{ext}"
    with polyopen(fp, "w") as w:
        w.write("café 日本語\nsecond line\n")
        w.flush()
    with pytest.raises(ValueError):
        w.flush()

    with polyopen(fp, "r") as r:
        assert r.readline() == "café 日本語\n"
        assert r.readlines() == ["second line\n"]
    with polyopen(fp, "r") as r:
        assert r.read(4) == "café"
        assert r.read() == " 日本語\nsecond line\n"
    for method in ("read", "readline", "readlines"):
        with pytest.raises(ValueError):
            getattr(r, method)()

    class BytesPathLike:
        def __init__(self, raw):
            self._raw = raw

        def __fspath__(self):
            return self._raw

    bp = BytesPathLike(os.fsencode(str(tmp_path / f"bytes_path{ext}")))
    with polyopen(bp, "w") as w:
        w.write("bytes path ok\n")
    with polyopen(bp, "r") as r:
        assert r.read() == "bytes path ok\n"


@pytest.mark.parametrize("ext", [".gz", ".bz2", ".xz", ".zst"])
def test_compressed_progress_tracks_raw_bytes(tmp_path, ext):
    """BUG-07: show_progress=True tracks exact raw compressed bytes so progress.n == progress.total."""
    payload = ("0123456789abcdef" * 6 + "\n") * 520
    fp = tmp_path / f"progress{ext}"
    with polyopen(fp, "w") as w:
        w.write(payload)

    raw_size = os.path.getsize(fp)
    reader = PolyReader(fp, show_progress=True).open()
    try:
        content = reader.read()
        assert content == payload
        assert reader._progress is not None
        assert reader._progress.total == raw_size
        assert reader._progress.n == reader._progress.total
    finally:
        reader.close()
