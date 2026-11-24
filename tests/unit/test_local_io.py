import bz2
import errno
import gzip
import os
import subprocess
import sys
import types
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


def test_lazy_imports_and_ftp_session_wrapper_factory():
    """PERF-06: import polyopen loads no optional stdlib/FTP modules; FTPSessionWrapper is cached."""
    script = (
        "import sys; "
        "before = set(sys.modules); "
        "import polyopen; "
        "added = set(sys.modules) - before; "
        "forbidden = ('argparse', 'ftplib', 'ssl', 'bz2', 'gzip', 'lzma', 'urllib.parse'); "
        "loaded = [m for m in forbidden if m in added]; "
        "assert loaded == [], f'unexpected eager imports: {loaded}'; "
        "from polyopen import FTPSessionWrapper as A; "
        "from polyopen import FTPSessionWrapper as B; "
        "import ftplib; "
        "assert issubclass(A, ftplib.FTP); "
        "assert A is B; "
        "print('OK')"
    )
    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
    env = os.environ.copy()
    existing_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{existing_pp}" if existing_pp else src_dir
    res = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env=env,
    )
    assert res.returncode == 0, res.stderr
    assert res.stdout.strip() == "OK"


@pytest.mark.parametrize("ext", [".txt", ".gz", ".bz2", ".xz", ".zst"])
def test_polyreader_for_loop_direct_next_and_file_uri(tmp_path, ext):
    """PERF-02: PolyReader supports for-iteration, direct next(), and file: URLs."""
    expected = [f"row-{i}\n" for i in range(10)]
    fp = tmp_path / f"iter_check{ext}"
    with PolyWriter(fp) as w:
        for line in expected:
            w.write(line)

    with PolyReader(fp) as r:
        assert list(r) == expected

    with PolyReader(fp) as r:
        got = []
        while True:
            try:
                got.append(next(r))
            except StopIteration:
                break
        assert got == expected

    if ext == ".txt":
        with PolyReader(fp.as_uri()) as r:
            assert list(r) == expected


def test_polyreader_progress_batched_full_and_early_break_flush(tmp_path):
    """PERF-02: show_progress=True flushes exact remainder on EOF and early break + close."""
    # Size: 7,500 * 10 = 75,000 bytes (not a multiple of 65,536).
    lines = [f"{i:09d}\n" for i in range(7500)]
    fp = tmp_path / "ascii_progress.txt"
    fp.write_text("".join(lines), encoding="utf-8")
    expected_size = os.path.getsize(fp)
    assert expected_size % 65536 != 0

    reader = PolyReader(fp, show_progress=True).open()
    try:
        pbar = reader._progress
        assert pbar is not None
        seen = list(reader)
        assert len(seen) == len(lines)
        assert reader._progress.n == expected_size
    finally:
        reader.close()
    assert pbar.n == expected_size

    # Early break followed by close() flushes exact summed length of yielded lines.
    reader2 = PolyReader(fp, show_progress=True).open()
    pbar2 = reader2._progress
    assert pbar2 is not None
    yielded = []
    for line in reader2:
        yielded.append(line)
        if len(yielded) == 25:
            break
    reader2.close()
    assert pbar2.n == sum(len(line) for line in yielded)

    # Paused iterator still alive when close() runs flushes pending progress before close.
    reader3 = PolyReader(fp, show_progress=True).open()
    pbar3 = reader3._progress
    assert pbar3 is not None
    it = iter(reader3)
    yielded3 = [next(it) for _ in range(7)]
    reader3.close()
    assert pbar3.n == sum(len(line) for line in yielded3)


def test_polywriter_writelines_subclass_override_and_closed_error(tmp_path):
    """PERF-08: PolyWriter.writelines(), subclass write() override preservation, and closed ValueError."""
    fp = tmp_path / "writelines.txt"
    payload = ["alpha\n", "beta\n", "gamma\n"]
    writer = PolyWriter(fp).open(append=False, backup=False)
    writer.writelines(payload)
    writer.close()
    assert fp.read_text(encoding="utf-8") == "".join(payload)

    with pytest.raises(ValueError):
        writer.write("after close\n")
    with pytest.raises(ValueError):
        writer.writelines(["after close\n"])

    class CountingWriter(PolyWriter):
        def __init__(self, filename):
            super().__init__(filename)
            self.calls = []

        def write(self, data):
            self.calls.append(data)
            return super().write(data)

    fp_sub = tmp_path / "subclass.txt"
    with CountingWriter(fp_sub) as cw:
        cw.write("one\n")
        cw.write("two\n")
    assert cw.calls == ["one\n", "two\n"]
    assert fp_sub.read_text(encoding="utf-8") == "one\ntwo\n"


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2"])
def test_ssh_reads_via_fake_paramiko(tmp_path, monkeypatch, ext):
    """PERF-02 (E-06): SSH reads of .gz, .bz2, and uncompressed files yield original str lines."""
    expected = ["ssh line 1\n", "ssh café 2\n", "ssh line 3\n"]
    raw_bytes = "".join(expected).encode("utf-8")
    fp = tmp_path / f"remote_sample{ext}"
    if ext == ".gz":
        fp.write_bytes(gzip.compress(raw_bytes))
    elif ext == ".bz2":
        fp.write_bytes(bz2.compress(raw_bytes))
    else:
        fp.write_bytes(raw_bytes)

    class FakeSFTP:
        def file(self, path, mode="r"):
            return open(fp, "rb")

        def close(self):
            pass

    class FakeSSHClient:
        def load_system_host_keys(self):
            pass

        def connect(self, hostname, port=22, username=None, password=None):
            pass

        def open_sftp(self):
            return FakeSFTP()

        def close(self):
            pass

    fake_paramiko = types.ModuleType("paramiko")
    fake_paramiko.SSHClient = FakeSSHClient
    monkeypatch.setitem(sys.modules, "paramiko", fake_paramiko)

    url = f"ssh://user:pass@host/dir/remote_sample{ext}"
    with PolyReader(url) as r:
        got = list(r)
    assert got == expected
    assert all(isinstance(line, str) for line in got)


def test_http_uncompressed_blank_line_not_truncated(monkeypatch):
    """PERF-02: blank line b'' from HTTP iter_lines() does not end for-loop or next() early."""

    class FakeResponse:
        headers = {}

        def raise_for_status(self):
            pass

        def iter_lines(self):
            return iter([b"a", b"", b"b"])

        def close(self):
            pass

    fake_requests = types.ModuleType("requests")
    fake_requests.get = lambda url, stream=True: FakeResponse()
    monkeypatch.setitem(sys.modules, "requests", fake_requests)

    with PolyReader("http://example.invalid/plain.txt") as r:
        assert list(r) == ["a", "", "b"]

    with PolyReader("http://example.invalid/plain.txt") as r:
        got = []
        while True:
            try:
                got.append(next(r))
            except StopIteration:
                break
        assert got == ["a", "", "b"]
