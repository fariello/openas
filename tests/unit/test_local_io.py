import bz2
import errno
import gzip
import lzma
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

    orig_fh = reader._fh
    sibling = MagicMock()
    failing = MagicMock()
    failing.close.side_effect = OSError("close failed")
    reader._fh = failing
    reader._ftp = sibling
    try:
        with pytest.raises(OSError):
            reader.close()
    finally:
        if orig_fh is not None:
            orig_fh.close()
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
    """PERF-02/PERF-05: blank line in uncompressed HTTP body does not end for-loop or next() early."""
    import io

    class FakeResponse:
        headers = {}

        def __init__(self):
            self.raw = io.BytesIO(b"a\n\nb\n")

        def raise_for_status(self):
            pass

        def close(self):
            pass

    class FakeSession:
        def get(self, url, stream=True):
            return FakeResponse()

    monkeypatch.setattr(sys.modules["polyopen.polyopen"], "_get_http_session", lambda: FakeSession())

    with PolyReader("http://example.invalid/plain.txt") as r:
        assert list(r) == ["a\n", "\n", "b\n"]

    with PolyReader("http://example.invalid/plain.txt") as r:
        got = []
        while True:
            try:
                got.append(next(r))
            except StopIteration:
                break
        assert got == ["a\n", "\n", "b\n"]


@pytest.mark.parametrize("ext", [".zst", ".gz", ".bz2", ".xz"])
def test_local_compresslevel_default_and_explicit_roundtrip(tmp_path, ext):
    """p1f3dc E-01: Local default (omitting None kwargs) and explicit compresslevel round-trips across all 4 formats."""
    payload = f"local compresslevel test for {ext} café\nsecond line\n"
    fp_default = tmp_path / f"default{ext}"
    fp_explicit = tmp_path / f"explicit{ext}"
    fp_shorthand = tmp_path / f"shorthand{ext}"

    if ext == ".zst":
        comp_patch = patch("zstandard.ZstdCompressor", wraps=zstd.ZstdCompressor)
    elif ext == ".gz":
        comp_patch = patch("gzip.open", wraps=gzip.open)
    elif ext == ".bz2":
        comp_patch = patch("bz2.open", wraps=bz2.open)
    else:
        comp_patch = patch("lzma.open", wraps=lzma.open)

    with comp_patch as mock_comp:
        with PolyWriter(fp_default).open(backup=False) as w:
            w.write(payload)
        call_args, call_kwargs = mock_comp.call_args
        if ext == ".zst":
            assert call_args == ()
            assert call_kwargs == {}
        elif ext in (".gz", ".bz2"):
            assert "compresslevel" not in call_kwargs
        else:
            assert "preset" not in call_kwargs

        mock_comp.reset_mock()
        with PolyWriter(fp_explicit).open(backup=False, compresslevel=2) as w:
            w.write(payload)
        _, call_kwargs = mock_comp.call_args
        if ext == ".zst":
            assert call_kwargs == {"level": 2}
        elif ext in (".gz", ".bz2"):
            assert call_kwargs["compresslevel"] == 2
        else:
            assert call_kwargs["preset"] == 2

        mock_comp.reset_mock()
        with polyopen(fp_shorthand, "w", backup=False, compresslevel=1) as w:
            w.write(payload)
        _, call_kwargs = mock_comp.call_args
        if ext == ".zst":
            assert call_kwargs == {"level": 1}
        elif ext in (".gz", ".bz2"):
            assert call_kwargs["compresslevel"] == 1
        else:
            assert call_kwargs["preset"] == 1

    for path in (fp_default, fp_explicit, fp_shorthand):
        with PolyReader(path) as r:
            assert r.read() == payload


def test_local_backup_fast_path_including_bare_relative_filename(tmp_path, monkeypatch):
    """p1f3dc E-01: Local .001.bu fast-path avoids os.scandir, including for bare relative filenames."""
    monkeypatch.chdir(tmp_path)
    bare = "bare_relative.txt"
    with PolyWriter(bare).open(backup=True) as w:
        w.write("v1\n")

    with patch("os.scandir", wraps=os.scandir) as mock_scandir:
        with PolyWriter(bare).open(backup=True) as w:
            w.write("v2\n")
        mock_scandir.assert_not_called()

    assert (tmp_path / "bare_relative.txt.001.bu").read_text(encoding="utf-8") == "v1\n"
    assert (tmp_path / bare).read_text(encoding="utf-8") == "v2\n"

    # Second overwrite scans "." once and selects .002.bu
    with patch("os.scandir", wraps=os.scandir) as mock_scandir:
        with PolyWriter(bare).open(backup=True) as w:
            w.write("v3\n")
        mock_scandir.assert_called_once_with(".")

    assert (tmp_path / "bare_relative.txt.002.bu").read_text(encoding="utf-8") == "v2\n"
    assert (tmp_path / bare).read_text(encoding="utf-8") == "v3\n"


def test_local_backup_500_single_scandir_and_exact_slot_matching(tmp_path):
    """p1f3dc E-01: 500-backup single os.scandir slot selection (.501.bu) and exact 3-digit prefix/suffix matching."""
    target = tmp_path / "many.txt"
    target.write_text("current\n", encoding="utf-8")
    for idx in range(1, 501):
        (tmp_path / f"many.txt.{idx:03d}.bu").touch()

    with patch("os.scandir", wraps=os.scandir) as mock_scandir:
        with PolyWriter(target).open(backup=True) as w:
            w.write("updated\n")
        mock_scandir.assert_called_once_with(str(tmp_path))

    assert (tmp_path / "many.txt.501.bu").read_text(encoding="utf-8") == "current\n"
    assert target.read_text(encoding="utf-8") == "updated\n"

    # Exact 3-digit ASCII prefix/suffix matching with regex metacharacters in filename
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()
    meta_name = "data[1]+(a).txt"
    meta_target = meta_dir / meta_name
    meta_target.write_text("meta-v2\n", encoding="utf-8")
    (meta_dir / f"{meta_name}.001.bu").write_text("meta-v1\n", encoding="utf-8")
    # Decoys that must be ignored as 3-digit backup slots:
    (meta_dir / f"{meta_name}.2.bu").touch()
    (meta_dir / f"{meta_name}.0002.bu").touch()
    (meta_dir / f"{meta_name}.002.bu.bak").touch()
    (meta_dir / f"{meta_name}.1700000000000000000.bu").touch()
    (meta_dir / f"{meta_name}.\u0660\u0660\u0662.bu").touch()  # Arabic-Indic digits
    (meta_dir / "data1+(a).txt.002.bu").touch()
    (meta_dir / "other.txt.002.bu").touch()

    with patch("os.scandir", wraps=os.scandir) as mock_scandir:
        with PolyWriter(meta_target).open(backup=True) as w:
            w.write("meta-v3\n")
        mock_scandir.assert_called_once_with(str(meta_dir))

    assert (meta_dir / f"{meta_name}.002.bu").read_text(encoding="utf-8") == "meta-v2\n"
    assert meta_target.read_text(encoding="utf-8") == "meta-v3\n"


def test_local_backup_999_cap_single_scandir_and_collision_safe_timestamp(tmp_path):
    """p1f3dc E-01: 999-cap local backup scans once and increments past an existing timestamp collision."""
    cap_dir = tmp_path / "cap999"
    cap_dir.mkdir()
    target = cap_dir / "cap.txt"
    target.write_text("before-cap\n", encoding="utf-8")
    for idx in range(1, 1000):
        (cap_dir / f"cap.txt.{idx:03d}.bu").touch()
    preexisting_ts = cap_dir / "cap.txt.1700000000000000999.bu"
    preexisting_ts.write_text("existing-ts\n", encoding="utf-8")

    with patch("os.scandir", wraps=os.scandir) as mock_scandir:
        with patch("time.time_ns", return_value=1700000000000000999):
            with PolyWriter(target).open(backup=True) as w:
                w.write("after-cap\n")
        mock_scandir.assert_called_once_with(str(cap_dir))

    assert preexisting_ts.read_text(encoding="utf-8") == "existing-ts\n"
    assert (cap_dir / "cap.txt.1700000000000001000.bu").read_text(encoding="utf-8") == "before-cap\n"
    assert target.read_text(encoding="utf-8") == "after-cap\n"


def test_local_backup_rejects_directory_and_preserves_broken_symlink(tmp_path):
    """S2-LIVE1: Local backup raises IsADirectoryError on directories and avoids overwriting broken symlinks."""
    existing_dir = tmp_path / "important_dir"
    existing_dir.mkdir()
    (existing_dir / "keep.txt").write_text("precious\n", encoding="utf-8")

    with pytest.raises(IsADirectoryError):
        polyopen(existing_dir, "w", backup=True)

    assert existing_dir.is_dir()
    assert (existing_dir / "keep.txt").read_text(encoding="utf-8") == "precious\n"
    assert not (tmp_path / "important_dir.001.bu").exists()

    # Broken symlink at .001.bu is skipped on fallback rather than silently overwritten
    src = tmp_path / "target.txt"
    src.write_text("v1\n", encoding="utf-8")
    broken_bu = tmp_path / "target.txt.001.bu"
    broken_bu.symlink_to(tmp_path / "does_not_exist")
    with patch("os.link", side_effect=OSError(errno.EOPNOTSUPP, "no hard links")):
        with polyopen(src, "w", backup=True) as w:
            w.write("v2\n")
    assert os.path.islink(broken_bu)
    assert (tmp_path / "target.txt.002.bu").read_text(encoding="utf-8") == "v1\n"
    assert src.read_text(encoding="utf-8") == "v2\n"


@pytest.mark.parametrize(
    "ext,bad_level,exc_type",
    [
        (".gz", 99, ValueError),
        (".bz2", 0, ValueError),
        (".xz", 99, ValueError),
        (".zst", 999, ValueError),
        (".gz", True, TypeError),
        (".zst", "fast", TypeError),
    ],
)
def test_invalid_compresslevel_fails_before_rotating_backup(tmp_path, ext, bad_level, exc_type):
    """S2-LIVE2: Invalid or bool compresslevel fails before rotating the existing file."""
    target = tmp_path / f"keep{ext}"
    with polyopen(target, "w", backup=False) as w:
        w.write("original\n")

    with pytest.raises(exc_type):
        polyopen(target, "w", backup=True, compresslevel=bad_level)

    assert not os.path.exists(f"{target}.001.bu")
    with polyopen(target, "r") as r:
        assert r.read() == "original\n"


def test_local_non_ascii_progress_and_mixed_readline_iter_no_double_count(tmp_path):
    """S2-B8: Plain local UTF-8 progress tracks exact byte length and mixed readline()+iter does not double-count."""
    fp = tmp_path / "utf8_prog.txt"
    lines = ["café 日本語 🚀\n" for _ in range(10)]
    payload = "".join(lines)
    fp.write_text(payload, encoding="utf-8")
    expected_bytes = len(payload.encode("utf-8"))
    assert expected_bytes > len(payload)

    with PolyReader(fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert pbar.total == expected_bytes
        first = r.readline()
        rest = list(r)
        assert [first, *rest] == lines
    assert pbar.n == expected_bytes


def test_file_like_attributes_repr_redaction_and_unknown_pkg_attr(tmp_path):
    """S5-F1 & S3-T1: PolyReader/PolyWriter expose closed/readable/writable/seekable/encoding/redacted __repr__."""
    import polyopen as pkg

    fp = tmp_path / "attrs.txt"
    w = PolyWriter(fp)
    assert w.closed is True
    assert w.writable() is True and w.readable() is False and w.seekable() is False
    assert w.encoding == "utf-8"
    w.open(backup=False)
    assert w.closed is False
    w.write("hello\n")
    w.close()
    assert w.closed is True
    # Reopen after close with backup=False overwrites without creating .bu
    with w.open(backup=False):
        w.write("rewritten\n")
    assert not os.path.exists(f"{fp}.001.bu")
    assert fp.read_text(encoding="utf-8") == "rewritten\n"

    r = PolyReader(fp)
    assert r.closed is True
    assert r.readable() is True and r.writable() is False and r.seekable() is False
    assert r.encoding == "utf-8"
    with r:
        assert r.closed is False
        assert r.readlines(hint=4) == ["rewritten\n"]
    assert r.closed is True

    secret_reader = PolyReader("sftp://alice:mysecret@host/data.txt?token=tok")
    rep = repr(secret_reader)
    assert "mysecret" not in rep and "tok" not in rep and "***" in rep

    with pytest.raises(AttributeError):
        getattr(pkg, "NonExistentSymbol123")


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_local_empty_no_newline_multimember_append_file_uri_and_colon_filename(tmp_path, ext):
    """S3-T1: Empty files, missing trailing newline, multi-member append, file:// writer (with spaces), and colons."""
    # Empty file round-trip (including show_progress=True)
    empty_fp = tmp_path / f"empty{ext or '.txt'}"
    with polyopen(empty_fp, "w", backup=False):
        pass
    with polyopen(empty_fp, "r", show_progress=True) as r:
        assert r.read() == ""

    # Missing trailing newline + multi-member append across all formats
    app_fp = tmp_path / f"append{ext or '.txt'}"
    with polyopen(app_fp, "w", backup=False) as w:
        w.write("part1\n")
    with polyopen(app_fp, "a") as w:
        w.write("part2_no_nl")
    with polyopen(app_fp, "r") as r:
        assert r.read() == "part1\npart2_no_nl"

    # file:// URI write/append/read with space (%20) and local colon filename
    spaced_dir = tmp_path / "dir with space"
    spaced_dir.mkdir()
    spaced_fp = spaced_dir / f"doc{ext or '.txt'}"
    file_uri = spaced_fp.as_uri()
    with polyopen(file_uri, "w", backup=False) as w:
        w.write("uri line 1\n")
    with polyopen(file_uri, "a") as w:
        w.write("uri line 2\n")
    with polyopen(file_uri, "r") as r:
        assert r.read() == "uri line 1\nuri line 2\n"

    colon_fp = tmp_path / f"2026:11:25{ext or '.txt'}"
    with polyopen(colon_fp, "w", backup=False) as w:
        w.write("colon ok\n")
    with polyopen(colon_fp, "r") as r:
        assert r.read() == "colon ok\n"


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_local_binary_mode_all_formats_roundtrip_append_iter_and_progress(tmp_path, ext):
    """Binary modes (wb/ab/rb) handle non-UTF-8 bytes, line iteration, writelines, and progress across all codecs."""
    fp = tmp_path / f"binary_sample{ext or '.bin'}"
    initial_lines = [
        b"\x00\xff\xfe\x80line1\n",
        b"\x89PNG\r\n",
        b"\x1a\x00line3\n",
    ]
    appended = b"\xde\xad\xbe\xefline4_no_nl"
    expected_lines = initial_lines + [appended]
    expected_bytes = b"".join(expected_lines)

    with polyopen(fp, "wb", backup=False) as w:
        assert w.binary is True
        assert w.encoding is None
        assert "mode='wb'" in repr(w)
        assert w.write(initial_lines[0]) == len(initial_lines[0])
        w.writelines(initial_lines[1:])
        with pytest.raises(TypeError):
            w.write("str rejected in binary mode\n")
        with pytest.raises(TypeError):
            w.writelines(["str line rejected\n"])

    with polyopen(fp, "ab") as w:
        assert w.binary is True
        assert w.encoding is None
        assert w.write(appended) == len(appended)

    # Full read(), partial read(size), readline(), next(), readlines(), and for-loop iteration
    with polyopen(fp, "rb") as r:
        assert r.binary is True
        assert r.encoding is None
        assert "mode='rb'" in repr(r)
        got = r.read()
        assert isinstance(got, bytes)
        assert got == expected_bytes

    with polyopen(fp, "rb") as r:
        got_lines = list(r)
        assert all(isinstance(line, bytes) for line in got_lines)
        assert got_lines == expected_lines

    with polyopen(fp, "rb") as r:
        assert r.readline() == initial_lines[0]
        assert next(r) == initial_lines[1]
        assert r.readlines() == [initial_lines[2], appended]

    with polyopen(fp, "rb") as r:
        assert r.read(4) == b"\x00\xff\xfe\x80"
        assert r.readline() == b"line1\n"

    # Multi-chunk (>128 KiB) binary line streaming with show_progress=True
    big_fp = tmp_path / f"big_binary{ext or '.bin'}"
    big_lines = [bytes([0x80 + (i % 128)]) + b"\xff\x00" + f"row-{i:05d}-".encode("ascii") + (b"X" * 60) + b"\n" for i in range(2200)]
    with PolyWriter(big_fp, binary=True).open(backup=False) as w:
        w.writelines(big_lines)

    raw_size = big_fp.stat().st_size
    with PolyReader(big_fp, show_progress=True, binary=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert pbar.total == raw_size
        streamed = list(r)
    assert streamed == big_lines
    assert pbar.n == raw_size


def test_local_crlf_and_mixed_newline_progress_reconciliation(tmp_path):
    """iev841 E-03: Plain local text progress reconciles CRLF/CR at natural EOF without jumping early."""
    crlf_fp = tmp_path / "crlf.txt"
    crlf_raw = b"alpha\r\nbeta\r\ngamma\r\n"
    crlf_fp.write_bytes(crlf_raw)
    crlf_size = len(crlf_raw)

    # 1. Full iteration reaches pbar.n == pbar.total
    with PolyReader(crlf_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert pbar.total == crlf_size
        assert list(r) == ["alpha\n", "beta\n", "gamma\n"]
        assert pbar.n == crlf_size
    assert pbar.n == crlf_size

    # 2. Full read() reaches pbar.n == pbar.total
    with PolyReader(crlf_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert r.read() == "alpha\nbeta\ngamma\n"
        assert pbar.n == crlf_size

    # 3. readline() loop reaches pbar.n == pbar.total at natural EOF, while readline(0) mid-file does not jump
    with PolyReader(crlf_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        first = r.readline()
        assert first == "alpha\n"
        mid_n = pbar.n
        assert mid_n == len(first.encode("utf-8"))
        assert r.readline(0) == ""
        assert pbar.n == mid_n
        assert r.read(0) == ""
        assert pbar.n == mid_n
        assert r.readline() == "beta\n"
        assert r.readline() == "gamma\n"
        assert pbar.n < crlf_size
        assert r.readline() == ""
        assert pbar.n == crlf_size

    # 4. Early break on CRLF does not jump pbar.n to pbar.total
    with PolyReader(crlf_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        seen = []
        for line in r:
            seen.append(line)
            break
    assert seen == ["alpha\n"]
    assert pbar.n == len("alpha\n".encode("utf-8"))
    assert pbar.n < crlf_size

    # 5. Plain local file with no newlines (newlines is None) reads cleanly without TypeError
    no_nl_fp = tmp_path / "no_newlines.txt"
    no_nl_raw = b"single line without newline"
    no_nl_fp.write_bytes(no_nl_raw)
    with PolyReader(no_nl_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert r.read() == "single line without newline"
        assert pbar.n == len(no_nl_raw)

    # 6. Mixed LF and CRLF file (newlines as tuple) reconciles cleanly to pbar.total
    mixed_fp = tmp_path / "mixed_newlines.txt"
    mixed_raw = b"line1\nline2\r\nline3\n"
    mixed_fp.write_bytes(mixed_raw)
    with PolyReader(mixed_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert list(r) == ["line1\n", "line2\n", "line3\n"]
        assert pbar.n == len(mixed_raw)

    # 7. CR-only file reads cleanly to pbar.total
    cr_fp = tmp_path / "cr_only.txt"
    cr_raw = b"one\rtwo\r"
    cr_fp.write_bytes(cr_raw)
    with PolyReader(cr_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert list(r) == ["one\n", "two\n"]
        assert pbar.n == len(cr_raw)


@pytest.mark.parametrize(
    "bad_url,secret,expected_fragment",
    [
        (
            "http://user:secret@[bad/a.txt?token=1",
            "secret",
            "http://user:***@[bad/a.txt?<redacted>",
        ),
        (
            "http://user:secret@[bad?token=1",
            "secret",
            "http://user:***@[bad?<redacted>",
        ),
        (
            "http://user:p@ss@[bad/a.txt?token=1#frag",
            "p@ss",
            "http://user:***@[bad/a.txt?<redacted>#frag",
        ),
    ],
)
def test_repr_redacts_malformed_ipv6_urls_without_raising(bad_url, secret, expected_fragment):
    """iev841 E-04: repr(PolyReader) and repr(PolyWriter) redact malformed URLs without raising ValueError."""
    for obj in (PolyReader(bad_url), PolyWriter(bad_url)):
        rep = repr(obj)
        assert secret not in rep
        assert "token=1" not in rep
        assert expected_fragment in rep
