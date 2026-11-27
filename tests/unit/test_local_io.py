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
from openas import OpenAsReader, OpenAsWriter, openas

def test_plain_text(tmp_path):
    data = ["hello", "world", "this", "is", "a", "test"]
    filepath = str(tmp_path / "test.txt")

    # Test writing
    with OpenAsWriter(filepath) as writer:
        for line in data:
            writer.write(line + '\n')
    
    assert os.path.exists(filepath)
    with open(filepath, 'r', encoding='utf-8') as f:
        assert f.read().splitlines() == data

    # Test reading
    read_data = []
    with OpenAsReader(filepath) as reader:
        for line in reader:
            read_data.append(line.rstrip('\n'))
            
    assert read_data == data

@pytest.mark.parametrize("ext", [".gz", ".bz2", ".zst", ".xz"])
def test_compressed_files(tmp_path, ext):
    data = ["line1 compressed", "line2 compressed", f"some data for {ext}"]
    filepath = str(tmp_path / f"test_data{ext}")

    # Write compressed
    with OpenAsWriter(filepath) as writer:
        for line in data:
            writer.write(line + '\n')

    assert os.path.exists(filepath)
    
    # Read compressed
    read_data = []
    with OpenAsReader(filepath) as reader:
        for line in reader:
            read_data.append(line.rstrip('\n'))
            
    assert read_data == data

def test_openas_shorthand(tmp_path):
    filepath = str(tmp_path / "shorthand_test.zst")
    data = ["openas", "shorthand", "rocks"]
    
    # Test writing
    with openas(filepath, 'w') as writer:
        for line in data:
            writer.write(line + '\n')
            
    assert os.path.exists(filepath)
    
    # Test appending
    append_data = ["another", "line"]
    with openas(filepath, 'a') as writer:
        for line in append_data:
            writer.write(line + '\n')
            
    read_appended = []
    with openas(filepath, 'r') as reader:
        for line in reader:
            read_appended.append(line.rstrip('\n'))
            
    assert read_appended == data + append_data

def test_backup_rotation(tmp_path):
    filepath = str(tmp_path / "backup.txt")
    
    with openas(filepath, 'w', backup=True) as w:
        w.write("First.\n")
        
    with openas(filepath, 'w', backup=True) as w:
        w.write("Second.\n")
        
    backup_path = f"{filepath}.001.bu"
    assert os.path.exists(backup_path)
    
    with open(backup_path, 'r', encoding='utf-8') as f:
        assert f.read() == "First.\n"

@pytest.mark.parametrize("ext", [".gz", ".bz2", ".zst", ".xz"])
@pytest.mark.parametrize("binary", [False, True])
def test_rigorous_line_chunking(tmp_path, ext, binary):
    """
    Rigorously assess how CPython buffers streaming algorithms across native memory constraints.
    Default IO chunks are routinely 131,072 bytes. This suite generates payloads explicitly 
    engineered to collide cleanly across chunk frames in both text ('r'/'w') and binary ('rb'/'wb') modes.
    """
    filepath = str(tmp_path / f"rigorous_chunk_test_{'bin' if binary else 'txt'}{ext}")
    
    # Constructing lines to meet explicit chunking edge logic
    CHUNK_SIZE = 131072
    text_lines = [
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
    lines_to_write = [line.encode("utf-8") for line in text_lines] if binary else text_lines
    
    with OpenAsWriter(filepath, binary=binary) as writer:
        for line in lines_to_write:
            writer.write(line)
            
    read_data = []
    with OpenAsReader(filepath, binary=binary) as reader:
        for line in reader:
             read_data.append(line)
             
    # Asserting exact identical retrieval
    assert len(read_data) == 12
    expected_type = bytes if binary else str
    assert all(isinstance(line, expected_type) for line in read_data)
    assert read_data == lines_to_write


def test_uppercase_extension_local(tmp_path):
    """BUG-03: Uppercase local extensions decompress and compress cleanly."""
    fp = tmp_path / "DATA.GZ"
    with openas(fp, "w") as w:
        w.write("upper gz\n")
    with openas(fp, "r") as r:
        assert r.read() == "upper gz\n"


def test_close_reopen_idempotency_and_sibling_cleanup(tmp_path):
    """BUG-04: Reopen after close, idempotent close, and cleanup on failing close or partial open."""
    fp = tmp_path / "lifecycle.txt"
    with openas(fp, "w") as w:
        w.write("line1\nline2\n")

    reader = OpenAsReader(fp).open()
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
    partial_reader = OpenAsReader(fp, show_progress=True)
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
        with openas(target, "w", backup=True) as w:
            w.write("v2\n")
    assert (tmp_path / "race.txt.001.bu").read_text(encoding="utf-8") == "racer\n"
    assert (tmp_path / "race.txt.002.bu").read_text(encoding="utf-8") == "v1\n"

    # Fallback to os.rename on EOPNOTSUPP and EXDEV
    for err_code in (errno.EOPNOTSUPP, errno.EXDEV):
        fb = tmp_path / f"fb_{err_code}.txt"
        fb.write_text("old\n", encoding="utf-8")
        with patch("os.link", side_effect=OSError(err_code, "unsupported")):
            with openas(fb, "w", backup=True) as w:
                w.write("new\n")
        assert (tmp_path / f"fb_{err_code}.txt.001.bu").read_text(encoding="utf-8") == "old\n"
        assert fb.read_text(encoding="utf-8") == "new\n"

    # Rapid post-999 nanosecond collision retry preserves both backups
    p999 = tmp_path / "full.txt"
    for i in range(1, 1000):
        (tmp_path / f"full.txt.{i:03d}.bu").touch()
    p999.write_text("first\n", encoding="utf-8")
    with patch("time.time_ns", return_value=1700000000000000000):
        with openas(p999, "w", backup=True) as w:
            w.write("second\n")
        with openas(p999, "w", backup=True) as w:
            w.write("third\n")
    assert (tmp_path / "full.txt.1700000000000000000.bu").read_text(encoding="utf-8") == "first\n"
    assert (tmp_path / "full.txt.1700000000000000001.bu").read_text(encoding="utf-8") == "second\n"

    # Multi-frame concatenated .zst reads all frames
    zpath = tmp_path / "multi.zst"
    cctx = zstd.ZstdCompressor()
    zpath.write_bytes(cctx.compress(b"frame1\n") + cctx.compress(b"frame2\n"))
    with OpenAsReader(zpath) as r:
        assert list(r) == ["frame1\n", "frame2\n"]


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_pathlike_utf8_and_file_object_methods(tmp_path, ext):
    """BUG-06: PathLike[str]/PathLike[bytes], UTF-8 round-trip, read/readline/readlines/flush, and closed ValueError."""
    fp = tmp_path / f"utf8{ext}"
    with openas(fp, "w") as w:
        w.write("café 日本語\nsecond line\n")
        w.flush()
    with pytest.raises(ValueError):
        w.flush()

    with openas(fp, "r") as r:
        assert r.readline() == "café 日本語\n"
        assert r.readlines() == ["second line\n"]
    with openas(fp, "r") as r:
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
    with openas(bp, "w") as w:
        w.write("bytes path ok\n")
    with openas(bp, "r") as r:
        assert r.read() == "bytes path ok\n"


@pytest.mark.parametrize("ext", [".gz", ".bz2", ".xz", ".zst"])
def test_compressed_progress_tracks_raw_bytes(tmp_path, ext):
    """BUG-07: show_progress=True tracks exact raw compressed bytes so progress.n == progress.total."""
    payload = ("0123456789abcdef" * 6 + "\n") * 520
    fp = tmp_path / f"progress{ext}"
    with openas(fp, "w") as w:
        w.write(payload)

    raw_size = os.path.getsize(fp)
    reader = OpenAsReader(fp, show_progress=True).open()
    try:
        content = reader.read()
        assert content == payload
        assert reader._progress is not None
        assert reader._progress.total == raw_size
        assert reader._progress.n == reader._progress.total
    finally:
        reader.close()


def test_lazy_imports_and_ftp_session_wrapper_factory():
    """PERF-06 / ckmclg E-05: import openas loads no optional stdlib/third-party modules; FTPSessionWrapper is internal to openas.openas."""
    script = (
        "import sys; "
        "before = set(sys.modules); "
        "import openas; "
        "added = set(sys.modules) - before; "
        "forbidden = ("
        "'argparse', 'ftplib', 'ssl', 'bz2', 'gzip', 'lzma', 'urllib.parse', "
        "'paramiko', 'ftputil', 'requests', 'zstandard', 'tqdm'"
        "); "
        "loaded = [m for m in forbidden if m in added]; "
        "assert loaded == [], f'unexpected eager imports: {loaded}'; "
        "assert 'FTPSessionWrapper' not in openas.__all__; "
        "assert not hasattr(openas, 'FTPSessionWrapper'); "
        "from openas.openas import FTPSessionWrapper as A; "
        "from openas.openas import FTPSessionWrapper as B; "
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
def test_openas_reader_for_loop_direct_next_and_file_uri(tmp_path, ext):
    """PERF-02: OpenAsReader supports for-iteration, direct next(), and file: URLs."""
    expected = [f"row-{i}\n" for i in range(10)]
    fp = tmp_path / f"iter_check{ext}"
    with OpenAsWriter(fp) as w:
        for line in expected:
            w.write(line)

    with OpenAsReader(fp) as r:
        assert list(r) == expected

    with OpenAsReader(fp) as r:
        got = []
        while True:
            try:
                got.append(next(r))
            except StopIteration:
                break
        assert got == expected

    if ext == ".txt":
        with OpenAsReader(fp.as_uri()) as r:
            assert list(r) == expected


def test_openas_reader_progress_batched_full_and_early_break_flush(tmp_path):
    """PERF-02: show_progress=True flushes exact remainder on EOF and early break + close."""
    # Size: 7,500 * 10 = 75,000 bytes (not a multiple of 65,536).
    lines = [f"{i:09d}\n" for i in range(7500)]
    fp = tmp_path / "ascii_progress.txt"
    fp.write_bytes("".join(lines).encode("utf-8"))
    expected_size = os.path.getsize(fp)
    assert expected_size % 65536 != 0

    reader = OpenAsReader(fp, show_progress=True).open()
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
    reader2 = OpenAsReader(fp, show_progress=True).open()
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
    reader3 = OpenAsReader(fp, show_progress=True).open()
    pbar3 = reader3._progress
    assert pbar3 is not None
    it = iter(reader3)
    yielded3 = [next(it) for _ in range(7)]
    reader3.close()
    assert pbar3.n == sum(len(line) for line in yielded3)


def test_openas_writer_writelines_subclass_override_and_closed_error(tmp_path):
    """PERF-08: OpenAsWriter.writelines(), subclass write() override preservation, and closed ValueError."""
    fp = tmp_path / "writelines.txt"
    payload = ["alpha\n", "beta\n", "gamma\n"]
    writer = OpenAsWriter(fp).open(append=False, backup=False)
    writer.writelines(payload)
    writer.close()
    assert fp.read_text(encoding="utf-8") == "".join(payload)

    with pytest.raises(ValueError):
        writer.write("after close\n")
    with pytest.raises(ValueError):
        writer.writelines(["after close\n"])

    class CountingWriter(OpenAsWriter):
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
    with OpenAsReader(url) as r:
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

    monkeypatch.setattr(sys.modules["openas.openas"], "_get_http_session", lambda: FakeSession())

    with OpenAsReader("http://example.invalid/plain.txt") as r:
        assert list(r) == ["a\n", "\n", "b\n"]

    with OpenAsReader("http://example.invalid/plain.txt") as r:
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
        with OpenAsWriter(fp_default).open(backup=False) as w:
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
        with OpenAsWriter(fp_explicit).open(backup=False, compresslevel=2) as w:
            w.write(payload)
        _, call_kwargs = mock_comp.call_args
        if ext == ".zst":
            assert call_kwargs == {"level": 2}
        elif ext in (".gz", ".bz2"):
            assert call_kwargs["compresslevel"] == 2
        else:
            assert call_kwargs["preset"] == 2

        mock_comp.reset_mock()
        with openas(fp_shorthand, "w", backup=False, compresslevel=1) as w:
            w.write(payload)
        _, call_kwargs = mock_comp.call_args
        if ext == ".zst":
            assert call_kwargs == {"level": 1}
        elif ext in (".gz", ".bz2"):
            assert call_kwargs["compresslevel"] == 1
        else:
            assert call_kwargs["preset"] == 1

    for path in (fp_default, fp_explicit, fp_shorthand):
        with OpenAsReader(path) as r:
            assert r.read() == payload


def test_local_backup_fast_path_including_bare_relative_filename(tmp_path, monkeypatch):
    """p1f3dc E-01: Local .001.bu fast-path avoids os.scandir, including for bare relative filenames."""
    monkeypatch.chdir(tmp_path)
    bare = "bare_relative.txt"
    with OpenAsWriter(bare).open(backup=True) as w:
        w.write("v1\n")

    with patch("os.scandir", wraps=os.scandir) as mock_scandir:
        with OpenAsWriter(bare).open(backup=True) as w:
            w.write("v2\n")
        mock_scandir.assert_not_called()

    assert (tmp_path / "bare_relative.txt.001.bu").read_text(encoding="utf-8") == "v1\n"
    assert (tmp_path / bare).read_text(encoding="utf-8") == "v2\n"

    # Second overwrite scans "." once and selects .002.bu
    with patch("os.scandir", wraps=os.scandir) as mock_scandir:
        with OpenAsWriter(bare).open(backup=True) as w:
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
        with OpenAsWriter(target).open(backup=True) as w:
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
        with OpenAsWriter(meta_target).open(backup=True) as w:
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
            with OpenAsWriter(target).open(backup=True) as w:
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
        openas(existing_dir, "w", backup=True)

    assert existing_dir.is_dir()
    assert (existing_dir / "keep.txt").read_text(encoding="utf-8") == "precious\n"
    assert not (tmp_path / "important_dir.001.bu").exists()

    # Broken symlink at .001.bu is skipped on fallback rather than silently overwritten
    src = tmp_path / "target.txt"
    src.write_text("v1\n", encoding="utf-8")
    broken_bu = tmp_path / "target.txt.001.bu"
    try:
        broken_bu.symlink_to(tmp_path / "does_not_exist")
    except OSError:
        if os.name == "nt":
            pytest.skip("Windows symlink creation requires Developer Mode or elevated privileges")
        raise
    with patch("os.link", side_effect=OSError(errno.EOPNOTSUPP, "no hard links")):
        with openas(src, "w", backup=True) as w:
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
    with openas(target, "w", backup=False) as w:
        w.write("original\n")

    with pytest.raises(exc_type):
        openas(target, "w", backup=True, compresslevel=bad_level)

    assert not os.path.exists(f"{target}.001.bu")
    with openas(target, "r") as r:
        assert r.read() == "original\n"


def test_local_non_ascii_progress_and_mixed_readline_iter_no_double_count(tmp_path):
    """S2-B8: Plain local UTF-8 progress tracks exact byte length and mixed readline()+iter does not double-count."""
    fp = tmp_path / "utf8_prog.txt"
    lines = ["café 日本語 🚀\n" for _ in range(10)]
    payload = "".join(lines)
    fp.write_bytes(payload.encode("utf-8"))
    expected_bytes = len(payload.encode("utf-8"))
    assert expected_bytes > len(payload)

    with OpenAsReader(fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert pbar.total == expected_bytes
        first = r.readline()
        rest = list(r)
        assert [first, *rest] == lines
    assert pbar.n == expected_bytes


def test_file_like_attributes_repr_redaction_and_unknown_pkg_attr(tmp_path):
    """S5-F1 & S3-T1: OpenAsReader/OpenAsWriter expose closed/readable/writable/seekable/encoding/redacted __repr__."""
    import openas as pkg

    fp = tmp_path / "attrs.txt"
    w = OpenAsWriter(fp)
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

    r = OpenAsReader(fp)
    assert r.closed is True
    assert r.readable() is True and r.writable() is False and r.seekable() is False
    assert r.encoding == "utf-8"
    with r:
        assert r.closed is False
        assert r.readlines(hint=4) == ["rewritten\n"]
    assert r.closed is True

    secret_reader = OpenAsReader("sftp://alice:mysecret@host/data.txt?token=tok")
    rep = repr(secret_reader)
    assert "mysecret" not in rep and "tok" not in rep and "***" in rep

    with pytest.raises(AttributeError):
        getattr(pkg, "NonExistentSymbol123")


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_local_empty_no_newline_multimember_append_file_uri_and_colon_filename(tmp_path, ext):
    """S3-T1: Empty files, missing trailing newline, multi-member append, file:// writer (with spaces), and colons."""
    # Empty file round-trip (including show_progress=True)
    empty_fp = tmp_path / f"empty{ext or '.txt'}"
    with openas(empty_fp, "w", backup=False):
        pass
    with openas(empty_fp, "r", show_progress=True) as r:
        assert r.read() == ""

    # Missing trailing newline + multi-member append across all formats
    app_fp = tmp_path / f"append{ext or '.txt'}"
    with openas(app_fp, "w", backup=False) as w:
        w.write("part1\n")
    with openas(app_fp, "a") as w:
        w.write("part2_no_nl")
    with openas(app_fp, "r") as r:
        assert r.read() == "part1\npart2_no_nl"

    # file:// URI write/append/read with space (%20) and local colon filename
    spaced_dir = tmp_path / "dir with space"
    spaced_dir.mkdir()
    spaced_fp = spaced_dir / f"doc{ext or '.txt'}"
    file_uri = spaced_fp.as_uri()
    with openas(file_uri, "w", backup=False) as w:
        w.write("uri line 1\n")
    with openas(file_uri, "a") as w:
        w.write("uri line 2\n")
    with openas(file_uri, "r") as r:
        assert r.read() == "uri line 1\nuri line 2\n"

    if os.name != "nt":
        colon_fp = tmp_path / f"2026:11:25{ext or '.txt'}"
        with openas(colon_fp, "w", backup=False) as w:
            w.write("colon ok\n")
        with openas(colon_fp, "r") as r:
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

    with openas(fp, "wb", backup=False) as w:
        assert w.binary is True
        assert w.encoding is None
        assert "mode='wb'" in repr(w)
        assert w.write(initial_lines[0]) == len(initial_lines[0])
        w.writelines(initial_lines[1:])
        with pytest.raises(TypeError):
            w.write("str rejected in binary mode\n")
        with pytest.raises(TypeError):
            w.writelines(["str line rejected\n"])

    with openas(fp, "ab") as w:
        assert w.binary is True
        assert w.encoding is None
        assert w.write(appended) == len(appended)

    # Full read(), partial read(size), readline(), next(), readlines(), and for-loop iteration
    with openas(fp, "rb") as r:
        assert r.binary is True
        assert r.encoding is None
        assert "mode='rb'" in repr(r)
        got = r.read()
        assert isinstance(got, bytes)
        assert got == expected_bytes

    with openas(fp, "rb") as r:
        got_lines = list(r)
        assert all(isinstance(line, bytes) for line in got_lines)
        assert got_lines == expected_lines

    with openas(fp, "rb") as r:
        assert r.readline() == initial_lines[0]
        assert next(r) == initial_lines[1]
        assert r.readlines() == [initial_lines[2], appended]

    with openas(fp, "rb") as r:
        assert r.read(4) == b"\x00\xff\xfe\x80"
        assert r.readline() == b"line1\n"

    # Multi-chunk (>128 KiB) binary line streaming with show_progress=True
    big_fp = tmp_path / f"big_binary{ext or '.bin'}"
    big_lines = [bytes([0x80 + (i % 128)]) + b"\xff\x00" + f"row-{i:05d}-".encode("ascii") + (b"X" * 60) + b"\n" for i in range(2200)]
    with OpenAsWriter(big_fp, binary=True).open(backup=False) as w:
        w.writelines(big_lines)

    raw_size = big_fp.stat().st_size
    with OpenAsReader(big_fp, show_progress=True, binary=True) as r:
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
    with OpenAsReader(crlf_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert pbar.total == crlf_size
        assert list(r) == ["alpha\n", "beta\n", "gamma\n"]
        assert pbar.n == crlf_size
    assert pbar.n == crlf_size

    # 2. Full read() reaches pbar.n == pbar.total
    with OpenAsReader(crlf_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert r.read() == "alpha\nbeta\ngamma\n"
        assert pbar.n == crlf_size

    # 3. readline() loop reaches pbar.n == pbar.total at natural EOF, while readline(0) mid-file does not jump
    with OpenAsReader(crlf_fp, show_progress=True) as r:
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
    with OpenAsReader(crlf_fp, show_progress=True) as r:
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
    with OpenAsReader(no_nl_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert r.read() == "single line without newline"
        assert pbar.n == len(no_nl_raw)

    # 6. Mixed LF and CRLF file (newlines as tuple) reconciles cleanly to pbar.total
    mixed_fp = tmp_path / "mixed_newlines.txt"
    mixed_raw = b"line1\nline2\r\nline3\n"
    mixed_fp.write_bytes(mixed_raw)
    with OpenAsReader(mixed_fp, show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert list(r) == ["line1\n", "line2\n", "line3\n"]
        assert pbar.n == len(mixed_raw)

    # 7. CR-only file reads cleanly to pbar.total
    cr_fp = tmp_path / "cr_only.txt"
    cr_raw = b"one\rtwo\r"
    cr_fp.write_bytes(cr_raw)
    with OpenAsReader(cr_fp, show_progress=True) as r:
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
    """iev841 E-04: repr(OpenAsReader) and repr(OpenAsWriter) redact malformed URLs without raising ValueError."""
    for obj in (OpenAsReader(bad_url), OpenAsWriter(bad_url)):
        rep = repr(obj)
        assert secret not in rep
        assert "token=1" not in rep
        assert expected_fragment in rep


def test_openas_writer_repr_modes_open_and_closed(tmp_path):
    """n5d1ot E-01: OpenAsWriter.__repr__ reports 'w', 'wb', 'a', 'ab' accurately when open and resets on close."""
    fp = tmp_path / "repr_modes.txt"

    # Unopened text and binary writers report closed with 'w' / 'wb'
    w_txt = OpenAsWriter(fp, binary=False)
    assert repr(w_txt) == f"OpenAsWriter({str(fp)!r}, mode='w', state='closed')"
    w_bin = OpenAsWriter(fp, binary=True)
    assert repr(w_bin) == f"OpenAsWriter({str(fp)!r}, mode='wb', state='closed')"

    # Write mode ('w' and 'wb') via openas()
    with openas(fp, "w", backup=False) as w:
        assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='w', state='open')"
    assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='w', state='closed')"

    with openas(fp, "wb", backup=False) as w:
        assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='wb', state='open')"
    assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='wb', state='closed')"

    # Append mode ('a' and 'ab') reports 'a' / 'ab' while open and resets to 'w' / 'wb' when closed
    with openas(fp, "a") as w:
        assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='a', state='open')"
        # Calling open(append=False) on an already-open append writer returns early without mutating _append
        assert w.open(append=False, backup=False) is w
        assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='a', state='open')"
    assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='w', state='closed')"

    with openas(fp, "ab") as w:
        assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='ab', state='open')"
        assert w.open(append=False, backup=False) is w
        assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='ab', state='open')"
    assert repr(w) == f"OpenAsWriter({str(fp)!r}, mode='wb', state='closed')"

    # Direct OpenAsWriter.open(append=True, backup=False) then close() and reopen(append=False)
    w_direct = OpenAsWriter(fp)
    w_direct.open(append=True, backup=False)
    try:
        assert repr(w_direct) == f"OpenAsWriter({str(fp)!r}, mode='a', state='open')"
    finally:
        w_direct.close()
    assert repr(w_direct) == f"OpenAsWriter({str(fp)!r}, mode='w', state='closed')"
    with w_direct.open(append=False, backup=False):
        assert repr(w_direct) == f"OpenAsWriter({str(fp)!r}, mode='w', state='open')"
    assert repr(w_direct) == f"OpenAsWriter({str(fp)!r}, mode='w', state='closed')"


@pytest.mark.parametrize("ext", [".gz", ".bz2", ".xz", ".zst"])
@pytest.mark.parametrize("binary", [False, True])
def test_compressed_progress_eof_before_close_early_break_and_mixed_reads(tmp_path, ext, binary):
    """a224p3 E-02/E-03: _CountingByteStream flushes exact raw_size at EOF before close, on early break, and on mixed reads."""
    text_lines = [f"row-{i:05d}-café-" + ("0123456789abcdef" * 4) + "\n" for i in range(1500)]
    payload_text = "".join(text_lines)
    expected_lines = [line.encode("utf-8") for line in text_lines] if binary else text_lines
    fp = tmp_path / f"counted_prog{ext}"
    with OpenAsWriter(fp, binary=True).open(backup=False) as w:
        w.write(payload_text.encode("utf-8"))
    raw_size = fp.stat().st_size

    # 1. Full iteration hits exact raw_size AT EOF BEFORE close() and stays exact after close()
    reader = OpenAsReader(fp, show_progress=True, binary=binary).open()
    try:
        pbar = reader._progress
        assert pbar is not None
        assert pbar.total == raw_size
        seen = list(reader)
        assert seen == expected_lines
        assert pbar.n == raw_size
    finally:
        reader.close()
    assert pbar.n == raw_size

    # 2. Early break flushes pending bytes cleanly on close() without exceeding raw_size
    reader_early = OpenAsReader(fp, show_progress=True, binary=binary).open()
    pbar_early = reader_early._progress
    assert pbar_early is not None
    partial = []
    for line in reader_early:
        partial.append(line)
        if len(partial) == 3:
            break
    reader_early.close()
    assert len(partial) == 3
    assert 0 < pbar_early.n <= raw_size

    # 3. Mixed readline() + read(10) + iteration never double-counts
    reader_mix = OpenAsReader(fp, show_progress=True, binary=binary).open()
    try:
        pbar_mix = reader_mix._progress
        assert pbar_mix is not None
        first_line = reader_mix.readline()
        head = reader_mix.read(10)
        rest = list(reader_mix)
        if binary:
            assert first_line + head + b"".join(rest) == payload_text.encode("utf-8")
        else:
            assert first_line + head + "".join(rest) == payload_text
        assert pbar_mix.n == raw_size
    finally:
        reader_mix.close()
    assert pbar_mix.n == raw_size


def test_counted_stream_open_header_bytes_not_double_counted(tmp_path):
    """a224p3 E-03: Header bytes read during open() initialize tqdm once and are never double-counted."""
    import io as _io

    raw_payload = b"HDR!line1\nline2\nline3\n"
    raw_size = len(raw_payload)

    class HeaderReadingReader(OpenAsReader):
        def _wrap_local(self, parsed=None):
            self._raw_fh = _io.BytesIO(raw_payload)
            stream = self._make_counted_stream(self._raw_fh)
            header = stream.read(4)
            assert header == b"HDR!"
            self._fh = _io.TextIOWrapper(stream, encoding="utf-8")

        def _get_file_size(self):
            return raw_size

    reader = HeaderReadingReader(tmp_path / "virtual_header.txt", show_progress=True).open()
    try:
        pbar = reader._progress
        assert pbar is not None
        assert pbar.total == raw_size
        assert pbar.n == 4
        assert list(reader) == ["line1\n", "line2\n", "line3\n"]
        assert pbar.n == raw_size
    finally:
        reader.close()
    assert pbar.n == raw_size


@pytest.mark.parametrize("ext", [".txt", ".gz", ".bz2", ".xz", ".zst"])
@pytest.mark.parametrize("binary", [False, True])
def test_openas_reader_readlines_hints_binary_and_progress(tmp_path, ext, binary):
    """a224p3 E-05: readlines() handles hint=-1/0/None/positive, binary mode, and show_progress=True without double-counting."""
    lines = [f"line-{i:03d}-café-payload\n" for i in range(40)]
    expected = [line.encode("utf-8") for line in lines] if binary else lines
    fp = tmp_path / f"readlines_sample{ext}"
    with OpenAsWriter(fp, binary=True).open(backup=False) as w:
        w.writelines([line.encode("utf-8") for line in lines])
    raw_size = fp.stat().st_size

    # Unbounded hints (-1, 0, None) with show_progress=False and show_progress=True
    for show_progress in (False, True):
        for hint in (-1, 0, None):
            with OpenAsReader(fp, show_progress=show_progress, binary=binary) as r:
                pbar = r._progress
                got = r.readlines() if hint == -1 else r.readlines(hint)
                assert got == expected
                if show_progress:
                    assert pbar is not None
                    assert pbar.n == raw_size
            if show_progress:
                assert pbar.n == raw_size

    # Positive hint followed by remaining readlines()
    for show_progress in (False, True):
        with OpenAsReader(fp, show_progress=show_progress, binary=binary) as r:
            pbar = r._progress
            first_batch = r.readlines(50)
            assert 0 < len(first_batch) < len(expected)
            rest_batch = r.readlines()
            assert first_batch + rest_batch == expected
            if show_progress:
                assert pbar is not None
                assert pbar.n == raw_size
        if show_progress:
            assert pbar.n == raw_size

    # Plain local CRLF readlines() with show_progress=True reconciles to raw_size at EOF and not early on positive hint
    if ext == ".txt" and not binary:
        crlf_fp = tmp_path / "readlines_crlf.txt"
        crlf_raw = b"alpha\r\nbeta\r\ngamma\r\n"
        crlf_fp.write_bytes(crlf_raw)
        with OpenAsReader(crlf_fp, show_progress=True) as r:
            pbar = r._progress
            assert pbar is not None
            assert r.readlines() == ["alpha\n", "beta\n", "gamma\n"]
            assert pbar.n == len(crlf_raw)
        with OpenAsReader(crlf_fp, show_progress=True) as r:
            pbar = r._progress
            assert pbar is not None
            head = r.readlines(2)
            assert head == ["alpha\n"]
            assert pbar.n < len(crlf_raw)
            tail = r.readlines()
            assert tail == ["beta\n", "gamma\n"]
            assert pbar.n == len(crlf_raw)


@pytest.mark.parametrize(
    "ext,kind",
    [
        (".gz", "corrupt"),
        (".gz", "truncated"),
        (".bz2", "corrupt"),
        (".bz2", "truncated"),
        (".xz", "corrupt"),
        (".xz", "truncated"),
        (".zst", "corrupt"),
        (".zst", "truncated"),
    ],
)
@pytest.mark.parametrize("mode", ["r", "rb"])
@pytest.mark.parametrize("show_progress", [False, True])
def test_corrupt_and_truncated_compressed_streams_raise_and_close_handles(
    tmp_path, ext, kind, mode, show_progress
):
    """was3ig E-03: Corrupt and truncated compressed streams raise codec errors and close all handles cleanly."""
    import zlib

    sample_bytes = ("corrupt-or-truncated payload line 0123456789abcdef\n" * 200).encode("utf-8")
    if kind == "corrupt":
        wire = b"\x00\x01\x02not-valid-compressed-stream\xff\xfe" * 32
    else:
        if ext == ".gz":
            full_wire = gzip.compress(sample_bytes)
        elif ext == ".bz2":
            full_wire = bz2.compress(sample_bytes)
        elif ext == ".xz":
            full_wire = lzma.compress(sample_bytes)
        else:
            full_wire = zstd.ZstdCompressor().compress(sample_bytes)
        wire = full_wire[: max(10, len(full_wire) // 2)]

    fp = tmp_path / f"{kind}_{mode}_{show_progress}{ext}"
    fp.write_bytes(wire)

    if ext == ".gz":
        expected_exc = (OSError, EOFError, zlib.error)
    elif ext == ".bz2":
        expected_exc = (OSError, EOFError)
    elif ext == ".xz":
        expected_exc = (lzma.LZMAError, EOFError)
    else:
        expected_exc = (zstd.ZstdError, EOFError)

    reader = OpenAsReader(fp, show_progress=show_progress, binary=(mode == "rb"))
    try:
        with pytest.raises(expected_exc):
            with reader:
                reader.read()
    finally:
        assert reader.closed is True
        assert reader._fh is None
        assert reader._raw_fh is None


@pytest.mark.parametrize("mode", ["r", "rb"])
@pytest.mark.parametrize("show_progress", [False, True])
def test_truncated_zst_stream_raises_and_closes_handles_without_resource_warning(tmp_path, mode, show_progress):
    """hxile6: Truncated .zst streams raise EOFError across offsets, multi-frame, and skippable frames while closing handles."""
    import struct

    # 1. Exhaustive byte-offset truncation across default, checksum, no-content-size, RLE, and empty frames
    for label, cctx, payload in (
        ("default", zstd.ZstdCompressor(), b"truncated zst line 0123456789\n" * 20),
        ("checksum", zstd.ZstdCompressor(write_checksum=True), b"checksum zst line\n" * 20),
        ("no_fcs", zstd.ZstdCompressor(write_content_size=False), b"no content size\n" * 20),
        ("rle", zstd.ZstdCompressor(write_checksum=True), b"A" * 8192 + b"\n"),
        ("empty", zstd.ZstdCompressor(write_checksum=True), b""),
    ):
        full_wire = cctx.compress(payload)
        fp_full = tmp_path / f"valid_{label}_{mode}_{show_progress}.zst"
        fp_full.write_bytes(full_wire)
        with OpenAsReader(fp_full, show_progress=show_progress, binary=(mode == "rb")) as r:
            expected = payload if mode == "rb" else payload.decode("utf-8")
            assert r.read() == expected

        # Verify early/mid/last-byte truncation points raise cleanly and close handles
        cuts = sorted({1, 2, 4, len(full_wire) // 2, len(full_wire) - 1})
        for cut in cuts:
            if not (0 < cut < len(full_wire)):
                continue
            fp_bad = tmp_path / f"trunc_{label}_{cut}_{mode}_{show_progress}.zst"
            fp_bad.write_bytes(full_wire[:cut])
            reader = OpenAsReader(fp_bad, show_progress=show_progress, binary=(mode == "rb"))
            with pytest.raises((EOFError, zstd.ZstdError)):
                with reader:
                    reader.read()
            assert reader.closed is True
            assert reader._fh is None
            assert reader._raw_fh is None

    # 2. Multi-frame .zst with skippable frame succeeds when intact and raises when 2nd frame or skippable frame is truncated
    f1 = zstd.ZstdCompressor(write_checksum=True).compress(b"frame1\n")
    f2 = zstd.ZstdCompressor(write_checksum=False).compress(b"frame2\n")
    skippable = struct.pack("<II", 0x184D2A50, 5) + b"HELLO"
    fp_multi = tmp_path / f"multi_skip_{mode}_{show_progress}.zst"
    fp_multi.write_bytes(skippable + f1 + f2)
    with OpenAsReader(fp_multi, show_progress=show_progress, binary=(mode == "rb")) as r:
        expected = b"frame1\nframe2\n" if mode == "rb" else "frame1\nframe2\n"
        assert r.read() == expected

    for bad_name, bad_wire in (
        ("trunc_frame2", skippable + f1 + f2[:-2]),
        ("trunc_skippable", skippable[:-2]),
        ("trailing_partial_magic", f1 + b"\x28\xb5"),
    ):
        fp_bad = tmp_path / f"{bad_name}_{mode}_{show_progress}.zst"
        fp_bad.write_bytes(bad_wire)
        reader = OpenAsReader(fp_bad, show_progress=show_progress, binary=(mode == "rb"))
        with pytest.raises((EOFError, zstd.ZstdError)):
            with reader:
                reader.read()
        assert reader.closed is True
        assert reader._fh is None
        assert reader._raw_fh is None

    # 3. Early break on a valid multi-line .zst file does NOT falsely raise EOFError
    fp_early = tmp_path / f"early_break_{mode}_{show_progress}.zst"
    fp_early.write_bytes(zstd.ZstdCompressor().compress(b"line\n" * 5000))
    with OpenAsReader(fp_early, show_progress=show_progress, binary=(mode == "rb")) as r:
        first = next(iter(r))
        assert first == (b"line\n" if mode == "rb" else "line\n")


@pytest.mark.parametrize("ext", [".txt", ".gz", ".bz2", ".xz", ".zst"])
def test_non_utf8_bytes_raise_unicode_decode_error_in_text_and_succeed_in_binary(tmp_path, ext):
    """was3ig E-03: Non-UTF-8 bytes raise UnicodeDecodeError in 'r' mode (closing handles) and return raw bytes in 'rb'."""
    raw_bytes = b"valid prefix\n\xff\xfe\x80\x81invalid-utf8\n"
    fp = tmp_path / f"non_utf8{ext}"
    with openas(fp, "wb", backup=False) as w:
        w.write(raw_bytes)

    reader = OpenAsReader(fp, show_progress=True)
    with pytest.raises(UnicodeDecodeError):
        with reader:
            reader.read()
    assert reader.closed is True
    assert reader._fh is None
    assert reader._raw_fh is None

    with openas(fp, "rb") as r:
        assert r.read() == raw_bytes


def test_keyboard_interrupt_mid_open_cleans_up_local_handles(tmp_path):
    """was3ig E-03: KeyboardInterrupt (BaseException) mid-open closes acquired local reader and writer handles."""
    zst_fp = tmp_path / "ki_sample.zst"
    with openas(zst_fp, "w", backup=False) as w:
        w.write("hello interrupt\n")

    # 1. OpenAsReader local open interrupted after _raw_fh and _fh are acquired
    r_local = OpenAsReader(zst_fp, show_progress=True)
    captured = {}

    def interrupt_size():
        captured["raw_fh"] = r_local._raw_fh
        raise KeyboardInterrupt("ctrl-c local reader")

    with patch.object(r_local, "_get_file_size", side_effect=interrupt_size):
        with pytest.raises(KeyboardInterrupt, match="ctrl-c local reader"):
            r_local.open()

    assert captured["raw_fh"] is not None
    assert captured["raw_fh"].closed is True
    assert r_local.closed is True
    assert r_local._fh is None
    assert r_local._raw_fh is None

    # 2. OpenAsWriter local open interrupted after _raw_fh is opened
    out_zst = tmp_path / "ki_out.zst"
    w_local = OpenAsWriter(out_zst)
    captured_writer = {}

    def interrupt_stream_writer(self, writer_fh, *args, **kwargs):
        captured_writer["raw_fh"] = writer_fh
        raise KeyboardInterrupt("ctrl-c local writer")

    with patch.object(zstd.ZstdCompressor, "stream_writer", interrupt_stream_writer):
        with pytest.raises(KeyboardInterrupt, match="ctrl-c local writer"):
            w_local.open(backup=False)

    assert captured_writer["raw_fh"] is not None
    assert captured_writer["raw_fh"].closed is True
    assert w_local.closed is True
    assert w_local._fh is None
    assert w_local._raw_fh is None


def test_openas_reader_close_flush_progress_error_alone_and_with_handle_close_error(tmp_path):
    """was3ig E-03: OpenAsReader.close() closes every handle and propagates the primary _flush_progress error."""
    fp = tmp_path / "flush_err.txt"
    fp.write_bytes(b"line 1\nline 2\nline 3\n")

    # 1. _flush_progress raises alone: all handles still close and primary flush error propagates
    reader1 = OpenAsReader(fp, show_progress=True).open()
    orig_fh1 = reader1._fh
    pbar1 = reader1._progress
    assert orig_fh1 is not None and pbar1 is not None
    it1 = iter(reader1)
    assert next(it1) == "line 1\n"
    assert reader1._progress_pending > 0

    pbar1_closed = []
    orig_pbar1_close = pbar1.close

    def spy_pbar1_close():
        pbar1_closed.append(True)
        return orig_pbar1_close()

    pbar1.close = spy_pbar1_close
    pbar1.update = MagicMock(side_effect=RuntimeError("primary flush boom"))

    with pytest.raises(RuntimeError, match="primary flush boom"):
        reader1.close()

    assert orig_fh1.closed is True
    assert pbar1_closed == [True]
    assert reader1.closed is True
    assert reader1._fh is None
    assert reader1._progress is None

    # 2. Both _flush_progress and a handle close raise: every handle still closes and primary flush error wins
    reader2 = OpenAsReader(fp, show_progress=True).open()
    orig_fh2 = reader2._fh
    pbar2 = reader2._progress
    assert orig_fh2 is not None and pbar2 is not None
    it2 = iter(reader2)
    assert next(it2) == "line 1\n"
    assert reader2._progress_pending > 0

    pbar2_closed = []
    orig_pbar2_close = pbar2.close

    def spy_pbar2_close():
        pbar2_closed.append(True)
        return orig_pbar2_close()

    pbar2.close = spy_pbar2_close
    pbar2.update = MagicMock(side_effect=RuntimeError("primary flush boom 2"))

    failing_fh = MagicMock()
    failing_fh.close.side_effect = OSError("secondary fh close boom")
    sibling_ftp = MagicMock()
    reader2._fh = failing_fh
    reader2._ftp = sibling_ftp

    try:
        with pytest.raises(RuntimeError, match="primary flush boom 2"):
            reader2.close()
    finally:
        if not orig_fh2.closed:
            orig_fh2.close()

    assert failing_fh.close.call_count == 1
    assert sibling_ftp.close.call_count == 1
    assert pbar2_closed == [True]
    assert reader2.closed is True
    assert reader2._fh is None
    assert reader2._ftp is None
    assert reader2._progress is None


@pytest.mark.parametrize("ext", [".gz", ".bz2", ".xz", ".zst"])
def test_streaming_memory_bound_o1_tracemalloc(tmp_path, ext):
    """was3ig E-04: Line-by-line streaming over 16 MiB stays O(1) in Python heap across all four codecs."""
    import gc
    import tracemalloc

    line_bytes = (b"0123456789abcdef" * 6) + b"ABCD\n"  # 101 bytes/line
    lines_16m = (16 * 1024 * 1024 // len(line_bytes)) + 1  # > 16 MiB uncompressed
    expected_bytes_16m = lines_16m * len(line_bytes)

    fp16 = tmp_path / f"mem16m{ext}"
    with OpenAsWriter(fp16, binary=True).open(backup=False) as w:
        block = line_bytes * 1024
        full_blocks, rem = divmod(lines_16m, 1024)
        for _ in range(full_blocks):
            w.write(block)
        if rem:
            w.write(line_bytes * rem)

    fp32 = None
    lines_32m = lines_16m * 2
    expected_bytes_32m = lines_32m * len(line_bytes)
    if ext == ".xz":
        fp32 = tmp_path / "mem32m.xz"
        with OpenAsWriter(fp32, binary=True).open(backup=False) as w:
            block = line_bytes * 1024
            full_blocks, rem = divmod(lines_32m, 1024)
            for _ in range(full_blocks):
                w.write(block)
            if rem:
                w.write(line_bytes * rem)

    def _measure_peak(path, mode: str, expected_lines: int, expected_len: int) -> int:
        # Warm up lazy codec imports before measuring streaming heap.
        with openas(path, mode) as warm:
            warm.readline()
        gc.collect()
        tracemalloc.start()
        try:
            count = 0
            total_len = 0
            with openas(path, mode) as reader:
                for line in reader:
                    count += 1
                    total_len += len(line)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert count == expected_lines
        assert total_len == expected_len
        return peak

    for mode in ("r", "rb"):
        peak_16 = _measure_peak(fp16, mode, lines_16m, expected_bytes_16m)
        if ext == ".xz":
            assert peak_16 < 12 * 1024 * 1024, f"{ext} ({mode}) 16 MiB peak {peak_16} >= 12 MiB"
            peak_32 = _measure_peak(fp32, mode, lines_32m, expected_bytes_32m)
            delta = peak_32 - peak_16
            assert delta < 512 * 1024, (
                f"{ext} ({mode}) 32 MiB peak growth {delta} >= 512 KiB (16M={peak_16}, 32M={peak_32})"
            )
        else:
            assert peak_16 < 2 * 1024 * 1024, f"{ext} ({mode}) 16 MiB peak {peak_16} >= 2 MiB"


def test_local_backup_moves_broken_and_valid_symlinks_without_following(tmp_path, monkeypatch):
    """kfvf7i E-02: Local backup=True moves broken and valid symlinks as symlinks across both link paths."""
    probe_link = tmp_path / "_symlink_probe"
    try:
        probe_link.symlink_to(tmp_path / "_probe_target")
    except OSError:
        if os.name == "nt":
            pytest.skip("Windows symlink creation requires Developer Mode or elevated privileges")
        raise
    probe_link.unlink()

    def _run_case(case_dir):
        case_dir.mkdir()
        # 1. Broken symlink: moved to .001.bu, dangling target never created, new regular file at path
        dangling_target = case_dir / "missing_target.txt"
        broken_link = case_dir / "broken.txt"
        broken_link.symlink_to(dangling_target)

        with openas(broken_link, "w", backup=True) as w:
            w.write("fresh regular\n")

        broken_bu = case_dir / "broken.txt.001.bu"
        assert os.path.islink(broken_bu)
        assert os.readlink(broken_bu) == str(dangling_target)
        assert not os.path.lexists(dangling_target)
        assert not os.path.islink(broken_link)
        assert broken_link.read_text(encoding="utf-8") == "fresh regular\n"

        # 2. Valid symlink to existing file: moved to .001.bu without hard-linking or modifying real target
        real_file = case_dir / "real.txt"
        real_file.write_text("precious target\n", encoding="utf-8")
        valid_link = case_dir / "alias.txt"
        valid_link.symlink_to(real_file)

        with openas(valid_link, "w", backup=True) as w:
            w.write("replacement file\n")

        valid_bu = case_dir / "alias.txt.001.bu"
        assert os.path.islink(valid_bu)
        assert os.readlink(valid_bu) == str(real_file)
        assert os.stat(real_file).st_nlink == 1
        assert real_file.read_text(encoding="utf-8") == "precious target\n"
        assert not os.path.islink(valid_link)
        assert valid_link.read_text(encoding="utf-8") == "replacement file\n"

    # Path A: native supports_follow_symlinks behavior (os.link(..., follow_symlinks=False) on POSIX)
    _run_case(tmp_path / "native")

    # Path B: fallback branch when os.link not in os.supports_follow_symlinks
    fallback_dir = tmp_path / "no_follow_support"
    with monkeypatch.context() as mp:
        mp.setattr(os, "supports_follow_symlinks", set())
        link_calls = []
        real_link = os.link

        def spy_link(*args, **kwargs):
            link_calls.append((args, kwargs))
            return real_link(*args, **kwargs)

        mp.setattr(os, "link", spy_link)
        _run_case(fallback_dir)
        assert link_calls == []

        # Existing broken symlink at .001.bu is never overwritten on rename fallback; advances to .002.bu
        pre_bu = fallback_dir / "collide.txt.001.bu"
        pre_bu.symlink_to(fallback_dir / "nonexistent_bu_target")
        target_sym = fallback_dir / "collide.txt"
        dangling2 = fallback_dir / "dangling2.txt"
        target_sym.symlink_to(dangling2)
        with openas(target_sym, "w", backup=True) as w:
            w.write("after collision\n")
        assert os.path.islink(pre_bu)
        assert os.readlink(pre_bu) == str(fallback_dir / "nonexistent_bu_target")
        bu2 = fallback_dir / "collide.txt.002.bu"
        assert os.path.islink(bu2)
        assert os.readlink(bu2) == str(dangling2)
        assert target_sym.read_text(encoding="utf-8") == "after collision\n"

    # Path C: os.link raises FileNotFoundError on a broken symlink (following symlink) while lexists(src) is True
    fnf_dir = tmp_path / "link_fnf"
    fnf_dir.mkdir()
    dangling3 = fnf_dir / "dangling3.txt"
    sym3 = fnf_dir / "sym3.txt"
    sym3.symlink_to(dangling3)
    with patch("os.link", side_effect=FileNotFoundError("followed broken symlink")):
        with patch.object(os, "supports_follow_symlinks", {os.link}):
            with openas(sym3, "w", backup=True) as w:
                w.write("recovered\n")
    bu3 = fnf_dir / "sym3.txt.001.bu"
    assert os.path.islink(bu3)
    assert os.readlink(bu3) == str(dangling3)
    assert not os.path.lexists(dangling3)
    assert not os.path.islink(sym3)
    assert sym3.read_text(encoding="utf-8") == "recovered\n"


def test_keyboard_interrupt_in_close_and_flush_progress_closes_all_remaining_handles(tmp_path):
    """kfvf7i E-03: KeyboardInterrupt in _flush_progress() or _close_handles() still closes all remaining handles."""
    from openas.openas import _close_handles

    # 1. _close_handles: KeyboardInterrupt in first handle still closes all remaining handles
    h1 = MagicMock()
    h1.closed = False
    h1.close.side_effect = KeyboardInterrupt("ctrl-c h1")
    h2 = MagicMock()
    h2.closed = False
    h3 = MagicMock()
    h3.closed = False
    with pytest.raises(KeyboardInterrupt, match="ctrl-c h1"):
        _close_handles(h1, h2, h3)
    assert h1.close.call_count == 1
    assert h2.close.call_count == 1
    assert h3.close.call_count == 1

    fp = tmp_path / "ki_close.txt"
    fp.write_bytes(b"line1\nline2\n")

    # 2. OpenAsReader.close: KeyboardInterrupt in _flush_progress() alone still closes all handles
    r1 = OpenAsReader(fp, show_progress=True).open()
    orig_fh1 = r1._fh
    pbar1 = r1._progress
    assert orig_fh1 is not None and pbar1 is not None
    it1 = iter(r1)
    assert next(it1) == "line1\n"
    pbar1_closed = []
    orig_pbar1_close = pbar1.close

    def spy_pbar1_close():
        pbar1_closed.append(True)
        return orig_pbar1_close()

    pbar1.close = spy_pbar1_close
    pbar1.update = MagicMock(side_effect=KeyboardInterrupt("ctrl-c flush alone"))
    sibling1 = MagicMock()
    sibling1.closed = False
    r1._ssh_client = sibling1

    with pytest.raises(KeyboardInterrupt, match="ctrl-c flush alone"):
        r1.close()
    assert orig_fh1.closed is True
    assert sibling1.close.call_count == 1
    assert pbar1_closed == [True]
    assert r1.closed is True
    assert r1._ssh_client is None

    # 3. OpenAsReader.close: KeyboardInterrupt in _flush_progress() + secondary handle close error
    for secondary_exc in (OSError("secondary os error"), KeyboardInterrupt("secondary ki")):
        r2 = OpenAsReader(fp, show_progress=True).open()
        orig_fh2 = r2._fh
        pbar2 = r2._progress
        assert orig_fh2 is not None and pbar2 is not None
        it2 = iter(r2)
        assert next(it2) == "line1\n"
        pbar2_closed = []
        orig_pbar2_close = pbar2.close

        def spy_pbar2_close(closed_list=pbar2_closed, orig=orig_pbar2_close):
            closed_list.append(True)
            return orig()

        pbar2.close = spy_pbar2_close
        pbar2.update = MagicMock(side_effect=KeyboardInterrupt("primary ki flush"))
        failing_fh = MagicMock()
        failing_fh.closed = False
        failing_fh.close.side_effect = secondary_exc
        sibling2 = MagicMock()
        sibling2.closed = False
        r2._fh = failing_fh
        r2._ssh_client = sibling2
        try:
            with pytest.raises(KeyboardInterrupt, match="primary ki flush"):
                r2.close()
        finally:
            if not orig_fh2.closed:
                orig_fh2.close()
        assert failing_fh.close.call_count == 1
        assert sibling2.close.call_count == 1
        assert pbar2_closed == [True]
        assert r2.closed is True
        assert r2._ssh_client is None


def test_readinto_typed_buffers_and_zero_length_across_stream_adapters():
    """kfvf7i E-04: Typed buffers (array('I')) and zero-length buffers work across all four stream adapters."""
    import array
    import io as _io
    from openas.openas import (
        _BinaryStreamAdapter,
        _CountingByteStream,
        _HttpRawStream,
        _ZstdFrameValidatingStream,
    )

    class ReadOnlyStream:
        """Minimal binary stream supporting only read() (no readinto)."""

        def __init__(self, data: bytes):
            self._buf = _io.BytesIO(data)
            self.closed = False

        def read(self, size: int = -1):
            return self._buf.read() if (size is None or size < 0) else self._buf.read(size)

        def close(self):
            self.closed = True
            self._buf.close()

    class NonBlockingStream:
        """Binary stream whose readinto/read returns None to signal EAGAIN (not EOF)."""

        def __init__(self, *, with_readinto: bool):
            self._with_readinto = with_readinto
            if with_readinto:
                self.readinto = self._readinto

        def _readinto(self, b):
            return None

        def read(self, size: int = -1):
            return None

    payload = b"0123456789ABCDEF"  # 16 bytes (multiple of 4-byte uint32)

    # 1. _BinaryStreamAdapter & _HttpRawStream (delegating readinto and fallback read-only)
    for make_adapter in (
        lambda raw: _BinaryStreamAdapter(raw, readable=True),
        lambda raw: _HttpRawStream(raw),
    ):
        for make_raw in (_io.BytesIO, ReadOnlyStream):
            adapter = make_adapter(make_raw(payload))
            assert adapter.readinto(bytearray(0)) == 0
            assert adapter.readinto(array.array("I")) == 0
            buf = array.array("I", [0, 0, 0, 0])
            n = adapter.readinto(buf)
            assert n == 16
            assert buf.tobytes() == payload
            assert adapter.readinto(buf) == 0
            adapter.close()

        for with_ri in (True, False):
            nb_adapter = make_adapter(NonBlockingStream(with_readinto=with_ri))
            assert nb_adapter.readinto(array.array("I", [0])) is None

    # 2. _CountingByteStream (delegating readinto, fallback read-only, zero-length, and non-blocking None)
    for make_raw in (_io.BytesIO, ReadOnlyStream):
        flushes = []
        counting = _CountingByteStream(make_raw(payload), on_progress=flushes.append, batch_size=1024)
        assert counting.readinto(bytearray(0)) == 0
        assert flushes == []
        buf = array.array("I", [0, 0])  # 8 bytes
        assert counting.readinto(buf) == 8
        assert buf.tobytes() == payload[:8]
        assert counting.bytes_read == 8
        assert flushes == []
        assert counting.readinto(buf) == 8
        assert buf.tobytes() == payload[8:]
        assert counting.bytes_read == 16
        # EOF triggers flush
        assert counting.readinto(buf) == 0
        assert flushes == [16]

    for with_ri in (True, False):
        flushes = []
        counting_nb = _CountingByteStream(
            NonBlockingStream(with_readinto=with_ri),
            on_progress=flushes.append,
            batch_size=1024,
        )
        counting_nb._pending = 5
        assert counting_nb.readinto(array.array("I", [0])) is None
        assert flushes == []

    # 3. _ZstdFrameValidatingStream: valid and truncated .zst read via array('I') across both paths
    full_zst = zstd.ZstdCompressor(write_checksum=True).compress(b"hello zstd typed buffer\n" * 40)
    trunc_zst = full_zst[: max(12, len(full_zst) // 2)]

    for make_raw in (_io.BytesIO, ReadOnlyStream):
        # Valid .zst succeeds and reaches clean EOF
        valid_stream = _ZstdFrameValidatingStream(make_raw(full_zst))
        assert valid_stream.readinto(bytearray(0)) == 0
        collected = bytearray()
        while True:
            arr = array.array("I", [0] * 4)
            n = valid_stream.readinto(arr)
            if n == 0:
                break
            collected.extend(arr.tobytes()[:n])
        assert bytes(collected) == full_zst
        assert valid_stream._state == valid_stream._S_MAGIC

        # Truncated .zst raises EOFError instead of transitioning to _S_PASSTHROUGH
        trunc_stream = _ZstdFrameValidatingStream(make_raw(trunc_zst))
        assert trunc_stream.readinto(bytearray(0)) == 0
        with pytest.raises(EOFError, match="Truncated zstd stream"):
            while True:
                arr = array.array("I", [0] * 4)
                n = trunc_stream.readinto(arr)
                if n == 0:
                    break
        assert trunc_stream._state != trunc_stream._S_PASSTHROUGH

    # Non-blocking None on _ZstdFrameValidatingStream does not falsely trigger EOFError mid-frame
    for with_ri in (True, False):
        nb_zst = _ZstdFrameValidatingStream(NonBlockingStream(with_readinto=with_ri))
        nb_zst._feed(full_zst[:8])
        assert nb_zst.readinto(array.array("I", [0])) is None


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
def test_local_text_encoding_errors_newline_and_csv_writer(tmp_path, ext):
    """ckmclg E-01: encoding='latin-1', errors='replace', newline='' (csv.writer), newline='\\n', and .encoding."""
    import csv

    # 1. latin-1 write/append/read round-trip and .encoding reporting
    latin_fp = tmp_path / f"latin1{ext or '.txt'}"
    with openas(latin_fp, "w", backup=False, encoding="latin-1") as w:
        assert w.encoding == "latin-1"
        w.write("café naïve\n")
    with openas(latin_fp, "a", encoding="latin-1") as w:
        assert w.encoding == "latin-1"
        w.write("über\n")
    with openas(latin_fp, "r", encoding="latin-1") as r:
        assert r.encoding == "latin-1"
        assert r.read() == "café naïve\nüber\n"
    with openas(latin_fp, "rb") as r:
        assert r.encoding is None
        raw = r.read()
        assert raw == f"café naïve{os.linesep}über{os.linesep}".encode("latin-1")

    # 2. Dirty stream decoded cleanly with errors='replace'
    dirty_fp = tmp_path / f"dirty{ext or '.txt'}"
    with openas(dirty_fp, "wb", backup=False) as w:
        w.write(b"ok-\xff\xfe-tail\n")
    with openas(dirty_fp, "r", encoding="utf-8", errors="replace") as r:
        assert r.encoding == "utf-8"
        assert r.read() == "ok-\ufffd\ufffd-tail\n"

    # 3. newline='' with csv.writer preserves exact CRLF without doubled blank rows
    csv_fp = tmp_path / f"table{ext or '.csv'}"
    rows = [["id", "name"], ["1", "alice"], ["2", "bob"]]
    with openas(csv_fp, "w", backup=False, newline="") as w:
        writer = csv.writer(w)
        writer.writerows(rows)
    with openas(csv_fp, "rb") as r:
        assert r.read() == b"id,name\r\n1,alice\r\n2,bob\r\n"
    with openas(csv_fp, "r", newline="") as r:
        assert list(csv.reader(r)) == rows
    with openas(csv_fp, "r", newline="") as r:
        assert list(r) == ["id,name\r\n", "1,alice\r\n", "2,bob\r\n"]

    # 4. newline='\n' writes exact LF bytes regardless of platform os.linesep
    lf_fp = tmp_path / f"lf_only{ext or '.txt'}"
    with openas(lf_fp, "w", backup=False, newline="\n") as w:
        w.write("line1\nline2\n")
    with openas(lf_fp, "rb") as r:
        assert r.read() == b"line1\nline2\n"


def test_local_plain_progress_with_non_utf8_errors_replace_and_newline(tmp_path):
    """ckmclg E-01: Plain-local show_progress=True works with latin-1, ascii+errors='replace', and newline='' without error."""
    import io as _io

    # 1. latin-1 with show_progress=True tracks exact byte length
    latin_fp = tmp_path / "prog_latin1.txt"
    latin_raw = "café\nnaïve\nüber\n".encode("latin-1")
    latin_fp.write_bytes(latin_raw)
    with OpenAsReader(latin_fp, show_progress=True, encoding="latin-1") as r:
        pbar = r._progress
        assert pbar is not None
        assert pbar.total == len(latin_raw)
        assert list(r) == ["café\n", "naïve\n", "über\n"]
        assert pbar.n == len(latin_raw)

    # 2. Lossy ascii + errors='replace' produces U+FFFD which cannot strict-encode to ascii; must not raise UnicodeEncodeError
    dirty_fp = tmp_path / "prog_dirty.txt"
    dirty_fp.write_bytes(b"abc\xff\xfe\ndef\x80\n")
    for enc in ("ascii", "latin-1", "utf-8"):
        with OpenAsReader(dirty_fp, show_progress=True, encoding=enc, errors="replace") as r:
            lines = list(r)
            assert len(lines) == 2
        with OpenAsReader(dirty_fp, show_progress=True, encoding=enc, errors="replace") as r:
            first = r.readline()
            rest = r.read()
            assert first + rest == "".join(lines)

    # 3. newline='' on CRLF file preserves '\r\n' and skips _reconcile_crlf_eof_progress()
    crlf_fp = tmp_path / "prog_crlf_raw.txt"
    crlf_raw = b"one\r\ntwo\r\n"
    crlf_fp.write_bytes(crlf_raw)
    with OpenAsReader(crlf_fp, show_progress=True, newline="") as r:
        pbar = r._progress
        assert pbar is not None
        assert list(r) == ["one\r\n", "two\r\n"]
        assert pbar.n == len(crlf_raw)
    # Verify _reconcile_crlf_eof_progress() returns immediately when newline is not None
    with OpenAsReader(crlf_fp, show_progress=True, newline="") as r:
        pbar = r._progress
        assert pbar is not None
        r.readline()
        n_before = pbar.n
        r._reconcile_crlf_eof_progress()
        assert pbar.n == n_before

    # 4. Internal fallback decode paths (_iter_decoded, _iter_progress, read, readline) with errors=None and errors='replace'
    class RawByteHandle:
        def __init__(self, data: bytes):
            self._buf = _io.BytesIO(data)

        def __iter__(self):
            return iter(self._buf)

        def read(self, size=-1):
            return self._buf.read() if (size is None or size < 0) else self._buf.read(size)

        def readline(self, size=-1):
            return self._buf.readline() if (size is None or size < 0) else self._buf.readline(size)

        def close(self):
            self._buf.close()

    r_dec = OpenAsReader(dirty_fp, encoding="utf-8", errors="replace")
    r_dec._fh = RawByteHandle(b"hi\xff\nthere\n")
    try:
        assert list(r_dec) == ["hi\ufffd\n", "there\n"]
    finally:
        r_dec.close()

    r_dec2 = OpenAsReader(dirty_fp, encoding="utf-8", errors=None)
    r_dec2._fh = RawByteHandle(b"ok\nline2\n")
    try:
        assert r_dec2.readline() == "ok\n"
        assert r_dec2.read() == "line2\n"
    finally:
        r_dec2.close()


@pytest.mark.parametrize("ext", ["", ".gz", ".zst"])
def test_openas_reader_textiowrapper_composability_read1_and_flush(tmp_path, ext):
    """ckmclg E-02: io.TextIOWrapper(openas(path, 'rb')) composes cleanly with zero ResourceWarning, plus flush/read1."""
    import io as _io

    fp = tmp_path / f"compose{ext or '.txt'}"
    raw_payload = "café line 1\nnaïve line 2\n".encode("latin-1")
    with openas(fp, "wb", backup=False) as w:
        w.write(raw_payload)

    # 1. Direct `with io.TextIOWrapper(openas(path, "rb"), ...)` closes cleanly with zero ResourceWarning
    with _io.TextIOWrapper(openas(fp, "rb"), encoding="latin-1", errors="replace") as wrapper:
        assert list(wrapper) == ["café line 1\n", "naïve line 2\n"]

    # 2. `detach()` inside `with openas(path, "rb") as raw:` closes cleanly with zero ResourceWarning
    with openas(fp, "rb") as raw:
        wrapper = _io.TextIOWrapper(raw, encoding="latin-1", errors="replace")
        try:
            assert wrapper.read() == "café line 1\nnaïve line 2\n"
        finally:
            detached = wrapper.detach()
            assert detached is raw
        assert not raw.closed
    assert raw.closed

    # 3. OpenAsReader.flush() succeeds when open and raises ValueError when closed (text & binary)
    for mode in ("r", "rb"):
        r = openas(fp, mode, **({"encoding": "latin-1"} if mode == "r" else {}))
        r.flush()
        r.close()
        with pytest.raises(ValueError, match="closed"):
            r.flush()

    # 4. OpenAsReader.read1() in binary mode (with show_progress=True), closed ValueError, and text UnsupportedOperation
    raw_size = fp.stat().st_size
    with openas(fp, "rb", show_progress=True) as r:
        pbar = r._progress
        assert pbar is not None
        assert r.read1(0) == b""
        first = r.read1(4)
        assert first == raw_payload[:4]
        chunks = [first]
        while True:
            part = r.read1()
            if not part:
                break
            chunks.append(part)
        assert b"".join(chunks) == raw_payload
        assert pbar.n == raw_size
    with pytest.raises(ValueError, match="closed"):
        r.read1()

    with openas(fp, "r", encoding="latin-1") as r_txt:
        with pytest.raises(_io.UnsupportedOperation):
            r_txt.read1()
    with pytest.raises(ValueError, match="closed"):
        r_txt.read1()

    # 5. read1() fallback when underlying handle lacks read1()
    if ext == "":
        class ReadOnlyNoRead1:
            def __init__(self, data: bytes):
                self._buf = _io.BytesIO(data)

            def read(self, size=-1):
                return self._buf.read() if (size is None or size < 0) else self._buf.read(size)

            def close(self):
                self._buf.close()

        r_fb = OpenAsReader(fp, binary=True)
        r_fb._fh = ReadOnlyNoRead1(b"abcdef")
        try:
            assert r_fb.read1(2) == b"ab"
            assert r_fb.read1(None) == b"cdef"
            assert r_fb.read1(-1) == b""
        finally:
            r_fb.close()


def test_counting_write_stream_progress_typed_buffers_and_none_return():
    """va1pd2 E-01: _CountingWriteStream handles typed buffers, None returns, 64 KiB batching, flush, and close."""
    import array
    import io as _io
    from openas.openas import _CountingWriteStream

    class NoneReturningStream:
        def __init__(self):
            self.chunks = []
            self.flushed = 0
            self.closed = False
            self.close_calls = 0

        def write(self, data):
            self.chunks.append(bytes(data))
            return None  # Mimics Paramiko SFTPFile / ftputil FTPFile

        def flush(self):
            self.flushed += 1

        def close(self):
            self.close_calls += 1
            self.closed = True

    raw = NoneReturningStream()
    updates = []
    cws = _CountingWriteStream(raw, on_progress=updates.append, batch_size=16)

    assert cws.readable() is False
    assert cws.writable() is True
    assert cws.seekable() is False
    assert cws.closed is False

    # Empty write does not trigger callback
    assert cws.write(b"") == 0
    assert cws.bytes_written == 0
    assert updates == []

    # Sub-batch writes accumulate without emitting until threshold or flush()
    assert cws.write(b"123456") == 6
    assert cws.write(bytearray(b"7890")) == 4
    assert cws.bytes_written == 10
    assert updates == []

    # Typed buffer (4 uint32 = 16 bytes) pushes pending to 26 >= 16 -> batched update of 26
    arr = array.array("I", [1, 2, 3, 4])
    assert cws.write(arr) == 16
    assert cws.bytes_written == 26
    assert updates == [26]

    # Memoryview slice below batch threshold flushed explicitly on flush()
    mv = memoryview(b"abcdefgh")[2:7]
    assert cws.write(mv) == 5
    assert cws.bytes_written == 31
    assert updates == [26]
    cws.flush()
    assert raw.flushed == 1
    assert updates == [26, 5]

    # Pending bytes remain flushable via flush_progress() even after close()
    assert cws.write(b"XYZ") == 3
    cws.close()
    assert cws.closed is True
    assert cws._raw_close_attempted is True
    assert raw.closed is True
    assert raw.close_calls == 1
    cws.close()  # idempotent
    assert raw.close_calls == 1
    cws.flush_progress()
    assert updates == [26, 5, 3]
    assert b"".join(raw.chunks) == b"1234567890" + arr.tobytes() + b"cdefgXYZ"

    for op in (lambda: cws.write(b"x"), lambda: cws.flush()):
        with pytest.raises(ValueError, match="closed"):
            op()

    # Standard BytesIO (returns int) works identically
    bio = _io.BytesIO()
    updates_bio = []
    cws_bio = _CountingWriteStream(bio, on_progress=updates_bio.append, batch_size=8)
    assert cws_bio.write(b"hello") == 5
    assert updates_bio == []
    assert cws_bio.write(b"world") == 5
    assert updates_bio == [10]
    assert bio.getvalue() == b"helloworld"
    cws_bio.close()
    assert bio.closed is True


@pytest.mark.parametrize("ext", ["", ".gz", ".bz2", ".xz", ".zst"])
@pytest.mark.parametrize("binary", [False, True])
def test_local_writer_progress_all_formats_write_append_and_encoding(tmp_path, ext, binary):
    """va1pd2 E-01: OpenAsWriter(show_progress=True) tracks exact raw disk bytes (including trailers) on write and append."""
    # Payload large enough to exceed 64 KiB in plain mode and produce multi-block compressed streams with trailers.
    lines_text = [f"row-{i:05d}-café-{i % 97:02d}-" + ("0123456789abcdef" * 4) + "\n" for i in range(1200)]
    payload_text = "".join(lines_text)
    payload_bytes = payload_text.encode("utf-8")
    append_text = "appended-line-1-café\nappended-line-2\n"
    append_bytes = append_text.encode("utf-8")

    fp = tmp_path / f"writer_prog_{'bin' if binary else 'txt'}{ext or '.txt'}"
    w_mode = "wb" if binary else "w"
    a_mode = "ab" if binary else "a"
    r_mode = "rb" if binary else "r"

    # 1. Write with show_progress=True: header bytes captured in initial, trailer bytes flushed on close()
    with openas(fp, w_mode, backup=False, show_progress=True) as w:
        pbar = w._progress
        assert pbar is not None
        assert pbar.total is None
        if ext == ".gz":
            # gzip writes its header during open(); initial captures it without loss or double-counting
            assert pbar.n > 0
        w.write(payload_bytes if binary else payload_text)
        w.flush()
    raw_size_after_write = fp.stat().st_size
    assert raw_size_after_write > 0
    assert pbar.n == raw_size_after_write

    # 2. Append with show_progress=True: tracks exact delta written during append session
    with openas(fp, a_mode, show_progress=True) as w_app:
        pbar_app = w_app._progress
        assert pbar_app is not None
        assert pbar_app.total is None
        w_app.write(append_bytes if binary else append_text)
    raw_size_after_append = fp.stat().st_size
    assert pbar_app.n == raw_size_after_append - raw_size_after_write

    # 3. Round-trip verification
    with openas(fp, r_mode) as r:
        assert r.read() == (payload_bytes + append_bytes if binary else payload_text + append_text)

    # 4. Text mode encoding/errors/newline preserved with show_progress=True
    if not binary:
        latin_fp = tmp_path / f"writer_prog_latin{ext or '.txt'}"
        with openas(latin_fp, "w", backup=False, show_progress=True, encoding="latin-1", newline="\n") as w_lat:
            pbar_lat = w_lat._progress
            assert w_lat.encoding == "latin-1"
            w_lat.write("café\nnaïve\n")
        assert pbar_lat is not None
        assert pbar_lat.n == latin_fp.stat().st_size
        with openas(latin_fp, "r", encoding="latin-1", newline="") as r_lat:
            assert r_lat.read() == "café\nnaïve\n"


def test_local_writer_progress_error_teardown_keyboard_interrupt_and_zero_overhead(tmp_path):
    """va1pd2 E-01: Early-open/close errors and KeyboardInterrupt close all writer handles cleanly; show_progress=False keeps fast path."""
    import tqdm as tqdm_mod

    # 1. Zero-overhead fast path when show_progress=False
    fast_fp = tmp_path / "fast_writer.txt"
    w_fast = OpenAsWriter(fast_fp, show_progress=False).open(backup=False)
    try:
        assert w_fast._counting_stream is None
        assert w_fast._progress is None
        assert w_fast.write == w_fast._fh.write
        w_fast.write("fast\n")
    finally:
        w_fast.close()
    assert w_fast.closed is True

    # 2. Mid-open RuntimeError and KeyboardInterrupt while initializing tqdm close raw_fh cleanly with zero ResourceWarning
    for ext in (".txt", ".gz", ".zst"):
        for exc_cls in (RuntimeError, KeyboardInterrupt):
            out_fp = tmp_path / f"mid_open_{exc_cls.__name__}{ext}"
            writer = OpenAsWriter(out_fp, show_progress=True)
            captured = {}

            def failing_tqdm(*args, **kwargs):
                captured["raw_fh"] = writer._raw_fh
                captured["counting"] = writer._counting_stream
                raise exc_cls(f"boom {exc_cls.__name__}")

            with patch.object(tqdm_mod, "tqdm", side_effect=failing_tqdm):
                with pytest.raises(exc_cls, match=f"boom {exc_cls.__name__}"):
                    writer.open(backup=False)

            assert captured["raw_fh"] is not None
            assert captured["raw_fh"].closed is True
            assert captured["counting"] is not None
            assert captured["counting"].closed is True
            assert writer.closed is True
            assert writer._fh is None
            assert writer._raw_fh is None
            assert writer._counting_stream is None
            assert writer._progress is None

    # 3. close() when flush_progress() raises RuntimeError or KeyboardInterrupt still closes raw_fh and tqdm
    for err_cls in (RuntimeError, KeyboardInterrupt):
        err_fp = tmp_path / f"flush_err_{err_cls.__name__}.gz"
        w_err = OpenAsWriter(err_fp, show_progress=True).open(backup=False)
        raw_fh = w_err._raw_fh
        pbar = w_err._progress
        assert raw_fh is not None and pbar is not None
        w_err.write("buffered line\n")
        pbar_closed = []
        orig_pbar_close = pbar.close

        def spy_pbar_close(closed_list=pbar_closed, orig=orig_pbar_close):
            closed_list.append(True)
            return orig()

        pbar.close = spy_pbar_close
        w_err._counting_stream._on_progress = MagicMock(side_effect=err_cls("progress flush boom"))

        with pytest.raises(err_cls, match="progress flush boom"):
            w_err.close()
        assert raw_fh.closed is True
        assert pbar_closed == [True]
        assert w_err.closed is True
        assert w_err._raw_fh is None
        assert w_err._counting_stream is None
        assert w_err._progress is None

    # 4. close() when fh.close() raises still flushes pending progress and closes raw_fh and tqdm
    fh_err_fp = tmp_path / "fh_close_err.gz"
    w_fh_err = OpenAsWriter(fh_err_fp, show_progress=True).open(backup=False)
    orig_fh = w_fh_err._fh
    raw_fh2 = w_fh_err._raw_fh
    pbar2 = w_fh_err._progress
    assert orig_fh is not None and raw_fh2 is not None and pbar2 is not None
    w_fh_err.write("data before close error\n")
    orig_fh.close()  # flushes trailer through _CountingWriteStream into pending
    failing_fh = MagicMock()
    failing_fh.closed = False
    failing_fh.close.side_effect = OSError("writer fh close boom")
    w_fh_err._fh = failing_fh
    pbar2_closed = []
    orig_pbar2_close = pbar2.close
    pbar2.close = lambda: (pbar2_closed.append(True), orig_pbar2_close())[1]

    with pytest.raises(OSError, match="writer fh close boom"):
        w_fh_err.close()
    assert raw_fh2.closed is True
    assert pbar2_closed == [True]
    assert pbar2.n == fh_err_fp.stat().st_size
    assert w_fh_err.closed is True


def test_plain_local_text_progress_bom_and_lossy_error_handlers(tmp_path):
    """S7-X4: Plain local text progress never overshoots and finishes at file_size for BOM encodings and lossy errors."""
    # 1. BOM encodings (utf-16, utf-32, utf-8-sig with BOM) across iteration and readline
    lines = [f"bom line {i} café\n" for i in range(40)]
    text = "".join(lines)
    for enc in ("utf-16", "utf-32", "utf-8-sig"):
        fp = tmp_path / f"sample_{enc}.txt"
        fp.write_bytes(text.encode(enc))
        fsize = fp.stat().st_size

        with OpenAsReader(fp, show_progress=True, encoding=enc) as r:
            pbar = r._progress
            assert pbar is not None and pbar.total == fsize
            read_lines = []
            for ln in r:
                read_lines.append(ln)
                assert pbar.n <= fsize
        assert read_lines == lines
        assert pbar.n == fsize

        with OpenAsReader(fp, show_progress=True, encoding=enc) as r2:
            pbar2 = r2._progress
            assert pbar2 is not None and pbar2.total == fsize
            got = []
            while True:
                ln = r2.readline()
                assert pbar2.n <= fsize
                if not ln:
                    break
                got.append(ln)
        assert got == lines
        assert pbar2.n == fsize

    # 2. utf-8-sig on a file WITHOUT a BOM does not overshoot
    no_bom_fp = tmp_path / "no_bom.txt"
    no_bom_fp.write_bytes(b"a\nb\nc\n")
    with OpenAsReader(no_bom_fp, show_progress=True, encoding="utf-8-sig") as r_nb:
        pbar_nb = r_nb._progress
        assert r_nb.read() == "a\nb\nc\n"
    assert pbar_nb is not None
    assert pbar_nb.n == no_bom_fp.stat().st_size

    # 3. errors='replace' on invalid UTF-8 bytes (>64 KiB so batching triggers) never overshoots
    bad_chunk = b"ok-" + b"\x80\x81\x82\xff" * 20 + b"-end\n"
    rep_fp = tmp_path / "lossy_replace.txt"
    rep_fp.write_bytes(bad_chunk * 1000)
    rep_size = rep_fp.stat().st_size
    assert rep_size > 65536
    with OpenAsReader(rep_fp, show_progress=True, errors="replace") as r_rep:
        pbar_rep = r_rep._progress
        assert pbar_rep is not None and pbar_rep.total == rep_size
        count = 0
        for _ in r_rep:
            count += 1
            assert pbar_rep.n <= rep_size
    assert count == 1000
    assert pbar_rep.n == rep_size

    # 4. errors='ignore' on dropped bytes finishes at exact file_size even with explicit newline='\n'
    ign_fp = tmp_path / "lossy_ignore.txt"
    ign_fp.write_bytes(b"abc\xff\xfe\x80def\n123\xff456\n")
    ign_size = ign_fp.stat().st_size
    with OpenAsReader(ign_fp, show_progress=True, errors="ignore", newline="\n") as r_ign:
        pbar_ign = r_ign._progress
        assert r_ign.read() == "abcdef\n123456\n"
    assert pbar_ign is not None
    assert pbar_ign.n == ign_size


def test_typing_get_type_hints_resolves_public_and_helper_signatures():
    """S7-X6: typing.get_type_hints() resolves annotations on openas, OpenAsReader, OpenAsWriter, and validators."""
    import typing
    from openas.openas import _validate_headers, _validate_key_filename

    for obj in (openas, OpenAsReader.__init__, OpenAsWriter.__init__, _validate_headers, _validate_key_filename):
        hints = typing.get_type_hints(obj)
        assert isinstance(hints, dict)
        assert len(hints) > 0
    assert "headers" in typing.get_type_hints(openas)
    assert "key_filename" in typing.get_type_hints(OpenAsReader.__init__)
    assert "key_filename" in typing.get_type_hints(OpenAsWriter.__init__)


def test_text_write_errors_newlines_valid_local_key_filename_and_textiowrapper_bz2_xz(tmp_path):
    """S7-X9: Cover errors='replace'/'ignore' writes, newline='\\r'/'\\r\\n', local key_filename, and TextIOWrapper over .bz2/.xz."""
    import io as _io

    # 1. errors='replace' and errors='ignore' on text writes across plain and compressed codecs
    for ext in (".txt", ".gz", ".zst"):
        fp_rep = tmp_path / f"err_replace{ext}"
        with openas(fp_rep, "w", backup=False, encoding="ascii", errors="replace", newline="\n") as w:
            w.write("café\n")
        with openas(fp_rep, "r", encoding="ascii", newline="") as r:
            assert r.read() == "caf?\n"

        fp_ign = tmp_path / f"err_ignore{ext}"
        with openas(fp_ign, "w", backup=False, encoding="ascii", errors="ignore", newline="\n") as w:
            w.write("café\n")
        with openas(fp_ign, "r", encoding="ascii", newline="") as r:
            assert r.read() == "caf\n"

    # 2. Explicit newline='\r' and newline='\r\n' on writes and reads
    for ext in (".txt", ".gz", ".zst"):
        for nl, expected_raw in (("\r", b"a\rb\r"), ("\r\n", b"a\r\nb\r\n")):
            fp_nl = tmp_path / f"nl_{len(nl)}{ext}"
            with openas(fp_nl, "w", backup=False, newline=nl) as w:
                w.write("a\nb\n")
            with openas(fp_nl, "rb") as rb:
                assert rb.read() == expected_raw
            with openas(fp_nl, "r", newline=nl) as rt:
                assert rt.readlines() == [f"a{nl}", f"b{nl}"]

    # 3. Valid key_filename is ignored on local read and write targets
    local_fp = tmp_path / "local_with_key.txt"
    fake_key = tmp_path / "nonexistent_deploy_key"
    with openas(local_fp, "w", backup=False, key_filename=fake_key) as w:
        w.write("local ok\n")
    with openas(local_fp, "r", key_filename=[fake_key]) as r:
        assert r.read() == "local ok\n"

    # 4. io.TextIOWrapper composition over binary .bz2 and .xz readers
    sample_text = "line 1 café\nline 2 日本語\n"
    for ext in (".bz2", ".xz"):
        fp_comp = tmp_path / f"wrap{ext}"
        with openas(fp_comp, "w", backup=False, newline="\n") as w:
            w.write(sample_text)
        with openas(fp_comp, "rb") as raw_reader:
            wrapper = _io.TextIOWrapper(raw_reader, encoding="utf-8", newline="")
            try:
                assert wrapper.read() == sample_text
            finally:
                wrapper.close()
        assert raw_reader.closed is True


def test_zstd_frame_validator_read1_multibyte_fcs_and_zero_length_skippable(tmp_path):
    """S7-X9: _ZstdFrameValidatingStream handles read1(), 2/4/8-byte FCS headers, and 0-byte skippable frames."""
    import io
    import struct
    from openas.openas import _ZstdFrameValidatingStream

    # 1. Zero-length skippable frame followed by a real zstd frame
    zero_skip = struct.pack("<II", 0x184D2A50, 0)
    real_frame = zstd.ZstdCompressor().compress(b"after-zero-skippable\n")
    fp_skip = tmp_path / "zero_skip.zst"
    fp_skip.write_bytes(zero_skip + real_frame)
    with openas(fp_skip, "rb") as r:
        assert r.read() == b"after-zero-skippable\n"

    # 2. 2-byte FCS (~300B) and 4-byte FCS (~70KiB) frames
    for size in (300, 70000):
        payload = (b"fcs-data-" * ((size // 9) + 1))[:size]
        frame = zstd.ZstdCompressor(write_content_size=True).compress(payload)
        fp_fcs = tmp_path / f"fcs_{size}.zst"
        fp_fcs.write_bytes(frame)
        with openas(fp_fcs, "rb") as r:
            assert r.read() == payload

    # 3. Hand-crafted RFC 8878 8-byte FCS valid raw-block frame (magic + FHD=0xC0 + win=0x00 + 8B FCS + 3B raw block + b"hi")
    fcs8_frame = (
        b"\x28\xb5\x2f\xfd"
        + b"\xc0\x00"
        + struct.pack("<Q", 2)
        + struct.pack("<I", (2 << 3) | 1)[:3]
        + b"hi"
    )
    fp_fcs8 = tmp_path / "fcs8.zst"
    fp_fcs8.write_bytes(fcs8_frame)
    with openas(fp_fcs8, "rb") as r:
        assert r.read() == b"hi"

    # 4. Direct read1() through _ZstdFrameValidatingStream (both valid and truncated)
    valid_stream = _ZstdFrameValidatingStream(io.BytesIO(fcs8_frame))
    collected = bytearray()
    while True:
        chunk = valid_stream.read1(5)
        if not chunk:
            break
        collected.extend(chunk)
    assert bytes(collected) == fcs8_frame

    trunc_stream = _ZstdFrameValidatingStream(io.BytesIO(fcs8_frame[:-1]))
    with pytest.raises(EOFError):
        while trunc_stream.read1(5):
            pass


def test_openas_writer_close_dual_error_precedence(tmp_path):
    """S7-X9: OpenAsWriter.close() preserves primary fh.close() error when progress flush and raw close also fail."""
    fp = tmp_path / "dual_err.gz"
    writer = OpenAsWriter(fp, show_progress=True).open(backup=False)
    orig_fh = writer._fh
    orig_raw = writer._raw_fh
    pbar = writer._progress
    assert orig_fh is not None and orig_raw is not None and pbar is not None

    writer.write("payload\n")
    orig_fh.close()

    failing_fh = MagicMock()
    failing_fh.closed = False
    failing_fh.close.side_effect = OSError("primary fh close error")
    writer._fh = failing_fh

    writer._counting_stream._on_progress = MagicMock(side_effect=RuntimeError("secondary flush error"))

    failing_sibling = MagicMock()
    failing_sibling.closed = False
    failing_sibling.close.side_effect = OSError("tertiary raw close error")
    writer._ssh_client = failing_sibling

    with pytest.raises(OSError, match="primary fh close error"):
        writer.close()

    assert orig_raw.closed is True
    assert failing_sibling.close.call_count == 1
    assert writer.closed is True
    assert writer._ssh_client is None
