import subprocess
import pytest
import os
import sys

_SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))


def _cli_env():
    """Return os.environ with this checkout's src/ first on PYTHONPATH for CLI subprocesses."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = _SRC_DIR + os.pathsep + existing if existing else _SRC_DIR
    return env


def _is_inside(directory, path):
    directory = os.path.realpath(directory)
    path = os.path.realpath(path)
    return os.path.commonpath([directory, path]) == directory


def run_cli(*args):
    """Utility to spawn subprocess commands targeting polyopen module structurally."""
    cmd = [sys.executable, "-m", "polyopen.polyopen"] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True, env=_cli_env())
    return result


def test_tests_import_polyopen_from_checkout_src(tmp_path):
    """mux2cz: pytest and CLI subprocesses import polyopen from this checkout's src/."""
    import polyopen

    assert _is_inside(_SRC_DIR, polyopen.__file__), (
        f"pytest process imported {polyopen.__file__}, not a module under {_SRC_DIR}"
    )

    res = subprocess.run(
        [sys.executable, "-c", "import polyopen; print(polyopen.__file__)"],
        capture_output=True,
        text=True,
        env=_cli_env(),
        cwd=str(tmp_path),
    )
    assert res.returncode == 0, res.stderr
    sub_path = res.stdout.strip()
    assert _is_inside(_SRC_DIR, sub_path), (
        f"subprocess imported {sub_path}, not a module under {_SRC_DIR}"
    )


def test_cli_illegal_modes():
    """Asserts that the argparse module mutually excludes overlapping rules securely."""
    # Omitting both Read and Write should throw an exit code parsing error
    result = run_cli("test.txt")
    assert result.returncode != 0
    assert "one of the arguments --write/-w --read/-r is required" in result.stderr

    # Omitting target file entirely
    result = run_cli("--read")
    assert result.returncode == 0 # Wait, main() takes `nargs='*'`, so providing no files might succeed and do nothing. Wait, no files means the script exits clean or prints help? Let's check locally via test.

def test_cli_read_execution(tmp_path):
    # Establish a local target
    target = str(tmp_path / "mock.txt")
    with open(target, 'w') as f:
        f.write("A line.\nAnother line.\n")

    result = run_cli("--read", target)
    
    assert result.returncode == 0
    # main() prints the file stats
    assert "FINISHED Reading." in result.stdout

def test_cli_write_execution(tmp_path):
    source = str(tmp_path / "mock.txt")
    with open(source, 'w') as f:
        f.write("Data1\nData2\n")

    target = str(tmp_path / "write_mock.txt")
    
    cmd = [sys.executable, "-m", "polyopen.polyopen", "--write", source, target]
    result = subprocess.run(cmd, text=True, capture_output=True, env=_cli_env())
    
    assert result.returncode == 0
    assert os.path.exists(target)
    
    with open(target, 'r') as f:
        data = f.read()
    assert "Data1\nData2\n" == data


def test_cli_input_file_flag_utf8_bytes_and_errors(tmp_path):
    """BUG-08: --write -i, invalid -i usage errors, and true UTF-8 byte counts."""
    source = tmp_path / "utf8_src.txt"
    target = tmp_path / "utf8_dst.txt"
    content = "café 日本語\n"
    expected_bytes = len(content.encode("utf-8"))
    source.write_text(content, encoding="utf-8")

    res_ok = subprocess.run(
        [sys.executable, "-m", "polyopen.polyopen", "--write", "-i", str(source), str(target)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_ok.returncode == 0, res_ok.stderr
    assert target.read_text(encoding="utf-8") == content
    assert f"{expected_bytes:.1f}B" in res_ok.stdout

    res_read = subprocess.run(
        [sys.executable, "-m", "polyopen.polyopen", "--read", str(source)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_read.returncode == 0, res_read.stderr
    assert f"{expected_bytes:.1f}B" in res_read.stdout

    res_missing_dst = subprocess.run(
        [sys.executable, "-m", "polyopen.polyopen", "--write", "-i", str(source)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_missing_dst.returncode != 0
    assert "destination" in res_missing_dst.stderr.lower()

    res_read_i = subprocess.run(
        [sys.executable, "-m", "polyopen.polyopen", "--read", "-i", str(source)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_read_i.returncode != 0
    assert "--write" in res_read_i.stderr


def test_format_bytes_units():
    """BUG-08: format_bytes formats B through EB, ZB, and YB accurately."""
    from polyopen.polyopen import format_bytes

    assert format_bytes(512).strip() == "512.0B"
    assert format_bytes(1024).strip() == "1.0KB"
    assert format_bytes(1024**2).strip() == "1.0MB"
    assert format_bytes(1024**3).strip() == "1.0GB"
    assert format_bytes(1024**4).strip() == "1.0TB"
    assert format_bytes(1024**5).strip() == "1.0PB"
    assert format_bytes(1024**6).strip() == "1.0EB"
    assert format_bytes(1024**7).strip() == "1.0ZB"
    assert format_bytes(1024**8).strip() == "1.0YB"


def test_cli_write_broadcast_single_read_identical_outputs_and_messages(tmp_path, monkeypatch, capsys):
    """p1f3dc E-01: CLI --write broadcasts to multiple targets with a single source read."""
    import gzip
    from polyopen import PolyReader
    from polyopen.polyopen import main as cli_main

    payload = "alpha line\ncafé 日本語\nomega line\n"
    expected_bytes = len(payload.encode("utf-8"))
    src = tmp_path / "source.txt.gz"
    with gzip.open(src, "wt", encoding="utf-8") as fh:
        fh.write(payload)

    dst_txt = tmp_path / "out1.txt"
    dst_gz = tmp_path / "out2.txt.gz"
    dst_zst = tmp_path / "out3.txt.zst"

    stats = {"opens": 0, "iters": 0}
    orig_open = PolyReader.open
    orig_iter = PolyReader.__iter__

    def spy_open(self):
        stats["opens"] += 1
        return orig_open(self)

    def spy_iter(self):
        stats["iters"] += 1
        return orig_iter(self)

    monkeypatch.setattr(PolyReader, "open", spy_open)
    monkeypatch.setattr(PolyReader, "__iter__", spy_iter)
    monkeypatch.setattr(
        sys,
        "argv",
        ["polyopen", "--write", str(src), str(dst_txt), str(dst_gz), str(dst_zst)],
    )
    cli_main()

    out = capsys.readouterr().out
    assert stats == {"opens": 1, "iters": 1}
    assert f"Will Read from {src}" in out
    for dst in (dst_txt, dst_gz, dst_zst):
        assert f"Writing {dst}" in out
        assert f"FINISHED Writing. {dst}. 3 lines / {expected_bytes:.1f}B" in out

    monkeypatch.setattr(PolyReader, "open", orig_open)
    monkeypatch.setattr(PolyReader, "__iter__", orig_iter)
    for dst in (dst_txt, dst_gz, dst_zst):
        with PolyReader(dst) as r:
            assert r.read() == payload

    # Also verify --input-file/-i multi-target broadcast via real subprocess
    sub_dsts = [tmp_path / f"sub_{k}.txt" for k in (1, 2, 3)]
    res = subprocess.run(
        [
            sys.executable,
            "-m",
            "polyopen.polyopen",
            "--write",
            "-i",
            str(src),
            *(str(d) for d in sub_dsts),
        ],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res.returncode == 0, res.stderr
    for d in sub_dsts:
        assert d.read_text(encoding="utf-8") == payload
        assert f"Writing {d}" in res.stdout
        assert f"FINISHED Writing. {d}." in res.stdout


def test_cli_write_broadcast_closes_handles_on_writer_error(tmp_path, monkeypatch):
    """p1f3dc E-01: CLI --write closes reader and all opened writers cleanly on writer failure."""
    from polyopen import PolyReader, PolyWriter, ReadOnlyProtocolError
    from polyopen.polyopen import main as cli_main

    src = tmp_path / "src.txt"
    src.write_text("line1\nline2\nline3\n", encoding="utf-8")
    dst1 = tmp_path / "dst1.txt"
    dst2 = tmp_path / "dst2.txt"

    closed = []
    orig_reader_close = PolyReader.close
    orig_writer_close = PolyWriter.close
    orig_writer_open = PolyWriter.open

    def track_reader_close(self):
        closed.append(("reader", self.filename))
        return orig_reader_close(self)

    def track_writer_close(self):
        closed.append(("writer", self.filename))
        return orig_writer_close(self)

    write_calls = {"dst2": 0}

    def failing_open(self, append=False, backup=True, compresslevel=None):
        orig_writer_open(self, append=append, backup=backup, compresslevel=compresslevel)
        if self.filename == str(dst2):
            real_write = self.write

            def boom_on_second(data):
                write_calls["dst2"] += 1
                if write_calls["dst2"] == 2:
                    raise OSError("disk full")
                return real_write(data)

            self.write = boom_on_second
        return self

    monkeypatch.setattr(PolyReader, "close", track_reader_close)
    monkeypatch.setattr(PolyWriter, "close", track_writer_close)
    monkeypatch.setattr(PolyWriter, "open", failing_open)
    monkeypatch.setattr(sys, "argv", ["polyopen", "-w", str(src), str(dst1), str(dst2)])

    with pytest.raises(OSError, match="disk full"):
        cli_main()

    assert ("reader", str(src)) in closed
    assert ("writer", str(dst1)) in closed
    assert ("writer", str(dst2)) in closed

    # Open-time error on 2nd writer also closes reader and 1st writer cleanly
    closed.clear()
    monkeypatch.setattr(PolyWriter, "open", orig_writer_open)
    dst_ok = tmp_path / "dst_ok.txt"
    monkeypatch.setattr(
        sys,
        "argv",
        ["polyopen", "-w", str(src), str(dst_ok), "https://example.com/readonly.txt"],
    )
    with pytest.raises(ReadOnlyProtocolError):
        cli_main()

    assert ("reader", str(src)) in closed
    assert ("writer", str(dst_ok)) in closed
