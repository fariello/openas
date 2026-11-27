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
    """Utility to spawn subprocess commands targeting openas module structurally."""
    cmd = [sys.executable, "-m", "openas.openas"] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True, env=_cli_env())
    return result


def test_tests_import_openas_from_checkout_src(tmp_path):
    """mux2cz: pytest and CLI subprocesses import openas from this checkout's src/."""
    import openas

    assert _is_inside(_SRC_DIR, openas.__file__), (
        f"pytest process imported {openas.__file__}, not a module under {_SRC_DIR}"
    )

    res = subprocess.run(
        [sys.executable, "-c", "import openas; print(openas.__file__)"],
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

    # Omitting target file in --read mode must fail via parser.error
    result = run_cli("--read")
    assert result.returncode != 0
    assert "requires at least one file" in result.stderr

def test_cli_read_execution(tmp_path):
    # Establish a local target
    target = str(tmp_path / "mock.txt")
    with open(target, 'w', encoding='utf-8') as f:
        f.write("A line.\nAnother line.\n")

    result = run_cli("--read", target)
    
    assert result.returncode == 0
    # main() prints the file stats
    assert "FINISHED Reading." in result.stdout

def test_cli_write_execution(tmp_path):
    source = str(tmp_path / "mock.txt")
    with open(source, 'w', encoding='utf-8') as f:
        f.write("Data1\nData2\n")

    target = str(tmp_path / "write_mock.txt")
    
    cmd = [sys.executable, "-m", "openas.openas", "--write", source, target]
    result = subprocess.run(cmd, text=True, capture_output=True, env=_cli_env())
    
    assert result.returncode == 0
    assert os.path.exists(target)
    
    with open(target, 'r', encoding='utf-8') as f:
        data = f.read()
    assert "Data1\nData2\n" == data


def test_cli_input_file_flag_utf8_bytes_and_errors(tmp_path):
    """BUG-08: --write -i, invalid -i usage errors, and true UTF-8 byte counts."""
    source = tmp_path / "utf8_src.txt"
    target = tmp_path / "utf8_dst.txt"
    content = "café 日本語\n"
    expected_bytes = len(content.encode("utf-8"))
    source.write_bytes(content.encode("utf-8"))

    res_ok = subprocess.run(
        [sys.executable, "-m", "openas.openas", "--write", "-i", str(source), str(target)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_ok.returncode == 0, res_ok.stderr
    assert target.read_text(encoding="utf-8") == content
    assert f"{expected_bytes:.1f}B" in res_ok.stdout

    res_read = subprocess.run(
        [sys.executable, "-m", "openas.openas", "--read", str(source)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_read.returncode == 0, res_read.stderr
    assert f"{expected_bytes:.1f}B" in res_read.stdout

    res_missing_dst = subprocess.run(
        [sys.executable, "-m", "openas.openas", "--write", "-i", str(source)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_missing_dst.returncode != 0
    assert "destination" in res_missing_dst.stderr.lower()

    res_read_i = subprocess.run(
        [sys.executable, "-m", "openas.openas", "--read", "-i", str(source)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_read_i.returncode != 0
    assert "--write" in res_read_i.stderr


def test_format_bytes_units():
    """BUG-08: format_bytes formats B through EB, ZB, and YB accurately."""
    from openas.openas import format_bytes

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
    from openas import OpenAsReader
    from openas.openas import main as cli_main

    payload = "alpha line\ncafé 日本語\nomega line\n"
    expected_bytes = len(payload.encode("utf-8"))
    src = tmp_path / "source.txt.gz"
    with gzip.open(src, "wb") as fh:
        fh.write(payload.encode("utf-8"))

    dst_txt = tmp_path / "out1.txt"
    dst_gz = tmp_path / "out2.txt.gz"
    dst_zst = tmp_path / "out3.txt.zst"

    stats = {"opens": 0, "reads": 0}
    orig_open = OpenAsReader.open
    orig_read = OpenAsReader.read

    def spy_open(self):
        stats["opens"] += 1
        return orig_open(self)

    def spy_read(self, size=-1):
        stats["reads"] += 1
        return orig_read(self, size)

    monkeypatch.setattr(OpenAsReader, "open", spy_open)
    monkeypatch.setattr(OpenAsReader, "read", spy_read)
    monkeypatch.setattr(
        sys,
        "argv",
        ["openas", "--write", str(src), str(dst_txt), str(dst_gz), str(dst_zst)],
    )
    cli_main()

    out = capsys.readouterr().out
    assert stats["opens"] == 1
    assert stats["reads"] >= 1
    assert f"Will Read from {src}" in out
    for dst in (dst_txt, dst_gz, dst_zst):
        assert f"Writing {dst}" in out
        assert f"FINISHED Writing. {dst}. 3 lines / {expected_bytes:.1f}B" in out

    monkeypatch.setattr(OpenAsReader, "open", orig_open)
    monkeypatch.setattr(OpenAsReader, "read", orig_read)
    for dst in (dst_txt, dst_gz, dst_zst):
        with OpenAsReader(dst) as r:
            assert r.read() == payload

    # Also verify --input-file/-i multi-target broadcast via real subprocess
    sub_dsts = [tmp_path / f"sub_{k}.txt" for k in (1, 2, 3)]
    res = subprocess.run(
        [
            sys.executable,
            "-m",
            "openas.openas",
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
    from openas import OpenAsReader, OpenAsWriter, ReadOnlyProtocolError
    from openas.openas import main as cli_main

    src = tmp_path / "src.txt"
    # Exceed the 64 KiB CLI chunk size so a second chunk triggers the simulated writer failure.
    src.write_text("line1\nline2\nline3\n" * 4000, encoding="utf-8")
    dst1 = tmp_path / "dst1.txt"
    dst2 = tmp_path / "dst2.txt"

    closed = []
    orig_reader_close = OpenAsReader.close
    orig_writer_close = OpenAsWriter.close
    orig_writer_open = OpenAsWriter.open

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

    monkeypatch.setattr(OpenAsReader, "close", track_reader_close)
    monkeypatch.setattr(OpenAsWriter, "close", track_writer_close)
    monkeypatch.setattr(OpenAsWriter, "open", failing_open)
    monkeypatch.setattr(sys, "argv", ["openas", "-w", str(src), str(dst1), str(dst2)])

    with pytest.raises(OSError, match="disk full"):
        cli_main()

    assert ("reader", str(src)) in closed
    assert ("writer", str(dst1)) in closed
    assert ("writer", str(dst2)) in closed

    # Pre-validation rejects read-only destination before opening/creating earlier destinations
    closed.clear()
    monkeypatch.setattr(OpenAsWriter, "open", orig_writer_open)
    dst_never = tmp_path / "dst_never.txt"
    monkeypatch.setattr(
        sys,
        "argv",
        ["openas", "-w", str(src), str(dst_never), "https://example.com/readonly.txt"],
    )
    with pytest.raises(ReadOnlyProtocolError):
        cli_main()
    assert not dst_never.exists()

    # Open-time error on 2nd writer (missing parent dir) still closes reader and 1st writer cleanly
    closed.clear()
    dst_ok = tmp_path / "dst_ok.txt"
    dst_missing_dir = tmp_path / "missing_subdir" / "out.txt"
    monkeypatch.setattr(
        sys,
        "argv",
        ["openas", "-w", str(src), str(dst_ok), str(dst_missing_dir)],
    )
    with pytest.raises(FileNotFoundError):
        cli_main()

    assert ("reader", str(src)) in closed
    assert ("writer", str(dst_ok)) in closed


def test_cli_package_entrypoint_help_version_and_errors(tmp_path):
    """S5-U1 / S4-U1 / S2-B9: python -m openas works without RuntimeWarning, exposes --version, and validates args."""
    res_help = subprocess.run(
        [sys.executable, "-m", "openas", "--help"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_help.returncode == 0
    assert "RuntimeWarning" not in res_help.stderr
    assert ".xz" in res_help.stdout
    assert "Examples:" in res_help.stdout

    res_ver = subprocess.run(
        [sys.executable, "-m", "openas", "--version"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_ver.returncode == 0
    assert "0.1.0" in res_ver.stdout

    # Mutually exclusive --read --write fails
    res_both = subprocess.run(
        [sys.executable, "-m", "openas", "--read", "--write", "a.txt", "b.txt"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_both.returncode != 0

    # --write with only 1 positional file fails
    res_one = subprocess.run(
        [sys.executable, "-m", "openas", "--write", "only_one.txt"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_one.returncode != 0

    # Multi-file --read and --write backup rotation
    f1 = tmp_path / "m1.txt"
    f2 = tmp_path / "m2.txt"
    f1.write_text("a\n", encoding="utf-8")
    f2.write_text("b\nc\n", encoding="utf-8")
    res_multi = subprocess.run(
        [sys.executable, "-m", "openas", "--read", str(f1), str(f2)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_multi.returncode == 0, res_multi.stderr
    assert str(f1) in res_multi.stdout and str(f2) in res_multi.stdout

    dst = tmp_path / "existing.txt"
    dst.write_text("old\n", encoding="utf-8")
    res_bu = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(f1), str(dst)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_bu.returncode == 0, res_bu.stderr
    assert (tmp_path / "existing.txt.001.bu").read_text(encoding="utf-8") == "old\n"
    assert dst.read_text(encoding="utf-8") == "a\n"


def test_cli_redacts_url_credentials_and_format_bytes_negative(monkeypatch, capsys):
    """S2-S1 & S2-B9: CLI stdout masks URL passwords/query tokens and format_bytes handles negative values."""
    import io
    from unittest.mock import MagicMock, patch
    from openas.openas import format_bytes, main as cli_main

    assert format_bytes(-2048) == "-2.0KB"

    mock_resp = MagicMock()
    mock_resp.headers = {"Content-Length": "3"}
    mock_resp.raw = io.BytesIO(b"hi\n")
    with patch("requests.Session.get", return_value=mock_resp):
        monkeypatch.setattr(
            sys,
            "argv",
            ["openas", "-r", "https://alice:supersecret@example.com/log.txt?token=abc123"],
        )
        cli_main()
    out = capsys.readouterr().out
    assert "supersecret" not in out
    assert "abc123" not in out
    assert "alice:***@example.com/log.txt?<redacted>" in out


def test_cli_empty_file_zero_rate_includes_byte_unit(tmp_path):
    """iev841 E-05: CLI --read and --write format zero byte-rate as (0.0B/s) instead of (0/s)."""
    empty_src = tmp_path / "empty.txt"
    empty_src.write_text("", encoding="utf-8")
    empty_dst = tmp_path / "empty_out.txt"

    res_read = subprocess.run(
        [sys.executable, "-m", "openas", "--read", str(empty_src)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_read.returncode == 0, res_read.stderr
    assert "(0.0B/s)" in res_read.stdout
    assert "(0/s)" not in res_read.stdout

    res_write = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(empty_src), str(empty_dst)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_write.returncode == 0, res_write.stderr
    assert "(0.0B/s)" in res_write.stdout
    assert "(0/s)" not in res_write.stdout
    assert empty_dst.exists()
    assert empty_dst.read_text(encoding="utf-8") == ""
    assert "0 lines / 0.0B" in res_read.stdout
    assert "0 lines / 0.0B" in res_write.stdout


def test_cli_binary_crlf_no_trailing_newline_and_help(tmp_path):
    """a224p3 E-04: CLI preserves non-UTF-8 binary and CRLF bytes, counts lines accurately, and documents chunked binary mode."""
    import gzip

    # 1. --help reflects binary and text 64 KiB chunked streaming
    res_help = subprocess.run(
        [sys.executable, "-m", "openas", "--help"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_help.returncode == 0
    assert "binary and text streams" in res_help.stdout
    assert "64 KiB binary chunks" in res_help.stdout

    # 2. Arbitrary non-UTF-8 binary payload (>64 KiB) across compressed .gz -> plain and .zst
    raw_bin = (b"\x00\xff\xfe\x80record\n\xde\xad\xbe\xef" * 4500) + b"\x89PNG"
    expected_bin_lines = raw_bin.count(b"\n") + 1  # ends without trailing newline
    bin_src = tmp_path / "payload.bin.gz"
    with gzip.open(bin_src, "wb") as fh:
        fh.write(raw_bin)
    bin_dst_plain = tmp_path / "payload_out.bin"
    bin_dst_zst = tmp_path / "payload_out.bin.zst"

    res_bin_read = run_cli("--read", str(bin_src))
    assert res_bin_read.returncode == 0, res_bin_read.stderr
    assert f"{expected_bin_lines:,d} lines" in res_bin_read.stdout

    res_bin_write = run_cli("--write", str(bin_src), str(bin_dst_plain), str(bin_dst_zst))
    assert res_bin_write.returncode == 0, res_bin_write.stderr
    assert f"{expected_bin_lines:,d} lines" in res_bin_write.stdout
    assert bin_dst_plain.read_bytes() == raw_bin
    from openas import OpenAsReader

    with OpenAsReader(bin_dst_zst, binary=True) as r:
        assert r.read() == raw_bin

    # 3. CRLF byte preservation
    crlf_src = tmp_path / "crlf_in.txt"
    crlf_dst = tmp_path / "crlf_out.txt"
    crlf_bytes = b"alpha\r\nbeta\r\n"
    crlf_src.write_bytes(crlf_bytes)

    res_crlf_read = run_cli("--read", str(crlf_src))
    assert res_crlf_read.returncode == 0, res_crlf_read.stderr
    assert f"2 lines / {len(crlf_bytes):.1f}B" in res_crlf_read.stdout

    res_crlf_write = run_cli("--write", str(crlf_src), str(crlf_dst))
    assert res_crlf_write.returncode == 0, res_crlf_write.stderr
    assert f"2 lines / {len(crlf_bytes):.1f}B" in res_crlf_write.stdout
    assert crlf_dst.read_bytes() == crlf_bytes

    # 4. Non-empty files without trailing newline
    for name, content, expected_lines in (
        ("single_no_nl.txt", b"single line", 1),
        ("two_no_nl.txt", b"first\nsecond", 2),
    ):
        src = tmp_path / name
        dst = tmp_path / f"copy_{name}"
        src.write_bytes(content)
        res_r = run_cli("--read", str(src))
        assert res_r.returncode == 0, res_r.stderr
        assert f"{expected_lines} lines / {len(content):.1f}B" in res_r.stdout
        res_w = run_cli("--write", str(src), str(dst))
        assert res_w.returncode == 0, res_w.stderr
        assert f"{expected_lines} lines / {len(content):.1f}B" in res_w.stdout
        assert dst.read_bytes() == content


@pytest.mark.parametrize(
    "archive_suffix",
    [
        "bad.zip",
        "bad.tar.gz",
        "sftp://u@h/a%2Ezip",
    ],
)
def test_cli_write_archive_prevalidation_leaves_earlier_outputs_untouched(tmp_path, archive_suffix):
    """was3ig E-05: CLI --write rejects archive targets before creating, modifying, or backing up earlier outputs."""
    src = tmp_path / "source.txt"
    src.write_bytes(b"fresh payload\n")

    archive_dst = archive_suffix if "://" in archive_suffix else str(tmp_path / archive_suffix)

    # 1. Absent earlier output is never created
    absent_dst = tmp_path / "absent_first.txt"
    res_absent = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), str(absent_dst), archive_dst],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_absent.returncode != 0
    assert "UnsupportedArchiveError" in res_absent.stderr
    assert not absent_dst.exists()
    assert not (tmp_path / "absent_first.txt.001.bu").exists()

    # 2. Pre-existing earlier output is neither modified nor backed up
    existing_dst = tmp_path / "existing_first.txt"
    existing_dst.write_bytes(b"original content\n")
    res_existing = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), str(existing_dst), archive_dst],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_existing.returncode != 0
    assert "UnsupportedArchiveError" in res_existing.stderr
    assert existing_dst.read_bytes() == b"original content\n"
    assert not (tmp_path / "existing_first.txt.001.bu").exists()


def test_cli_write_missing_source_preserves_existing_destination_without_backup(tmp_path):
    """was3ig E-05: CLI --write with a missing source leaves pre-existing outputs untouched with no .001.bu."""
    missing_src = tmp_path / "does_not_exist.txt"
    existing_dst = tmp_path / "keep_me.txt"
    existing_dst.write_bytes(b"do not overwrite\n")

    res = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(missing_src), str(existing_dst)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res.returncode != 0
    assert existing_dst.read_bytes() == b"do not overwrite\n"
    assert not (tmp_path / "keep_me.txt.001.bu").exists()


@pytest.mark.parametrize("ext", [".gz", ".bz2", ".xz", ".zst"])
def test_cli_compressed_formats_subprocess_roundtrip(tmp_path, ext):
    """was3ig E-05: CLI --write and --read handle .gz, .bz2, .xz, and .zst cleanly via subprocess."""
    from openas import OpenAsReader

    payload_bytes = "cli compressed line 1 café\nline 2 日本語\nline 3\n".encode("utf-8")
    src = tmp_path / "plain_src.txt"
    src.write_bytes(payload_bytes)
    comp_dst = tmp_path / f"roundtrip{ext}"
    plain_back = tmp_path / f"decoded_{ext.lstrip('.')}.txt"

    res_write = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), str(comp_dst)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_write.returncode == 0, res_write.stderr
    assert f"FINISHED Writing. {comp_dst}. 3 lines / {len(payload_bytes):.1f}B" in res_write.stdout
    assert comp_dst.exists()

    with OpenAsReader(comp_dst, binary=True) as r:
        assert r.read() == payload_bytes

    res_read = subprocess.run(
        [sys.executable, "-m", "openas", "--read", str(comp_dst)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_read.returncode == 0, res_read.stderr
    assert f"FINISHED Reading. {comp_dst}. 3 lines / {len(payload_bytes):.1f}B" in res_read.stdout

    res_decode = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(comp_dst), str(plain_back)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_decode.returncode == 0, res_decode.stderr
    assert plain_back.read_bytes() == payload_bytes


def test_cli_help_notes_examples_and_read_status_wording(tmp_path):
    """n5d1ot E-03: --help surfaces .001.bu backups, sftp:// notes, openas examples, and --read prints 'Reading '."""
    res_help = subprocess.run(
        [sys.executable, "-m", "openas", "--help"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_help.returncode == 0, res_help.stderr
    out = res_help.stdout
    assert "64 KiB binary chunks" in out
    assert ".001.bu" in out
    assert "Examples:" in out
    assert "openas --read " in out
    assert "openas --write " in out
    assert "openas --write -i " in out
    assert "Notes:" in out
    assert "sftp://user@host/abs/path" in out
    assert "~/.ssh/known_hosts" in out

    # parser.error examples reference `openas -r` and `openas -w`
    res_no_read_file = subprocess.run(
        [sys.executable, "-m", "openas", "--read"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_no_read_file.returncode == 2
    assert "'openas -r path/to/file.gz'" in res_no_read_file.stderr

    res_one_write_file = subprocess.run(
        [sys.executable, "-m", "openas", "--write", "only_src.txt"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_one_write_file.returncode == 2
    assert "'openas -w src dest'" in res_one_write_file.stderr

    # --read status line uses "Reading <file>" instead of "TESTING reading <file>"
    sample = tmp_path / "status_check.txt"
    sample.write_text("hello\n", encoding="utf-8")
    res_read = subprocess.run(
        [sys.executable, "-m", "openas", "--read", str(sample)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_read.returncode == 0, res_read.stderr
    assert f"Reading {sample}" in res_read.stdout
    assert "TESTING reading" not in res_read.stdout
    assert "FINISHED Reading." in res_read.stdout


@pytest.mark.parametrize("module_name", ["openas", "openas.openas"])
def test_cli_operational_errors_formatted_without_traceback(tmp_path, module_name):
    """n5d1ot E-04: Subprocess CLI operational failures exit 1 with concise redacted stderr and no traceback."""
    src = tmp_path / "src.txt"
    src.write_text("hello\n", encoding="utf-8")
    corrupt_xz = tmp_path / "corrupt.xz"
    corrupt_xz.write_bytes(b"not-a-valid-xz-stream")
    missing = tmp_path / "does_not_exist.txt"

    cases = [
        (["--read", "archive.zip"], "UnsupportedArchiveError", "zipfile"),
        (["--read", str(missing)], "FileNotFoundError", "does_not_exist.txt"),
        (
            ["--write", str(src), "https://alice:topsecret@example.com/out.txt?token=tok99"],
            "ReadOnlyProtocolError",
            "HTTP/HTTPS is read-only",
        ),
        (["--read", "s3://bucket/key.gz"], "UnsupportedProtocolError", "smart_open"),
        (["--read", str(corrupt_xz)], "LZMAError", ""),
    ]

    for cli_args, exc_name, expected_sub in cases:
        res = subprocess.run(
            [sys.executable, "-m", module_name, *cli_args],
            capture_output=True,
            text=True,
            env=_cli_env(),
        )
        assert res.returncode == 1, f"{module_name} {cli_args}: rc={res.returncode}, stderr={res.stderr!r}"
        assert f"openas: error ({exc_name}):" in res.stderr, res.stderr
        if expected_sub:
            assert expected_sub in res.stderr, res.stderr
        assert "Traceback (most recent call last):" not in res.stderr, res.stderr
        assert "topsecret" not in res.stderr
        assert "tok99" not in res.stderr

    # Explicit credential/query redaction in CLI stderr on missing-hostname URL error
    res_redact = subprocess.run(
        [sys.executable, "-m", module_name, "--read", "https:///missing.txt?token=secret_query_val"],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_redact.returncode == 1
    assert "openas: error (ValueError):" in res_redact.stderr
    assert "secret_query_val" not in res_redact.stderr
    assert "?<redacted>" in res_redact.stderr
    assert "Traceback (most recent call last):" not in res_redact.stderr


def test_cli_main_operational_classification_redaction_and_internal_reraise(monkeypatch, capsys):
    """n5d1ot E-04: _is_cli_operational_error and _cli_main cover codec/transport errors, redact URLs, and re-raise bugs."""
    import ftplib
    import importlib
    import lzma
    import zlib
    import ftputil.error
    import paramiko
    import requests
    import zstandard as zstd
    mod = importlib.import_module("openas.openas")

    operational_instances = [
        lzma.LZMAError("corrupt xz data"),
        zlib.error("invalid block type"),
        ftplib.error_perm("530 Login incorrect"),
        zstd.ZstdError("corrupt zstd frame"),
        paramiko.SSHException("Server not found in known_hosts"),
        ftputil.error.FTPError("ftp failure"),
    ]

    for exc in operational_instances:
        assert mod._is_cli_operational_error(exc) is True
        monkeypatch.setattr(mod, "main", lambda _e=exc: (_ for _ in ()).throw(_e))
        with pytest.raises(SystemExit) as sysexit:
            mod._cli_main()
        assert sysexit.value.code == 1
        err = capsys.readouterr().err
        assert err == f"openas: error ({type(exc).__name__}): {exc}\n"

    # Empty exception message omits trailing ": "
    monkeypatch.setattr(mod, "main", lambda: (_ for _ in ()).throw(OSError()))
    with pytest.raises(SystemExit) as sysexit:
        mod._cli_main()
    assert sysexit.value.code == 1
    assert capsys.readouterr().err == "openas: error (OSError)\n"

    # Embedded URL passwords and query strings in third-party exception text are redacted
    http_exc = requests.HTTPError(
        "401 Client Error: Unauthorized for url: https://alice:s3cr3t@example.com/data.gz?api_key=xyz987."
    )
    monkeypatch.setattr(mod, "main", lambda: (_ for _ in ()).throw(http_exc))
    with pytest.raises(SystemExit) as sysexit:
        mod._cli_main()
    assert sysexit.value.code == 1
    err_http = capsys.readouterr().err
    assert "s3cr3t" not in err_http
    assert "xyz987" not in err_http
    assert "https://alice:***@example.com/data.gz?<redacted>." in err_http
    assert err_http.startswith("openas: error (HTTPError): ")

    # Already-redacted URLs remain intact (idempotent)
    already = "openas failed on sftp://alice:***@host/a.zip?<redacted>."
    assert mod._redact_urls_in_text(already) == already

    # Unexpected internal bug (RuntimeError) is NOT swallowed and propagates with traceback
    bug = RuntimeError("internal invariant violated")
    assert mod._is_cli_operational_error(bug) is False
    monkeypatch.setattr(mod, "main", lambda: (_ for _ in ()).throw(bug))
    with pytest.raises(RuntimeError, match="internal invariant violated"):
        mod._cli_main()

    # KeyboardInterrupt is never intercepted by _cli_main
    monkeypatch.setattr(mod, "main", lambda: (_ for _ in ()).throw(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):
        mod._cli_main()


def test_cli_write_rejects_duplicate_destinations_and_redacts_credentials(tmp_path):
    """kfvf7i E-05: CLI --write rejects duplicate resolved destinations (exit 2) without touching files or leaking secrets."""
    src = tmp_path / "src.txt"
    src.write_bytes(b"fresh payload\n")
    existing = tmp_path / "out.txt"
    existing.write_bytes(b"original content\n")

    # 1. Identical local path
    res_same = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), str(existing), str(existing)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_same.returncode == 2
    assert "Duplicate destination specified" in res_same.stderr
    assert existing.read_bytes() == b"original content\n"
    assert not (tmp_path / "out.txt.001.bu").exists()

    # 2. ./ alias
    res_dot = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), "out.txt", "./out.txt"],
        capture_output=True,
        text=True,
        env=_cli_env(),
        cwd=str(tmp_path),
    )
    assert res_dot.returncode == 2
    assert "Duplicate destination specified" in res_dot.stderr
    assert existing.read_bytes() == b"original content\n"
    assert not (tmp_path / "out.txt.001.bu").exists()

    # 3. file:// alias
    res_uri = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), str(existing), existing.as_uri()],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_uri.returncode == 2
    assert "Duplicate destination specified" in res_uri.stderr
    assert existing.read_bytes() == b"original content\n"
    assert not (tmp_path / "out.txt.001.bu").exists()

    # 4. ssh:// vs sftp:// alias (default port 22)
    res_ssh_sftp = subprocess.run(
        [
            sys.executable,
            "-m",
            "openas",
            "--write",
            str(src),
            "ssh://alice@Example.COM/dir/out.txt",
            "sftp://bob@example.com:22/dir/out.txt",
        ],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_ssh_sftp.returncode == 2
    assert "Duplicate destination specified" in res_ssh_sftp.stderr

    # 5. Remote URL with inline password & query string: exit 2 and no password/token leak in stderr
    secret_url = "sftp://alice:supersecret@host/dir/out.txt?token=tok123"
    res_secret = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), secret_url, secret_url],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_secret.returncode == 2
    assert "Duplicate destination specified" in res_secret.stderr
    assert "supersecret" not in res_secret.stderr
    assert "tok123" not in res_secret.stderr
    assert "sftp://alice:***@host/dir/out.txt?<redacted>" in res_secret.stderr


def test_cli_write_distinct_local_symlinks_allowed_and_symlinked_parent_collides(tmp_path):
    """kfvf7i E-05: Distinct local symlinks (link.txt vs real.txt) remain allowed while symlinked parent dirs collide."""
    src = tmp_path / "src.txt"
    src.write_bytes(b"broadcast payload\n")
    real_dir = tmp_path / "real_dir"
    real_dir.mkdir()
    real_file = real_dir / "real.txt"
    real_file.write_bytes(b"real original\n")
    link_file = real_dir / "link.txt"
    sym_dir = tmp_path / "sym_dir"
    try:
        link_file.symlink_to(real_file)
        sym_dir.symlink_to(real_dir)
    except OSError:
        if os.name == "nt":
            pytest.skip("Windows symlink creation requires Developer Mode or elevated privileges")
        raise

    # Symlinked parent dir alias collides (exit 2) without touching files
    res_parent = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), str(real_file), str(sym_dir / "real.txt")],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_parent.returncode == 2
    assert "Duplicate destination specified" in res_parent.stderr
    assert real_file.read_bytes() == b"real original\n"
    assert not (real_dir / "real.txt.001.bu").exists()

    # Distinct local symlink (link.txt) and its target (real.txt) remain allowed
    res_distinct = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), str(link_file), str(real_file)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res_distinct.returncode == 0, res_distinct.stderr
    assert os.path.islink(real_dir / "link.txt.001.bu")
    assert (real_dir / "real.txt.001.bu").read_bytes() == b"real original\n"
    assert not os.path.islink(link_file)
    assert link_file.read_bytes() == b"broadcast payload\n"
    assert real_file.read_bytes() == b"broadcast payload\n"


def test_cli_write_local_directory_prevalidation_leaves_earlier_destination_untouched(tmp_path):
    """kfvf7i E-05: Existing local directory as 2nd destination raises IsADirectoryError before backing up 1st destination."""
    src = tmp_path / "src.txt"
    src.write_bytes(b"new payload\n")
    first_dst = tmp_path / "first.txt"
    first_dst.write_bytes(b"keep intact\n")
    existing_dir = tmp_path / "target_dir"
    existing_dir.mkdir()

    res = subprocess.run(
        [sys.executable, "-m", "openas", "--write", str(src), str(first_dst), str(existing_dir)],
        capture_output=True,
        text=True,
        env=_cli_env(),
    )
    assert res.returncode == 1
    assert "IsADirectoryError" in res.stderr
    assert first_dst.read_bytes() == b"keep intact\n"
    assert not (tmp_path / "first.txt.001.bu").exists()
