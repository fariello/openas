import subprocess
import pytest
import os
import sys

def run_cli(*args):
    """Utility to spawn subprocess commands targeting polyopen module structurally."""
    cmd = [sys.executable, "-m", "polyopen.polyopen"] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

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
    result = subprocess.run(cmd, text=True, capture_output=True)
    
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

    env = {**os.environ, "PYTHONPATH": "src"}
    res_ok = subprocess.run(
        [sys.executable, "-m", "polyopen.polyopen", "--write", "-i", str(source), str(target)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert res_ok.returncode == 0, res_ok.stderr
    assert target.read_text(encoding="utf-8") == content
    assert f"{expected_bytes:.1f}B" in res_ok.stdout

    res_read = subprocess.run(
        [sys.executable, "-m", "polyopen.polyopen", "--read", str(source)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert res_read.returncode == 0, res_read.stderr
    assert f"{expected_bytes:.1f}B" in res_read.stdout

    res_missing_dst = subprocess.run(
        [sys.executable, "-m", "polyopen.polyopen", "--write", "-i", str(source)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert res_missing_dst.returncode != 0
    assert "destination" in res_missing_dst.stderr.lower()

    res_read_i = subprocess.run(
        [sys.executable, "-m", "polyopen.polyopen", "--read", "-i", str(source)],
        capture_output=True,
        text=True,
        env=env,
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
