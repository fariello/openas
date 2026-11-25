import bz2
import gzip
import io
import lzma
import os
import pytest
import zstandard as zstd
from polyopen import polyopen

# The golden files injected contain 3 lines of byte-converted text.
EXPECTED_LINES = ["line 1\n", "line 2\n", "line 3\n"]


def _independent_decode(path, ext: str) -> str:
    if ext == ".txt":
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    if ext == ".txt.gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return fh.read()
    if ext == ".txt.bz2":
        with bz2.open(path, "rt", encoding="utf-8") as fh:
            return fh.read()
    if ext == ".txt.xz":
        with lzma.open(path, "rt", encoding="utf-8") as fh:
            return fh.read()
    if ext == ".txt.zst":
        with open(path, "rb") as raw:
            with zstd.ZstdDecompressor().stream_reader(raw, read_across_frames=True) as sr:
                return io.TextIOWrapper(sr, encoding="utf-8").read()
    raise ValueError(ext)


@pytest.mark.parametrize("ext", [".txt", ".txt.gz", ".txt.bz2", ".txt.xz", ".txt.zst"])
def test_golden_file_integrity(ext, tmp_path):
    """
    Guarantees that third-party streaming dependencies (like zstandard and lzma)
    do not alter their native extraction algorithms across API updates, and that
    polyopen writes streams readable by independent stdlib/zstandard decoders.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    fixture_path = os.path.join(base_dir, "fixtures", f"gold{ext}")

    assert os.path.exists(fixture_path), f"Golden fixture missing: {fixture_path}"

    with polyopen(fixture_path, 'r') as reader:
        lines = list(reader)
    assert lines == EXPECTED_LINES

    out_path = tmp_path / f"roundtrip{ext}"
    with polyopen(out_path, 'w', backup=False) as writer:
        writer.writelines(EXPECTED_LINES)
    assert _independent_decode(out_path, ext) == "".join(EXPECTED_LINES)
    with polyopen(out_path, 'r') as reader:
        assert list(reader) == EXPECTED_LINES

    expected_bytes = [line.encode("utf-8") for line in EXPECTED_LINES]
    with polyopen(fixture_path, 'rb') as reader:
        assert list(reader) == expected_bytes

    bin_out_path = tmp_path / f"roundtrip_bin{ext}"
    with polyopen(bin_out_path, 'wb', backup=False) as writer:
        writer.writelines(expected_bytes)
    assert _independent_decode(bin_out_path, ext) == "".join(EXPECTED_LINES)
    with polyopen(bin_out_path, 'rb') as reader:
        assert list(reader) == expected_bytes
