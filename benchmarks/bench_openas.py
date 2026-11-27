"""Reproducible performance benchmark suite for openas.

Measures nine workloads using only the Python standard library and openas
dependencies:
  1. Subprocess import latency (`import openas`)
  2. 200,000-line text read throughput across .txt, .gz, .bz2, .xz, .zst
     (show_progress=False and show_progress=True vs native iteration)
  3. 200,000-line local write throughput (OpenAsWriter vs native fh.write)
  4. Compression time and peak RSS for .zst and .xz writes vs idle child
  5. HTTP read throughput from a local 127.0.0.1 ephemeral-port server
  6. Local backup rotation latency with 500 existing .bu files
  7. CLI 3-target broadcast time via `python3 -m openas -w`
  8. 200,000-line binary ('rb') read throughput across .txt, .gz, .bz2, .xz, .zst
  9. 200,000-line OpenAsReader.readlines() throughput vs native fh.readlines()

Every generated file lives inside a `tempfile.TemporaryDirectory()`.
Run via:
    PYTHONPATH=src python3 benchmarks/bench_openas.py
"""

import bz2
import contextlib
import functools
import gzip
import http.server
import io
import lzma
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import zstandard as zstd

_REPO_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from openas import OpenAsReader, OpenAsWriter

REPS = 5
IMPORT_REPS = 7
READ_LINES = 200_000
WRITE_LINES = 200_000
COMPRESS_LINES = 20_000
HTTP_LINES = 100_000
CLI_LINES = 50_000
BACKUP_EXISTING = 500


def _subprocess_env() -> dict:
    """Return environment with checkout src/ prepended to PYTHONPATH."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{_REPO_SRC}{os.pathsep}{existing}" if existing else _REPO_SRC
    return env


def _median(values: list) -> float:
    return statistics.median(values)


def bench_import_time(env: dict) -> dict:
    """Workload 1: fresh-subprocess `import openas` latency."""
    code = (
        "import time; "
        "t0 = time.perf_counter(); "
        "import openas; "
        "print(f'{(time.perf_counter() - t0) * 1000.0:.4f}')"
    )
    # Warm OS page cache with one discarded invocation.
    subprocess.run(
        [sys.executable, "-c", "import openas"],
        check=True,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    import_ms_samples = []
    wall_ms_samples = []
    for _ in range(IMPORT_REPS):
        res = subprocess.run(
            [sys.executable, "-c", code],
            check=True,
            env=env,
            capture_output=True,
            text=True,
        )
        import_ms_samples.append(float(res.stdout.strip()))

        t0 = time.perf_counter()
        subprocess.run(
            [sys.executable, "-c", "import openas"],
            check=True,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        wall_ms_samples.append((time.perf_counter() - t0) * 1000.0)

    med_import_ms = _median(import_ms_samples)
    med_wall_ms = _median(wall_ms_samples)
    print(
        f"[1/9] import_openas: median={med_import_ms:.2f} ms "
        f"(subprocess_wall={med_wall_ms:.2f} ms, reps={IMPORT_REPS})"
    )
    return {"import_ms": med_import_ms, "wall_ms": med_wall_ms}


def _iter_native(path: str, ext: str) -> int:
    count = 0
    if ext == ".txt":
        with open(path, "r", encoding="utf-8") as fh:
            for _ in fh:
                count += 1
    elif ext == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for _ in fh:
                count += 1
    elif ext == ".bz2":
        with bz2.open(path, "rt", encoding="utf-8") as fh:
            for _ in fh:
                count += 1
    elif ext == ".xz":
        with lzma.open(path, "rt", encoding="utf-8") as fh:
            for _ in fh:
                count += 1
    elif ext == ".zst":
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        with open(path, "rb") as raw:
            with io.TextIOWrapper(
                dctx.stream_reader(raw, read_across_frames=True),
                encoding="utf-8",
            ) as fh:
                for _ in fh:
                    count += 1
    else:
        raise ValueError(f"Unsupported extension: {ext}")
    return count


def _build_read_fixtures(tmpdir: Path) -> dict:
    """Create shared 200,000-line fixtures across .txt, .gz, .bz2, .xz, and .zst."""
    lines = [
        f"2026-11-21T00:00:00Z INFO benchmark record {i:06d} "
        f"payload=0123456789abcdef0123456789abcdef\n"
        for i in range(READ_LINES)
    ]
    payload_bytes = "".join(lines).encode("utf-8")

    files = {}
    txt_path = tmpdir / "read_200k.txt"
    txt_path.write_bytes(payload_bytes)
    files[".txt"] = str(txt_path)

    gz_path = tmpdir / "read_200k.gz"
    with gzip.open(gz_path, "wb", compresslevel=6) as fh:
        fh.write(payload_bytes)
    files[".gz"] = str(gz_path)

    bz2_path = tmpdir / "read_200k.bz2"
    with bz2.open(bz2_path, "wb", compresslevel=9) as fh:
        fh.write(payload_bytes)
    files[".bz2"] = str(bz2_path)

    xz_path = tmpdir / "read_200k.xz"
    with lzma.open(xz_path, "wb", preset=6) as fh:
        fh.write(payload_bytes)
    files[".xz"] = str(xz_path)

    zst_path = tmpdir / "read_200k.zst"
    cctx = zstd.ZstdCompressor()
    zst_path.write_bytes(cctx.compress(payload_bytes))
    files[".zst"] = str(zst_path)
    return files


def bench_read_throughput(files: dict) -> None:
    """Workload 2: 200,000-line text read throughput across 5 formats."""
    for ext in (".txt", ".gz", ".bz2", ".xz", ".zst"):
        path = files[ext]

        native_times = []
        for _ in range(REPS):
            t0 = time.perf_counter()
            n = _iter_native(path, ext)
            native_times.append(time.perf_counter() - t0)
            if n != READ_LINES:
                raise RuntimeError(f"Native {ext} line mismatch: {n} != {READ_LINES}")

        plain_times = []
        for _ in range(REPS):
            t0 = time.perf_counter()
            n = 0
            with OpenAsReader(path, show_progress=False) as reader:
                for _ in reader:
                    n += 1
            plain_times.append(time.perf_counter() - t0)
            if n != READ_LINES:
                raise RuntimeError(f"OpenAsReader {ext} line mismatch: {n} != {READ_LINES}")

        prog_times = []
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            for _ in range(REPS):
                t0 = time.perf_counter()
                n = 0
                with contextlib.redirect_stderr(devnull):
                    with OpenAsReader(path, show_progress=True) as reader:
                        for _ in reader:
                            n += 1
                prog_times.append(time.perf_counter() - t0)
                if n != READ_LINES:
                    raise RuntimeError(
                        f"OpenAsReader(progress) {ext} line mismatch: {n} != {READ_LINES}"
                    )

        med_native = _median(native_times)
        med_plain = _median(plain_times)
        med_prog = _median(prog_times)
        ratio_plain = med_plain / med_native if med_native > 0 else float("inf")
        ratio_prog = med_prog / med_native if med_native > 0 else float("inf")
        print(
            f"[2/9] read_200k {ext:4s} (progress=False): "
            f"openas={med_plain:.4f}s native={med_native:.4f}s "
            f"ratio={ratio_plain:.2f}x (reps={REPS})"
        )
        print(
            f"[2/9] read_200k {ext:4s} (progress=True ): "
            f"openas={med_prog:.4f}s native={med_native:.4f}s "
            f"ratio={ratio_prog:.2f}x (reps={REPS})"
        )


def bench_write_throughput(tmpdir: Path) -> None:
    """Workload 3: 200,000-line local write throughput."""
    lines = [f"line-{i:06d} payload=0123456789abcdef\n" for i in range(WRITE_LINES)]
    native_out = str(tmpdir / "write_native.txt")
    openas_out = str(tmpdir / "write_openas.txt")

    native_times = []
    for _ in range(REPS):
        t0 = time.perf_counter()
        with open(native_out, "w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(line)
        native_times.append(time.perf_counter() - t0)

    openas_times = []
    for _ in range(REPS):
        t0 = time.perf_counter()
        with OpenAsWriter(openas_out).open(append=False, backup=False) as writer:
            for line in lines:
                writer.write(line)
        openas_times.append(time.perf_counter() - t0)

    med_native = _median(native_times)
    med_openas = _median(openas_times)
    ratio = med_openas / med_native if med_native > 0 else float("inf")
    print(
        f"[3/9] write_200k .txt: openas={med_openas:.4f}s native={med_native:.4f}s "
        f"ratio={ratio:.2f}x (reps={REPS})"
    )


def _measure_child_rss_and_time(child_code: str, env: dict) -> tuple:
    """Spawn a dedicated wrapper subprocess that runs child_code in a grandchild
    and reports elapsed seconds and RUSAGE_CHILDREN ru_maxrss in KiB (0 on non-POSIX)."""
    wrapper = (
        "import subprocess, sys, time\n"
        "try:\n"
        "    import resource\n"
        "except ImportError:\n"
        "    resource = None\n"
        "t0 = time.perf_counter()\n"
        "subprocess.run([sys.executable, '-c', sys.argv[1]], check=True)\n"
        "elapsed = time.perf_counter() - t0\n"
        "if resource is not None:\n"
        "    rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss\n"
        "    rss = rss // 1024 if sys.platform == 'darwin' else rss\n"
        "else:\n"
        "    rss = 0\n"
        "print(f'{elapsed:.6f} {rss}')\n"
    )
    res = subprocess.run(
        [sys.executable, "-c", wrapper, child_code],
        check=True,
        env=env,
        capture_output=True,
        text=True,
    )
    elapsed_str, rss_str = res.stdout.strip().split()
    return float(elapsed_str), int(rss_str)


def bench_compression_rss(tmpdir: Path, env: dict) -> None:
    """Workload 4: .zst and .xz write compression time and peak RSS vs idle child."""
    idle_code = "import openas"
    idle_times = []
    idle_rss_samples = []
    for _ in range(REPS):
        elapsed, rss_kib = _measure_child_rss_and_time(idle_code, env)
        idle_times.append(elapsed)
        idle_rss_samples.append(rss_kib)
    idle_rss = int(_median(idle_rss_samples))
    print(
        f"[4/9] compress_rss idle_child: median_time={_median(idle_times):.4f}s "
        f"peak_rss={idle_rss} KiB (reps={REPS})"
    )

    for ext in (".zst", ".xz"):
        out_path = str(tmpdir / f"compress_bench{ext}")
        child_code = (
            "from openas import OpenAsWriter\n"
            f"lines = [f'record-{{i:06d}} ' + ('abcdefghij' * 8) + '\\n' for i in range({COMPRESS_LINES})]\n"
            f"w = OpenAsWriter({out_path!r}).open(append=False, backup=False)\n"
            "try:\n"
            "    for line in lines:\n"
            "        w.write(line)\n"
            "finally:\n"
            "    w.close()\n"
        )
        times = []
        rss_samples = []
        for _ in range(REPS):
            elapsed, rss_kib = _measure_child_rss_and_time(child_code, env)
            times.append(elapsed)
            rss_samples.append(rss_kib)
        med_time = _median(times)
        med_rss = int(_median(rss_samples))
        delta_rss = med_rss - idle_rss
        print(
            f"[4/9] compress_write {ext:4s}: median_time={med_time:.4f}s "
            f"peak_rss={med_rss} KiB (delta_vs_idle={delta_rss:+d} KiB, reps={REPS})"
        )


def bench_http_read(tmpdir: Path) -> None:
    """Workload 5: HTTP read throughput from a local ephemeral-port http.server."""
    http_dir = tmpdir / "http_root"
    http_dir.mkdir()
    data_file = http_dir / "payload.txt"
    payload = "".join(
        f"http-line-{i:06d} 0123456789abcdef0123456789abcdef\n"
        for i in range(HTTP_LINES)
    )
    data_bytes = payload.encode("utf-8")
    data_file.write_bytes(data_bytes)

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format, *args):  # noqa: A002
            return

    handler = functools.partial(QuietHandler, directory=str(http_dir))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    url = f"http://127.0.0.1:{port}/payload.txt"
    try:
        times = []
        for _ in range(REPS):
            t0 = time.perf_counter()
            n = 0
            with OpenAsReader(url) as reader:
                for _ in reader:
                    n += 1
            times.append(time.perf_counter() - t0)
            if n != HTTP_LINES:
                raise RuntimeError(f"HTTP line mismatch: {n} != {HTTP_LINES}")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    med_time = _median(times)
    mib_s = (len(data_bytes) / (1024 * 1024)) / med_time if med_time > 0 else 0.0
    print(
        f"[5/9] http_read_{HTTP_LINES}: median={med_time:.4f}s "
        f"({mib_s:.1f} MiB/s, reps={REPS})"
    )


def bench_backup_rotation(tmpdir: Path) -> None:
    """Workload 6: local backup rotation latency with 500 existing .bu files."""
    bu_dir = tmpdir / "backups"
    bu_dir.mkdir()
    target = bu_dir / "target.txt"
    for idx in range(1, BACKUP_EXISTING + 1):
        (bu_dir / f"target.txt.{idx:03d}.bu").touch()
    expected_backup = bu_dir / f"target.txt.{BACKUP_EXISTING + 1:03d}.bu"

    times_ms = []
    for _ in range(REPS):
        if expected_backup.exists():
            expected_backup.unlink()
        target.write_text("previous\n", encoding="utf-8")
        t0 = time.perf_counter()
        with OpenAsWriter(str(target)).open(append=False, backup=True) as writer:
            writer.write("updated\n")
        times_ms.append((time.perf_counter() - t0) * 1000.0)
        if not expected_backup.exists():
            raise RuntimeError(f"Expected backup not created: {expected_backup}")

    med_ms = _median(times_ms)
    print(
        f"[6/9] backup_rotation_500: median={med_ms:.3f} ms "
        f"(existing={BACKUP_EXISTING}, reps={REPS})"
    )


def bench_cli_broadcast(tmpdir: Path, env: dict) -> None:
    """Workload 7: CLI 3-target broadcast via `python3 -m openas -w`."""
    src_path = tmpdir / "cli_src.txt.gz"
    payload = "".join(
        f"cli-line-{i:06d} 0123456789abcdef0123456789abcdef\n"
        for i in range(CLI_LINES)
    ).encode("utf-8")
    with gzip.open(src_path, "wb", compresslevel=6) as fh:
        fh.write(payload)

    dests = [tmpdir / f"cli_dst_{k}.txt" for k in (1, 2, 3)]
    times = []
    for _ in range(REPS):
        for dst in dests:
            if dst.exists():
                dst.unlink()
            for bu in tmpdir.glob(f"{dst.name}.*.bu"):
                bu.unlink()
        t0 = time.perf_counter()
        subprocess.run(
            [
                sys.executable,
                "-m",
                "openas",
                "-w",
                str(src_path),
                *(str(d) for d in dests),
            ],
            check=True,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        times.append(time.perf_counter() - t0)
        for dst in dests:
            if not dst.exists():
                raise RuntimeError(f"CLI broadcast did not create {dst}")

    med_time = _median(times)
    print(f"[7/9] cli_broadcast_3_targets: median={med_time:.4f}s (reps={REPS})")


def _iter_native_binary(path: str, ext: str) -> int:
    count = 0
    if ext == ".txt":
        with open(path, "rb") as fh:
            for _ in fh:
                count += 1
    elif ext == ".gz":
        with gzip.open(path, "rb") as fh:
            for _ in fh:
                count += 1
    elif ext == ".bz2":
        with bz2.open(path, "rb") as fh:
            for _ in fh:
                count += 1
    elif ext == ".xz":
        with lzma.open(path, "rb") as fh:
            for _ in fh:
                count += 1
    elif ext == ".zst":
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        with open(path, "rb") as raw:
            with io.BufferedReader(
                dctx.stream_reader(raw, read_across_frames=True)
            ) as fh:
                for _ in fh:
                    count += 1
    else:
        raise ValueError(f"Unsupported extension: {ext}")
    return count


def bench_binary_read_throughput(files: dict) -> None:
    """Workload 8: 200,000-line binary ('rb') read throughput across 5 formats."""
    for ext in (".txt", ".gz", ".bz2", ".xz", ".zst"):
        path = files[ext]

        native_times = []
        for _ in range(REPS):
            t0 = time.perf_counter()
            n = _iter_native_binary(path, ext)
            native_times.append(time.perf_counter() - t0)
            if n != READ_LINES:
                raise RuntimeError(f"Native rb {ext} line mismatch: {n} != {READ_LINES}")

        openas_times = []
        for _ in range(REPS):
            t0 = time.perf_counter()
            n = 0
            with OpenAsReader(path, binary=True) as reader:
                for _ in reader:
                    n += 1
            openas_times.append(time.perf_counter() - t0)
            if n != READ_LINES:
                raise RuntimeError(f"OpenAsReader rb {ext} line mismatch: {n} != {READ_LINES}")

        med_native = _median(native_times)
        med_openas = _median(openas_times)
        ratio = med_openas / med_native if med_native > 0 else float("inf")
        speedup = med_native / med_openas if med_openas > 0 else float("inf")
        print(
            f"[8/9] read_200k_rb {ext:4s}: "
            f"openas={med_openas:.4f}s native={med_native:.4f}s "
            f"ratio={ratio:.2f}x (speedup={speedup:.2f}x, reps={REPS})"
        )


def bench_readlines_throughput(files: dict) -> None:
    """Workload 9: 200,000-line OpenAsReader.readlines() vs native fh.readlines()."""
    for ext in (".txt", ".gz"):
        path = files[ext]

        native_times = []
        for _ in range(REPS):
            t0 = time.perf_counter()
            if ext == ".txt":
                with open(path, "r", encoding="utf-8") as fh:
                    lines = fh.readlines()
            else:
                with gzip.open(path, "rt", encoding="utf-8") as fh:
                    lines = fh.readlines()
            native_times.append(time.perf_counter() - t0)
            if len(lines) != READ_LINES:
                raise RuntimeError(f"Native readlines {ext} mismatch: {len(lines)} != {READ_LINES}")

        openas_times = []
        for _ in range(REPS):
            t0 = time.perf_counter()
            with OpenAsReader(path) as reader:
                lines = reader.readlines()
            openas_times.append(time.perf_counter() - t0)
            if len(lines) != READ_LINES:
                raise RuntimeError(f"OpenAsReader readlines {ext} mismatch: {len(lines)} != {READ_LINES}")

        med_native = _median(native_times)
        med_openas = _median(openas_times)
        ratio = med_openas / med_native if med_native > 0 else float("inf")
        print(
            f"[9/9] readlines_200k {ext:4s}: "
            f"openas={med_openas:.4f}s native={med_native:.4f}s "
            f"ratio={ratio:.2f}x (reps={REPS})"
        )


def main() -> int:
    env = _subprocess_env()
    print(f"openas benchmark (Python {sys.version.split()[0]}, reps={REPS})")
    with tempfile.TemporaryDirectory(prefix="openas-bench-") as raw_dir:
        tmpdir = Path(raw_dir)
        bench_import_time(env)
        read_files = _build_read_fixtures(tmpdir)
        bench_read_throughput(read_files)
        bench_write_throughput(tmpdir)
        bench_compression_rss(tmpdir, env)
        bench_http_read(tmpdir)
        bench_backup_rotation(tmpdir)
        bench_cli_broadcast(tmpdir, env)
        bench_binary_read_throughput(read_files)
        bench_readlines_throughput(read_files)
    return 0


if __name__ == "__main__":
    sys.exit(main())
