"""Independent MLS1 fixtures and end-to-end checks of the Node CLI."""
import json
from pathlib import Path
import struct
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "_build/js/release/build/cmd/collect/collect.js"


def shard(source, sequence, timestamp, unit, buckets):
    header = struct.pack(">4sBqqq4s", b"MLS1", unit, source, sequence, timestamp, b"MLH1")
    return header + b"".join(struct.pack(">Hq", index, count) for index, count in buckets)


def invoke(*paths, success=True):
    proc = subprocess.run(
        ["node", str(CLI), "ms", "10", "3", "400", "990000", *map(str, paths)],
        text=True, encoding="utf-8", capture_output=True, check=False,
    )
    assert proc.returncode == (0 if success else 2), proc
    return json.loads(proc.stdout) if success else proc.stderr


with tempfile.TemporaryDirectory() as folder:
    directory = Path(folder)
    first = directory / "first.mls"
    second = directory / "second.mls"
    bad = directory / "bad.mls"
    # Bucket 20 is exact. Bucket 87 covers 368..383, entirely below 400.
    first.write_bytes(shard(3, 1, 30, 2, [(20, 190)]))
    second.write_bytes(shard(4, 1, 11, 2, [(87, 10)]))
    assert invoke(first, second, second) == {
        "status": "met", "total": "200", "lower": "200", "upper": "200",
        "required": "198", "added": 2, "duplicate": 1,
    }
    bad.write_bytes(shard(4, 1, 11, 2, [(87, 11)]))
    assert "conflicting shard identity" in invoke(first, second, bad, success=False)
    bad.write_bytes(shard(4, 1, 11, 1, [(87, 10)]))
    assert "unit mismatch" in invoke(first, bad, success=False)
    bad.write_bytes(shard(4, 1, 11, 2, [(20, 1), (20, 1)]))
    assert "invalid shard encoding" in invoke(first, bad, success=False)
    bad.write_bytes(bytes(9634))
    assert "exceeds 9633 bytes" in invoke(first, bad, success=False)
    bad.write_bytes(shard(4, 1, 0, 2, [(20, 1)]))
    assert "older than the active window" in invoke(first, bad, success=False)
print("MLS1 CLI interoperability checks passed")
