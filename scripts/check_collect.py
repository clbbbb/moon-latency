"""Independent MLS1 fixtures and end-to-end checks of the Node CLI."""
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import zlib


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "_build/js/release/build/cmd/collect/collect.js"
PRODUCER = ROOT / "_build/js/release/build/cmd/produce/produce.js"


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
    bad.write_bytes(bytes(9874439))
    assert "exceeds the maximum batch size" in invoke(first, bad, success=False)
    bad.write_bytes(shard(4, 1, 0, 2, [(20, 1)]))
    assert "older than the active window" in invoke(first, bad, success=False)
    batch = directory / "shards.mlb"
    items = [first.read_bytes(), second.read_bytes(), second.read_bytes()]
    batch.write_bytes(b"MLB1" + struct.pack(">H", len(items)) + b"".join(
        struct.pack(">H", len(item)) + item for item in items
    ))
    assert invoke(batch) == invoke(first, second, second)
    batch.write_bytes(batch.read_bytes()[:-1])
    assert "invalid batch encoding" in invoke(batch, success=False)
    csv = directory / "samples.csv"
    csv.write_bytes(b"11,20,190\r\n30,375,10")
    output = subprocess.run(
        ["node", str(PRODUCER), "ms", "8", "10", "10", str(csv)],
        check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout.splitlines()
    assert len(output) == 2
    first.write_bytes(bytes.fromhex(output[0]))
    second.write_bytes(bytes.fromhex(output[1]))
    assert invoke(second, first, first)["total"] == "200"
    assert invoke(second, first, first)["duplicate"] == 1
    csv.write_bytes(b"11,20,190\n30,375,10\n\xff,3\n")
    failed = subprocess.run(
        ["node", str(PRODUCER), "ms", "8", "10", "10", str(csv)],
        check=False, capture_output=True, text=True, encoding="utf-8",
    )
    assert failed.returncode == 2 and "row 3" in failed.stderr
    assert len(failed.stdout.splitlines()) == 1
    journal = directory / "shards.mlj"
    packed = subprocess.run(
        ["node", str(CLI), "journal", "pack", str(first), str(second), str(first)],
        check=True, capture_output=True,
    ).stdout
    assert packed[:4] == b"MLJ1"
    size = struct.unpack_from(">I", packed, 4)[0]
    assert size + 12 == len(packed)
    assert struct.unpack_from(">I", packed, 8 + size)[0] == zlib.crc32(packed[8:8 + size])
    journal.write_bytes(packed)
    replay = ["node", str(CLI), "journal", "replay", "ms", "10", "3", "400", "990000", str(journal)]
    result = json.loads(subprocess.check_output(replay, text=True, encoding="utf-8"))
    assert result["frames"] == "1" and result["summary"]["total"] == "200"
    assert [item["epoch"] for item in result["epochs"]] == ["1", "2", "3"]
    assert result["epochs"][1]["result"]["status"] == "no_samples"
    journal.write_bytes(packed[:-1] + bytes([packed[-1] ^ 1]))
    failed = subprocess.run(replay, capture_output=True, text=True, encoding="utf-8", check=False)
    assert failed.returncode == 2 and "checksum mismatch" in failed.stderr
    config = directory / "fleet.json"
    config.write_text(json.dumps({"version": "1", "services": [
        {"service": "frontend", "unit": "ms", "width": "10", "slots": 3,
         "identities": 10, "objectives": [{"name": "p99", "threshold": "400", "target": 990000}]},
        {"service": "workers", "unit": "us", "width": "5", "slots": 2,
         "identities": 10, "objectives": [{"name": "all", "threshold": "20", "target": 1000000}]},
    ]}), encoding="utf-8")
    worker = directory / "workers.mls"
    worker.write_bytes(shard(8, 10, 200, 1, [(20, 54)]))
    fleet = ["node", str(CLI), "fleet", str(config), f"frontend={first}", f"workers={worker}"]
    output = json.loads(subprocess.check_output(fleet, text=True, encoding="utf-8"))
    assert [item["unit"] for item in output["services"]] == ["ms", "us"]
    assert [item["objectives"][0]["report"]["total"] for item in output["services"]] == ["190", "54"]
print("MLS1 CLI interoperability checks passed")
