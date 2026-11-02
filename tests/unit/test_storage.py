"""
Unit tests for storage layer — Module 2.

Covers WAL and StorageEngine. Snapshot integration tested via engine recovery.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from docstore.storage.engine import DedupEntry, StorageEngine
from docstore.storage.snapshot import SnapshotManager
from docstore.storage.wal import WAL, WALCorruptionError

pytestmark = pytest.mark.unit

# ═══════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════


@pytest.fixture
def tmp_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def wal(tmp_dir: Path) -> WAL:
    w = WAL(tmp_dir / "test.wal")
    w.open()
    yield w
    w.close()


@pytest.fixture
def engine() -> StorageEngine:
    return StorageEngine()


# ═══════════════════════════════════════════════════════════
# WAL tests
# ═══════════════════════════════════════════════════════════


class TestWALAppendReplay:
    def test_append_single(self, wal: WAL, tmp_dir: Path) -> None:
        wal.append(seq=1, term=1, op="insert", doc_id=42, request_id="r1", payload={"doc_id": 42})
        wal.close()

        w2 = WAL(tmp_dir / "test.wal")
        records = list(w2.replay())
        assert len(records) == 1
        assert records[0]["seq"] == 1
        assert records[0]["doc_id"] == 42

    def test_append_multiple_ordered(self, wal: WAL, tmp_dir: Path) -> None:
        for i in range(1, 6):
            wal.append(
                seq=i,
                term=1,
                op="insert",
                doc_id=i * 10,
                request_id=f"r{i}",
                payload={"doc_id": i * 10},
            )
        wal.close()

        w2 = WAL(tmp_dir / "test.wal")
        records = list(w2.replay())
        assert [r["seq"] for r in records] == [1, 2, 3, 4, 5]

    def test_replay_after_seq_filter(self, wal: WAL, tmp_dir: Path) -> None:
        for i in range(1, 6):
            wal.append(
                seq=i, term=1, op="insert", doc_id=i, request_id=f"r{i}", payload={"doc_id": i}
            )
        wal.close()

        w2 = WAL(tmp_dir / "test.wal")
        records = list(w2.replay(after_seq=3))
        assert [r["seq"] for r in records] == [4, 5]

    def test_crc32_stored(self, wal: WAL) -> None:
        record = wal.append(seq=1, term=1, op="insert", doc_id=1, request_id="r1", payload=None)
        assert "crc32" in record
        assert isinstance(record["crc32"], int)

    def test_empty_wal_replay(self, tmp_dir: Path) -> None:
        w = WAL(tmp_dir / "empty.wal")
        records = list(w.replay())
        assert records == []


class TestWALTornTail:
    def test_torn_tail_truncated_and_ignored(self, tmp_dir: Path) -> None:
        """A torn final line (no trailing newline) is silently truncated."""
        wal_path = tmp_dir / "torn.wal"
        w = WAL(wal_path)
        w.open()
        w.append(seq=1, term=1, op="insert", doc_id=1, request_id="r1", payload={"doc_id": 1})
        w.close()

        # Corrupt: append a partial (torn) line without newline
        with open(wal_path, "ab") as f:
            f.write(b'{"seq":2,"term":1,"op":"insert"')  # no closing brace or newline

        w2 = WAL(wal_path)
        records = list(w2.replay())
        # Only seq=1 should be recovered
        assert len(records) == 1
        assert records[0]["seq"] == 1

    def test_fully_torn_file(self, tmp_dir: Path) -> None:
        """File with no complete records at all is treated as empty."""
        wal_path = tmp_dir / "all_torn.wal"
        wal_path.write_bytes(b'{"seq":1,"partial":true')  # no newline
        w = WAL(wal_path)
        records = list(w.replay())
        assert records == []


class TestWALCorruption:
    def test_bad_crc_raises(self, tmp_dir: Path) -> None:
        """A complete record with bad CRC must raise WALCorruptionError."""
        wal_path = tmp_dir / "corrupt.wal"
        w = WAL(wal_path)
        w.open()
        w.append(seq=1, term=1, op="insert", doc_id=1, request_id="r1", payload={"doc_id": 1})
        w.close()

        # Tamper: read, modify crc, write back
        lines = wal_path.read_text().splitlines()
        record = json.loads(lines[0])
        record["crc32"] = record["crc32"] ^ 0xDEADBEEF  # flip bits
        wal_path.write_text(json.dumps(record) + "\n")

        w2 = WAL(wal_path)
        with pytest.raises(WALCorruptionError, match="bad CRC32"):
            list(w2.replay())

    def test_good_records_before_bad_are_not_returned(self, tmp_dir: Path) -> None:
        """Corruption in record N means recovery fails (not partial success)."""
        wal_path = tmp_dir / "partial_corrupt.wal"
        w = WAL(wal_path)
        w.open()
        w.append(seq=1, term=1, op="insert", doc_id=1, request_id="r1", payload={"doc_id": 1})
        w.append(seq=2, term=1, op="insert", doc_id=2, request_id="r2", payload={"doc_id": 2})
        w.close()

        # Corrupt seq=1 record
        lines = wal_path.read_text().splitlines()
        r1 = json.loads(lines[0])
        r1["crc32"] = 0
        lines[0] = json.dumps(r1)
        wal_path.write_text("\n".join(lines) + "\n")

        w2 = WAL(wal_path)
        with pytest.raises(WALCorruptionError):
            list(w2.replay())


class TestWALTruncateAfter:
    def test_truncate_removes_tail(self, tmp_dir: Path) -> None:
        wal_path = tmp_dir / "trunc.wal"
        w = WAL(wal_path)
        w.open()
        for i in range(1, 6):
            w.append(
                seq=i, term=1, op="insert", doc_id=i, request_id=f"r{i}", payload={"doc_id": i}
            )
        w.truncate_after(3)
        w.close()

        w2 = WAL(wal_path)
        records = list(w2.replay())
        assert [r["seq"] for r in records] == [1, 2, 3]

    def test_truncate_next_seq_updated(self, tmp_dir: Path) -> None:
        wal_path = tmp_dir / "trunc2.wal"
        w = WAL(wal_path)
        w.open()
        for i in range(1, 4):
            w.append(
                seq=i, term=1, op="insert", doc_id=i, request_id=f"r{i}", payload={"doc_id": i}
            )
        w.truncate_after(2)
        assert w.next_seq() == 3


# ═══════════════════════════════════════════════════════════
# StorageEngine tests
# ═══════════════════════════════════════════════════════════


class TestEngineInsert:
    def test_insert_and_get(self, engine: StorageEngine) -> None:
        engine.insert({"doc_id": 1, "name": "Alice"}, seq=1)
        doc = engine.get(1)
        assert doc is not None
        assert doc["name"] == "Alice"

    def test_duplicate_insert_raises(self, engine: StorageEngine) -> None:
        from docstore.common.errors import DuplicateKey

        engine.insert({"doc_id": 1}, seq=1)
        with pytest.raises(DuplicateKey):
            engine.insert({"doc_id": 1}, seq=2)

    def test_get_missing_returns_none(self, engine: StorageEngine) -> None:
        assert engine.get(999) is None


class TestEngineUpdate:
    def test_full_replace(self, engine: StorageEngine) -> None:
        engine.insert({"doc_id": 1, "a": 1}, seq=1)
        engine.update({"doc_id": 1, "b": 2}, seq=2)
        doc = engine.get(1)
        assert doc is not None
        assert "a" not in doc
        assert doc["b"] == 2

    def test_update_missing_raises(self, engine: StorageEngine) -> None:
        from docstore.common.errors import NotFound

        with pytest.raises(NotFound):
            engine.update({"doc_id": 99}, seq=1)


class TestEngineDelete:
    def test_delete_removes(self, engine: StorageEngine) -> None:
        engine.insert({"doc_id": 5}, seq=1)
        engine.delete(5, seq=2)
        assert engine.get(5) is None

    def test_delete_missing_raises(self, engine: StorageEngine) -> None:
        from docstore.common.errors import NotFound

        with pytest.raises(NotFound):
            engine.delete(999, seq=1)


class TestEngineRangeScan:
    def test_range_scan_ordered(self, engine: StorageEngine) -> None:
        for i in [5, 1, 3, 7, 2]:
            engine.insert({"doc_id": i}, seq=i)
        result = engine.range_scan(2, 6)
        ids = [d["doc_id"] for d in result]
        assert ids == [2, 3, 5]  # ordered, half-open [2,6)

    def test_range_scan_empty(self, engine: StorageEngine) -> None:
        assert engine.range_scan(100, 200) == []

    def test_range_scan_boundary_inclusive_start(self, engine: StorageEngine) -> None:
        engine.insert({"doc_id": 10}, seq=1)
        result = engine.range_scan(10, 20)
        assert len(result) == 1

    def test_range_scan_boundary_exclusive_end(self, engine: StorageEngine) -> None:
        engine.insert({"doc_id": 20}, seq=1)
        result = engine.range_scan(10, 20)  # [10, 20) — 20 excluded
        assert len(result) == 0


class TestEngineIdempotentApply:
    def test_apply_idempotent_same_seq(self, engine: StorageEngine) -> None:
        entry = {
            "seq": 1,
            "term": 1,
            "op": "insert",
            "doc_id": 42,
            "request_id": "r1",
            "payload": {"doc_id": 42},
        }
        engine.apply(entry)
        engine.apply(entry)  # second call must be no-op
        assert engine.doc_count() == 1

    def test_apply_lower_seq_noop(self, engine: StorageEngine) -> None:
        e1 = {
            "seq": 1,
            "term": 1,
            "op": "insert",
            "doc_id": 1,
            "request_id": "r1",
            "payload": {"doc_id": 1},
        }
        e2 = {
            "seq": 2,
            "term": 1,
            "op": "insert",
            "doc_id": 2,
            "request_id": "r2",
            "payload": {"doc_id": 2},
        }
        engine.apply(e1)
        engine.apply(e2)
        engine.apply(e1)  # lower seq — no-op
        assert engine.doc_count() == 2

    def test_apply_migration_upsert(self, engine: StorageEngine) -> None:
        engine.insert({"doc_id": 7, "v": 1}, seq=1)
        entry = {
            "seq": 2,
            "term": 1,
            "op": "insert",
            "doc_id": 7,
            "request_id": "r2",
            "payload": {"doc_id": 7, "v": 2},
        }
        engine.apply(entry, migration_mode=True)
        assert engine.get(7)["v"] == 2  # type: ignore[index]

    def test_apply_migration_delete_noop(self, engine: StorageEngine) -> None:
        entry = {
            "seq": 1,
            "term": 1,
            "op": "delete",
            "doc_id": 999,
            "request_id": "r1",
            "payload": None,
        }
        engine.apply(entry, migration_mode=True)  # should not raise


class TestEngineRangeSnapshot:
    def test_snapshot_is_copy(self, engine: StorageEngine) -> None:
        engine.insert({"doc_id": 1, "x": 1}, seq=1)
        snap = engine.range_snapshot(0, 10)
        # Mutate original
        engine.update({"doc_id": 1, "x": 99}, seq=2)
        # Snapshot should be unchanged
        assert snap[0]["x"] == 1


class TestEngineDedup:
    def test_store_and_check(self, engine: StorageEngine) -> None:
        entry = DedupEntry(seq=1, outcome="COMMITTED", result={"outcome": "COMMITTED"})
        engine.dedup_store("req-abc", entry)
        found = engine.dedup_check("req-abc")
        assert found is not None
        assert found.seq == 1

    def test_missing_returns_none(self, engine: StorageEngine) -> None:
        assert engine.dedup_check("nonexistent") is None

    def test_export_import_roundtrip(self, engine: StorageEngine) -> None:
        entry = DedupEntry(seq=5, outcome="COMMITTED", result={"outcome": "COMMITTED"})
        engine.dedup_store("req-xyz", entry)
        exported = engine.dedup_export()

        engine2 = StorageEngine()
        engine2.dedup_import(exported)
        found = engine2.dedup_check("req-xyz")
        assert found is not None
        assert found.seq == 5


# ═══════════════════════════════════════════════════════════
# Snapshot tests
# ═══════════════════════════════════════════════════════════


class TestSnapshot:
    def test_save_and_load(self, tmp_dir: Path) -> None:
        mgr = SnapshotManager(tmp_dir)
        docs = {1: {"doc_id": 1, "name": "Alice"}, 2: {"doc_id": 2, "name": "Bob"}}
        mgr.save(
            documents=docs,
            last_applied_seq=5,
            term=1,
            ownership_epochs={"0-100": 1},
            dedup_table={},
        )
        snap = mgr.load()
        assert snap is not None
        assert snap["last_applied_seq"] == 5
        assert snap["documents"][1]["name"] == "Alice"
        assert snap["documents"][2]["name"] == "Bob"

    def test_no_snapshot_returns_none(self, tmp_dir: Path) -> None:
        mgr = SnapshotManager(tmp_dir / "empty")
        assert mgr.load() is None

    def test_atomic_write(self, tmp_dir: Path) -> None:
        """Temp file should not be left behind after save."""
        mgr = SnapshotManager(tmp_dir)
        mgr.save(documents={}, last_applied_seq=0, term=1, ownership_epochs={}, dedup_table={})
        tmp = tmp_dir / "snapshot.json.tmp"
        assert not tmp.exists()

    def test_directory_fsync_succeeds_save_succeeds(self, tmp_dir: Path) -> None:
        """When directory fsync succeeds, SnapshotManager.save succeeds."""
        mgr = SnapshotManager(tmp_dir)
        mgr.save(
            documents={1: {"doc_id": 1, "k": "v"}},
            last_applied_seq=1,
            term=1,
            ownership_epochs={"0-100": 1},
            dedup_table={},
        )
        assert mgr.exists()
        snap = mgr.load()
        assert snap is not None
        assert snap["last_applied_seq"] == 1

    def test_directory_fsync_raises_oserror_save_reports_failure(
        self, tmp_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When directory fsync fails with OSError, save must raise OSError (report failure)."""
        mgr = SnapshotManager(tmp_dir)

        def mock_fsync_dir(directory: Path) -> None:
            raise OSError("Simulated directory fsync EIO failure")

        monkeypatch.setattr("docstore.storage.snapshot._fsync_dir", mock_fsync_dir)

        with pytest.raises(OSError, match="Simulated directory fsync EIO failure"):
            mgr.save(
                documents={1: {"doc_id": 1, "k": "v"}},
                last_applied_seq=1,
                term=1,
                ownership_epochs={"0-100": 1},
                dedup_table={},
            )
