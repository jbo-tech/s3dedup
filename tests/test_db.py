"""Tests du module db — index DuckDB."""

from datetime import datetime

from s3dedup.db import (
    connect,
    find_etag_duplicates,
    find_hash_duplicates,
    find_multipart_candidates,
    find_size_duplicates,
    get_all_duplicates,
    get_stats,
    update_sha256,
    upsert_objects,
)
from s3dedup.models import ObjectInfo

NOW = datetime(2026, 1, 15, 12, 0, 0)


def _make_obj(key, size=100, etag="abc123", multipart=False, sha256=None):
    """Crée un ObjectInfo pour les tests."""
    return ObjectInfo(
        key=key,
        size=size,
        etag=etag,
        is_multipart=multipart,
        last_modified=NOW,
        sha256=sha256,
    )


def _mem_db():
    """Connexion DuckDB en mémoire."""
    return connect(":memory:")


class TestConnect:
    def test_creates_table(self):
        conn = _mem_db()
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
        assert ("objects",) in tables

    def test_idempotent(self):
        """Appeler connect deux fois ne plante pas."""
        conn = _mem_db()
        conn.execute(
            "CREATE TABLE IF NOT EXISTS objects ("
            "key VARCHAR PRIMARY KEY, size BIGINT NOT NULL,"
            "etag VARCHAR NOT NULL, is_multipart BOOLEAN NOT NULL,"
            "sha256 VARCHAR, last_modified TIMESTAMP NOT NULL,"
            "scanned_at TIMESTAMP NOT NULL DEFAULT now())"
        )


class TestUpsert:
    def test_insert(self):
        conn = _mem_db()
        count = upsert_objects(conn, [_make_obj("a.txt"), _make_obj("b.txt")])
        assert count == 2
        rows = conn.execute("SELECT count(*) FROM objects").fetchone()
        assert rows[0] == 2

    def test_upsert_updates(self):
        conn = _mem_db()
        upsert_objects(conn, [_make_obj("a.txt", size=100)])
        upsert_objects(conn, [_make_obj("a.txt", size=200)])
        row = conn.execute(
            "SELECT size FROM objects WHERE key = 'a.txt'"
        ).fetchone()
        assert row[0] == 200

    def test_empty_list(self):
        conn = _mem_db()
        assert upsert_objects(conn, []) == 0


class TestFindSizeDuplicates:
    def test_groups_by_size(self):
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100, etag="e1"),
            _make_obj("b.txt", size=100, etag="e2"),
            _make_obj("c.txt", size=200, etag="e3"),
        ])
        groups = find_size_duplicates(conn)
        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_no_duplicates(self):
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100),
            _make_obj("b.txt", size=200),
        ])
        assert find_size_duplicates(conn) == []


class TestFindEtagDuplicates:
    def test_groups_by_etag(self):
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100, etag="same"),
            _make_obj("b.txt", size=100, etag="same"),
            _make_obj("c.txt", size=100, etag="diff"),
        ])
        groups = find_etag_duplicates(conn)
        assert len(groups) == 1
        assert groups[0].fingerprint == "same"
        assert len(groups[0].objects) == 2
        assert groups[0].wasted_bytes == 100

    def test_same_etag_different_size_not_grouped(self):
        """ETag identique mais taille différente : pas un doublon."""
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100, etag="same"),
            _make_obj("b.txt", size=200, etag="same"),
        ])
        assert find_etag_duplicates(conn) == []


class TestMultipartCandidates:
    def test_finds_multipart_groups(self):
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100, etag="abc-2", multipart=True),
            _make_obj("b.txt", size=100, etag="def-3", multipart=True),
        ])
        groups = find_multipart_candidates(conn)
        assert len(groups) == 1
        assert len(groups[0]) == 2

    def test_ignores_already_hashed(self):
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100, multipart=True, sha256="h1"),
            _make_obj("b.txt", size=100, multipart=True, sha256="h2"),
        ])
        assert find_multipart_candidates(conn) == []


class TestUpdateSha256:
    def test_updates_hash(self):
        conn = _mem_db()
        upsert_objects(conn, [_make_obj("a.txt")])
        update_sha256(conn, "a.txt", "deadbeef")
        row = conn.execute(
            "SELECT sha256 FROM objects WHERE key = 'a.txt'"
        ).fetchone()
        assert row[0] == "deadbeef"


class TestFindHashDuplicates:
    def test_groups_by_sha256(self):
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100, sha256="same_hash"),
            _make_obj("b.txt", size=100, sha256="same_hash"),
            _make_obj("c.txt", size=100, sha256="other"),
        ])
        groups = find_hash_duplicates(conn)
        assert len(groups) == 1
        assert groups[0].fingerprint == "same_hash"


class TestGetAllDuplicates:
    def test_combines_etag_and_hash(self):
        conn = _mem_db()
        upsert_objects(conn, [
            # Doublons ETag (non-multipart)
            _make_obj("a.txt", size=100, etag="e1"),
            _make_obj("b.txt", size=100, etag="e1"),
            # Doublons SHA256 (multipart)
            _make_obj("c.txt", size=200, etag="x-2", multipart=True,
                       sha256="h1"),
            _make_obj("d.txt", size=200, etag="y-3", multipart=True,
                       sha256="h1"),
        ])
        groups = get_all_duplicates(conn)
        assert len(groups) == 2

    def test_excludes_mixed_multipart_from_etag(self):
        """Un groupe ETag avec un multipart est exclu des résultats ETag."""
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100, etag="e1", multipart=False),
            _make_obj("b.txt", size=100, etag="e1", multipart=True),
        ])
        groups = get_all_duplicates(conn)
        assert len(groups) == 0


class TestGetStats:
    def test_stats(self):
        conn = _mem_db()
        upsert_objects(conn, [
            _make_obj("a.txt", size=100, etag="e1"),
            _make_obj("b.txt", size=100, etag="e1"),
            _make_obj("c.txt", size=300, etag="e2"),
        ])
        stats = get_stats(conn)
        assert stats.total_objects == 3
        assert stats.total_size == 500
        assert stats.duplicate_groups == 1
        assert stats.duplicate_objects == 1
        assert stats.wasted_bytes == 100

    def test_empty_db(self):
        conn = _mem_db()
        stats = get_stats(conn)
        assert stats.total_objects == 0
        assert stats.wasted_bytes == 0
