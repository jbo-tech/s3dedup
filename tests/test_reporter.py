"""Tests du reporter — rapports JSON et CSV."""

import csv
import json
from datetime import datetime
from io import StringIO

import pytest

from s3dedup.db import connect, upsert_objects
from s3dedup.models import ObjectInfo
from s3dedup.reporter import generate_report

NOW = datetime(2026, 1, 15, 12, 0, 0)


def _obj(key, size=100, etag="e1", multipart=False, sha256=None):
    return ObjectInfo(
        key=key, size=size, etag=etag,
        is_multipart=multipart, last_modified=NOW, sha256=sha256,
    )


@pytest.fixture
def db_with_duplicates():
    conn = connect(":memory:")
    upsert_objects(conn, [
        _obj("music/song.mp3", size=1000, etag="aaa"),
        _obj("backup/song.mp3", size=1000, etag="aaa"),
        _obj("unique.flac", size=5000, etag="bbb"),
    ])
    return conn


@pytest.fixture
def empty_db():
    return connect(":memory:")


class TestJsonReport:
    def test_valid_json(self, db_with_duplicates):
        result = generate_report(db_with_duplicates, fmt="json")
        data = json.loads(result)
        assert "stats" in data
        assert "groups" in data

    def test_stats_correct(self, db_with_duplicates):
        data = json.loads(generate_report(db_with_duplicates))
        stats = data["stats"]
        assert stats["total_objects"] == 3
        assert stats["duplicate_groups"] == 1
        assert stats["duplicate_objects"] == 1
        assert stats["wasted_bytes"] == 1000

    def test_group_content(self, db_with_duplicates):
        data = json.loads(generate_report(db_with_duplicates))
        groups = data["groups"]
        assert len(groups) == 1
        assert groups[0]["fingerprint"] == "aaa"
        assert len(groups[0]["objects"]) == 2

    def test_empty_db(self, empty_db):
        data = json.loads(generate_report(empty_db))
        assert data["stats"]["duplicate_groups"] == 0
        assert data["groups"] == []


class TestCsvReport:
    def test_valid_csv(self, db_with_duplicates):
        result = generate_report(db_with_duplicates, fmt="csv")
        reader = csv.reader(StringIO(result))
        rows = list(reader)
        # Header + 2 lignes (doublon)
        assert len(rows) == 3

    def test_header(self, db_with_duplicates):
        result = generate_report(db_with_duplicates, fmt="csv")
        reader = csv.reader(StringIO(result))
        header = next(reader)
        assert "group_fingerprint" in header
        assert "object_key" in header

    def test_empty_db(self, empty_db):
        result = generate_report(empty_db, fmt="csv")
        reader = csv.reader(StringIO(result))
        rows = list(reader)
        assert len(rows) == 1  # Header seul
