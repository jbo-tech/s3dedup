"""Tests du reporter — rapports JSON et CSV."""

import csv
import json
from datetime import datetime
from io import StringIO

import pytest

from s3dedup.db import connect, upsert_media_metadata, upsert_objects
from s3dedup.models import MediaMetadata, ObjectInfo
from s3dedup.reporter import find_suspect_names, generate_report

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
        data = json.loads(generate_report(db_with_duplicates, fmt="json"))
        stats = data["stats"]
        assert stats["total_objects"] == 3
        assert stats["duplicate_groups"] == 1
        assert stats["duplicate_objects"] == 1
        assert stats["wasted_bytes"] == 1000

    def test_group_content(self, db_with_duplicates):
        data = json.loads(generate_report(db_with_duplicates, fmt="json"))
        groups = data["groups"]
        assert len(groups) == 1
        assert groups[0]["fingerprint"] == "aaa"
        assert len(groups[0]["objects"]) == 2

    def test_empty_db(self, empty_db):
        data = json.loads(generate_report(empty_db, fmt="json"))
        assert data["stats"]["duplicate_groups"] == 0
        assert data["groups"] == []


class TestTableReport:
    def test_contains_summary(self, db_with_duplicates):
        result = generate_report(db_with_duplicates, fmt="table")
        assert "Résumé" in result
        assert "Espace récupérable" in result

    def test_contains_files(self, db_with_duplicates):
        result = generate_report(db_with_duplicates, fmt="table")
        assert "music/song.mp3" in result
        assert "backup/song.mp3" in result

    def test_empty_db(self, empty_db):
        result = generate_report(empty_db, fmt="table")
        assert "Aucun doublon" in result

    def test_is_default_format(self, db_with_duplicates):
        result = generate_report(db_with_duplicates)
        assert "Résumé" in result


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
        assert "group_id" in header
        assert "object_key" in header

    def test_empty_db(self, empty_db):
        result = generate_report(empty_db, fmt="csv")
        reader = csv.reader(StringIO(result))
        rows = list(reader)
        assert len(rows) == 1  # Header seul


@pytest.fixture
def db_with_suspect_names():
    """DB avec des fichiers aux noms similaires mais contenus différents."""
    conn = connect(":memory:")
    upsert_objects(conn, [
        # Même nom normalisé ("photo ete.jpg") mais etags différents
        _obj("photo été.jpg", size=1000, etag="aaa"),
        _obj("photo ete.jpg", size=1200, etag="bbb"),
        # Suffixe de copie, même nom normalisé mais contenu différent
        _obj("doc/rapport.pdf", size=500, etag="ccc"),
        _obj("doc/rapport (1).pdf", size=600, etag="ddd"),
        # Même nom normalisé ET même contenu → pas suspect (déjà doublon)
        _obj("music/song.mp3", size=800, etag="eee"),
        _obj("music/song (1).mp3", size=800, etag="eee"),
        # Fichier unique
        _obj("unique.txt", size=100, etag="fff"),
    ])
    return conn


class TestFindSuspectNames:
    def test_finds_different_content_same_name(
        self, db_with_suspect_names,
    ):
        groups = find_suspect_names(db_with_suspect_names)
        normalized_names = {g["normalized"] for g in groups}
        assert "photo ete.jpg" in normalized_names

    def test_finds_copy_suffix_suspects(
        self, db_with_suspect_names,
    ):
        groups = find_suspect_names(db_with_suspect_names)
        normalized_names = {g["normalized"] for g in groups}
        assert "rapport.pdf" in normalized_names

    def test_excludes_same_content(self, db_with_suspect_names):
        """Même nom normalisé + même etag → pas suspect."""
        groups = find_suspect_names(db_with_suspect_names)
        normalized_names = {g["normalized"] for g in groups}
        assert "song.mp3" not in normalized_names

    def test_excludes_unique_files(self, db_with_suspect_names):
        groups = find_suspect_names(db_with_suspect_names)
        normalized_names = {g["normalized"] for g in groups}
        assert "unique.txt" not in normalized_names

    def test_empty_db(self, empty_db):
        assert find_suspect_names(empty_db) == []

    def test_files_listed_in_group(self, db_with_suspect_names):
        groups = find_suspect_names(db_with_suspect_names)
        photo_group = next(
            g for g in groups if g["normalized"] == "photo ete.jpg"
        )
        keys = {f["key"] for f in photo_group["files"]}
        assert keys == {"photo été.jpg", "photo ete.jpg"}


class TestSuspectNamesInReport:
    def test_table_report_includes_suspects(
        self, db_with_suspect_names,
    ):
        result = generate_report(db_with_suspect_names, fmt="table")
        assert "Noms suspects" in result
        assert "photo ete.jpg" in result

    def test_json_report_includes_suspects(
        self, db_with_suspect_names,
    ):
        result = generate_report(db_with_suspect_names, fmt="json")
        data = json.loads(result)
        assert "suspect_names" in data
        assert len(data["suspect_names"]) >= 1

    def test_csv_report_includes_suspects(
        self, db_with_suspect_names,
    ):
        result = generate_report(db_with_suspect_names, fmt="csv")
        assert "suspect_name" in result

    def test_no_suspects_no_section(self, db_with_duplicates):
        """Pas de noms suspects → pas de section dans le JSON."""
        result = generate_report(db_with_duplicates, fmt="json")
        data = json.loads(result)
        assert "suspect_names" not in data


@pytest.fixture
def db_with_media_groups():
    """DB avec des fichiers média même œuvre, encodages différents."""
    conn = connect(":memory:")
    upsert_objects(conn, [
        _obj("music/song.flac", size=50_000_000, etag="aaa"),
        _obj("music/song.mp3", size=5_000_000, etag="bbb"),
        _obj("music/other.mp3", size=3_000_000, etag="ccc"),
    ])
    upsert_media_metadata(conn, [
        MediaMetadata(
            key="music/song.flac", artist="Artist", title="Song",
            codec="flac", bitrate=1411,
        ),
        MediaMetadata(
            key="music/song.mp3", artist="Artist", title="Song",
            codec="mp3", bitrate=320,
        ),
        MediaMetadata(
            key="music/other.mp3", artist="Artist", title="Other",
            codec="mp3", bitrate=320,
        ),
    ])
    return conn


class TestSameWorkInReport:
    def test_table_includes_same_work(self, db_with_media_groups):
        result = generate_report(db_with_media_groups, fmt="table")
        assert "Même œuvre" in result
        assert "Artist" in result
        assert "Song" in result

    def test_table_shows_codecs(self, db_with_media_groups):
        result = generate_report(db_with_media_groups, fmt="table")
        assert "flac" in result
        assert "mp3" in result

    def test_json_includes_same_work(self, db_with_media_groups):
        result = generate_report(db_with_media_groups, fmt="json")
        data = json.loads(result)
        assert "same_work" in data
        assert len(data["same_work"]) == 1
        group = data["same_work"][0]
        assert group["artist"] == "Artist"
        assert group["title"] == "Song"
        assert len(group["files"]) == 2

    def test_csv_includes_same_work(self, db_with_media_groups):
        result = generate_report(db_with_media_groups, fmt="csv")
        assert "same_work" in result
        assert "Artist - Song" in result

    def test_no_media_no_section(self, db_with_duplicates):
        """Pas de métadonnées média → pas de section."""
        result = generate_report(db_with_duplicates, fmt="json")
        data = json.loads(result)
        assert "same_work" not in data

    def test_no_media_table_no_section(self, db_with_duplicates):
        result = generate_report(db_with_duplicates, fmt="table")
        assert "Même œuvre" not in result

    def test_single_work_not_grouped(self, db_with_media_groups):
        """Un fichier seul pour une œuvre → pas dans le rapport."""
        result = generate_report(db_with_media_groups, fmt="json")
        data = json.loads(result)
        titles = [g["title"] for g in data["same_work"]]
        assert "Other" not in titles
