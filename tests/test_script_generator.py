"""Tests du script generator — génération bash."""

from datetime import datetime

import pytest

from s3dedup.db import connect, upsert_objects
from s3dedup.models import ObjectInfo
from s3dedup.script_generator import (
    generate_delete_script,
    parse_keep,
)
from s3dedup.utils import human_size


def _obj(key, size=100, etag="e1", multipart=False, last_modified=None):
    return ObjectInfo(
        key=key, size=size, etag=etag,
        is_multipart=multipart,
        last_modified=last_modified or datetime(2026, 1, 15),
    )


@pytest.fixture
def db_with_duplicates():
    conn = connect(":memory:")
    upsert_objects(conn, [
        _obj("dir1/song.mp3", size=1000, etag="aaa",
             last_modified=datetime(2026, 1, 1)),
        _obj("dir2/song.mp3", size=1000, etag="aaa",
             last_modified=datetime(2026, 6, 1)),
        _obj("unique.flac", size=5000, etag="bbb"),
    ])
    return conn


@pytest.fixture
def db_with_conflict_copies():
    """Doublons typiques de copies de conflit (_1, _2, _3)."""
    conn = connect(":memory:")
    upsert_objects(conn, [
        _obj("Photo/image.jpg", size=500, etag="aaa",
             last_modified=datetime(2026, 1, 1)),
        _obj("Photo/image_1.jpg", size=500, etag="aaa",
             last_modified=datetime(2026, 1, 2)),
        _obj("Photo/image_2.jpg", size=500, etag="aaa",
             last_modified=datetime(2026, 1, 3)),
    ])
    return conn


class TestParseKeep:
    def test_single_criterion(self):
        assert parse_keep("oldest") == ["oldest"]

    def test_multiple_criteria(self):
        assert parse_keep("shortest,oldest") == ["shortest", "oldest"]

    def test_strips_spaces(self):
        assert parse_keep("shortest, oldest") == ["shortest", "oldest"]

    def test_invalid_criterion(self):
        with pytest.raises(Exception):
            parse_keep("largest")

    def test_mixed_valid_invalid(self):
        with pytest.raises(Exception):
            parse_keep("shortest,invalid")


class TestGenerateDeleteScript:
    def test_generates_script(self, db_with_duplicates, tmp_path):
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            db_with_duplicates, "my-bucket", keep="oldest", output=output,
        )
        assert "#!/usr/bin/env bash" in content
        assert "my-bucket" in content

    def test_keep_oldest(self, db_with_duplicates, tmp_path):
        """Garde le plus ancien, supprime le plus récent."""
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            db_with_duplicates, "b", keep="oldest", output=output,
        )
        assert "Conservé    : dir1/song.mp3" in content
        assert "dir2/song.mp3" in content

    def test_keep_newest(self, db_with_duplicates, tmp_path):
        """Garde le plus récent."""
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            db_with_duplicates, "b", keep="newest", output=output,
        )
        assert "Conservé    : dir2/song.mp3" in content

    def test_keep_shortest(self, db_with_conflict_copies, tmp_path):
        """Garde le nom le plus court (l'original sans suffixe)."""
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            db_with_conflict_copies, "b", keep="shortest", output=output,
        )
        assert "Conservé    : Photo/image.jpg" in content
        assert "image_1.jpg" in content
        assert "image_2.jpg" in content

    def test_keep_shortest_oldest(self, db_with_conflict_copies, tmp_path):
        """Tri composite : shortest d'abord, oldest en cas d'égalité."""
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            db_with_conflict_copies, "b",
            keep="shortest,oldest", output=output,
        )
        assert "Conservé    : Photo/image.jpg" in content

    def test_keep_shortest_newest_tiebreak(self, tmp_path):
        """En cas d'égalité de longueur, newest départage."""
        conn = connect(":memory:")
        upsert_objects(conn, [
            _obj("dir/a.mp3", size=100, etag="aaa",
                 last_modified=datetime(2026, 1, 1)),
            _obj("dir/b.mp3", size=100, etag="aaa",
                 last_modified=datetime(2026, 6, 1)),
        ])
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            conn, "b", keep="shortest,newest", output=output,
        )
        # Même longueur → newest gagne → b.mp3 conservé
        assert "Conservé    : dir/b.mp3" in content

    def test_default_keep(self, db_with_conflict_copies, tmp_path):
        """Le défaut (shortest,oldest) garde l'original."""
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            db_with_conflict_copies, "b", output=output,
        )
        assert "Conservé    : Photo/image.jpg" in content

    def test_executable(self, db_with_duplicates, tmp_path):
        import os
        output = str(tmp_path / "delete.sh")
        generate_delete_script(db_with_duplicates, "b", output=output)
        assert os.access(output, os.X_OK)

    def test_dry_run_comment(self, db_with_duplicates, tmp_path):
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            db_with_duplicates, "b", output=output,
        )
        assert "DRY_RUN" in content

    def test_empty_db(self, tmp_path):
        conn = connect(":memory:")
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(conn, "b", output=output)
        assert "Aucun doublon" in content

    def test_special_chars_in_key(self, tmp_path):
        """Les clés avec apostrophes sont correctement échappées."""
        conn = connect(":memory:")
        upsert_objects(conn, [
            _obj("l'album/track.mp3", size=100, etag="aaa"),
            _obj("copie/l'album/track.mp3", size=100, etag="aaa"),
        ])
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(conn, "b", output=output)
        assert "'\\''" in content


class TestHumanSize:
    def test_bytes(self):
        assert human_size(500) == "500.0 o"

    def test_kilobytes(self):
        assert human_size(2048) == "2.0 Ko"

    def test_megabytes(self):
        assert human_size(5 * 1024 * 1024) == "5.0 Mo"

    def test_gigabytes(self):
        assert human_size(3 * 1024**3) == "3.0 Go"

    def test_terabytes(self):
        assert human_size(9 * 1024**4) == "9.0 To"
