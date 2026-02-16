"""Tests du script generator — génération bash."""

from datetime import datetime

import pytest

from s3dedup.db import connect, upsert_objects
from s3dedup.models import ObjectInfo
from s3dedup.script_generator import (
    _human_size,
    generate_delete_script,
)


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
        # dir1/song.mp3 (jan) est conservé, dir2/song.mp3 (jun) supprimé
        assert "Conservé    : dir1/song.mp3" in content
        assert "aws s3 rm" in content
        assert "dir2/song.mp3" in content

    def test_keep_newest(self, db_with_duplicates, tmp_path):
        """Garde le plus récent."""
        output = str(tmp_path / "delete.sh")
        content = generate_delete_script(
            db_with_duplicates, "b", keep="newest", output=output,
        )
        assert "Conservé    : dir2/song.mp3" in content

    def test_executable(self, db_with_duplicates, tmp_path):
        """Le script généré est exécutable."""
        import os
        output = str(tmp_path / "delete.sh")
        generate_delete_script(
            db_with_duplicates, "b", output=output,
        )
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
        # L'apostrophe doit être échappée dans le bash
        assert "'\\''" in content


class TestHumanSize:
    def test_bytes(self):
        assert _human_size(500) == "500.0 o"

    def test_kilobytes(self):
        assert _human_size(2048) == "2.0 Ko"

    def test_megabytes(self):
        assert _human_size(5 * 1024 * 1024) == "5.0 Mo"

    def test_gigabytes(self):
        assert _human_size(3 * 1024**3) == "3.0 Go"

    def test_terabytes(self):
        assert _human_size(9 * 1024**4) == "9.0 To"
