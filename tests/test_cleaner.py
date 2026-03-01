"""Tests du cleaner — nettoyage des clés S3."""

import os
from datetime import datetime

import pytest

from s3dedup.cleaner import (
    StripSpacesRule,
    _resolve_conflicts,
    generate_clean_script,
)
from s3dedup.db import connect, upsert_objects
from s3dedup.models import ObjectInfo


def _obj(key, size=100, etag="e1"):
    return ObjectInfo(
        key=key, size=size, etag=etag,
        is_multipart=False,
        last_modified=datetime(2026, 1, 15),
    )


class TestStripSpacesRule:
    def setup_method(self):
        self.rule = StripSpacesRule()

    def test_leading_space(self):
        assert self.rule.apply(" photo.jpg") == "photo.jpg"

    def test_trailing_space(self):
        assert self.rule.apply("photo.jpg ") == "photo.jpg"

    def test_both_spaces(self):
        assert self.rule.apply(" photo.jpg ") == "photo.jpg"

    def test_multiple_segments(self):
        assert self.rule.apply(" dir / photo.jpg ") == "dir/photo.jpg"

    def test_no_change(self):
        assert self.rule.apply("dir/photo.jpg") is None

    def test_nested_path(self):
        assert self.rule.apply(" a / b / c.txt ") == "a/b/c.txt"

    def test_space_only_segment(self):
        """Un segment composé uniquement d'espaces est supprimé."""
        assert self.rule.apply("dir/ /photo.jpg") == "dir/photo.jpg"


class TestResolveConflicts:
    def test_no_conflict(self):
        renames = {"a": "b"}
        result = _resolve_conflicts(renames, {"a"})
        assert result == {"a": "b"}

    def test_target_exists(self):
        """La cible existe déjà dans les clés existantes (pas renommée)."""
        renames = {" photo.jpg": "photo.jpg"}
        existing = {" photo.jpg", "photo.jpg"}
        result = _resolve_conflicts(renames, existing)
        assert result[" photo.jpg"] == "photo_2.jpg"

    def test_multiple_sources_same_target(self):
        """Deux sources différentes pointent vers la même cible."""
        renames = {" a.txt": "a.txt", "a.txt ": "a.txt"}
        existing = {" a.txt", "a.txt "}
        result = _resolve_conflicts(renames, existing)
        targets = sorted(result.values())
        assert "a.txt" in targets
        assert "a_2.txt" in targets

    def test_preserves_extension(self):
        renames = {" img.png": "img.png"}
        existing = {" img.png", "img.png"}
        result = _resolve_conflicts(renames, existing)
        assert result[" img.png"] == "img_2.png"


class TestGenerateCleanScript:
    def test_generates_script(self, tmp_path):
        conn = connect(":memory:")
        upsert_objects(conn, [_obj(" photo.jpg"), _obj("normal.jpg")])
        output = str(tmp_path / "clean.sh")
        stats = generate_clean_script(
            conn, "my-bucket", output=output,
        )
        content = (tmp_path / "clean.sh").read_text()
        assert "#!/usr/bin/env bash" in content
        assert "aws s3 mv" in content
        assert "s3://my-bucket/ photo.jpg" in content
        assert "s3://my-bucket/photo.jpg" in content
        assert stats.total_keys == 2
        assert stats.rename_count == 1
        assert stats.per_rule == {"strip-spaces": 1}

    def test_no_rename_needed(self, tmp_path):
        conn = connect(":memory:")
        upsert_objects(conn, [_obj("clean.jpg")])
        output = str(tmp_path / "clean.sh")
        stats = generate_clean_script(
            conn, "my-bucket", output=output,
        )
        content = (tmp_path / "clean.sh").read_text()
        assert "Aucun renommage" in content
        assert "aws s3 mv" not in content
        assert stats.total_keys == 1
        assert stats.rename_count == 0
        assert stats.per_rule == {}

    def test_executable(self, tmp_path):
        conn = connect(":memory:")
        upsert_objects(conn, [_obj(" photo.jpg")])
        output = str(tmp_path / "clean.sh")
        generate_clean_script(conn, "b", output=output)
        assert os.access(output, os.X_OK)

    def test_dry_run_support(self, tmp_path):
        conn = connect(":memory:")
        upsert_objects(conn, [_obj(" photo.jpg")])
        output = str(tmp_path / "clean.sh")
        generate_clean_script(conn, "b", output=output)
        content = (tmp_path / "clean.sh").read_text()
        assert "DRY_RUN" in content

    def test_endpoint_url(self, tmp_path):
        conn = connect(":memory:")
        upsert_objects(conn, [_obj(" photo.jpg")])
        output = str(tmp_path / "clean.sh")
        generate_clean_script(
            conn, "b", output=output,
            endpoint_url="http://localhost:9000",
        )
        content = (tmp_path / "clean.sh").read_text()
        assert "http://localhost:9000" in content

    def test_special_chars_escaping(self, tmp_path):
        """Les clés avec apostrophes sont correctement échappées."""
        conn = connect(":memory:")
        upsert_objects(conn, [_obj(" l'album.mp3")])
        output = str(tmp_path / "clean.sh")
        generate_clean_script(conn, "b", output=output)
        content = (tmp_path / "clean.sh").read_text()
        assert "'\\''" in content

    def test_conflict_resolved_with_suffix(self, tmp_path):
        """Quand la cible existe déjà, un suffixe est ajouté."""
        conn = connect(":memory:")
        upsert_objects(conn, [
            _obj(" photo.jpg"),
            _obj("photo.jpg"),
        ])
        output = str(tmp_path / "clean.sh")
        generate_clean_script(conn, "b", output=output)
        content = (tmp_path / "clean.sh").read_text()
        assert "photo_2.jpg" in content
        assert "Conflit résolu" in content

    def test_prefix_filter(self, tmp_path):
        """Seules les clés avec le préfixe sont traitées."""
        conn = connect(":memory:")
        upsert_objects(conn, [
            _obj("dir1/ photo.jpg"),
            _obj("dir2/ other.jpg"),
        ])
        output = str(tmp_path / "clean.sh")
        generate_clean_script(
            conn, "b", prefix="dir1/", output=output,
        )
        content = (tmp_path / "clean.sh").read_text()
        assert "dir1/" in content
        assert "dir2/" not in content

    def test_invalid_rule(self, tmp_path):
        conn = connect(":memory:")
        output = str(tmp_path / "clean.sh")
        with pytest.raises(Exception):
            generate_clean_script(
                conn, "b", rules=["nonexistent"], output=output,
            )
