"""Tests du module diagnose — détection de dossiers en doublon."""

from datetime import datetime

import pytest

from s3dedup.db import connect, upsert_objects
from s3dedup.diagnose import (
    Category,
    FolderInfo,
    _classify,
    _group_by_base_name,
    find_duplicate_folders,
    format_report,
    generate_orphan_script,
)
from s3dedup.models import ObjectInfo


def _obj(key, size=100):
    return ObjectInfo(
        key=key, size=size, etag="abc",
        is_multipart=False,
        last_modified=datetime(2026, 1, 15),
    )


@pytest.fixture
def conn(tmp_path):
    db_path = str(tmp_path / "test.duckdb")
    c = connect(db_path)
    yield c
    c.close()


class TestGroupByBaseName:
    def test_groups_with_and_without_id(self):
        folders = [
            "Music/Artist/Album",
            "Music/Artist/Album [123] [2020]",
        ]
        groups = _group_by_base_name(folders, depth=3)
        assert "Music/Artist/Album" in groups
        assert len(groups["Music/Artist/Album"]) == 2

    def test_no_grouping_for_different_names(self):
        folders = [
            "Music/Artist/Album One",
            "Music/Artist/Album Two [123] [2020]",
        ]
        groups = _group_by_base_name(folders, depth=3)
        assert len(groups) == 2

    def test_id_only_no_year(self):
        folders = [
            "Music/Artist/Album",
            "Music/Artist/Album [999]",
        ]
        groups = _group_by_base_name(folders, depth=3)
        assert len(groups["Music/Artist/Album"]) == 2

    def test_two_different_ids_same_base(self):
        folders = [
            "Music/Artist/Album [111] [2020]",
            "Music/Artist/Album [222] [2021]",
        ]
        groups = _group_by_base_name(folders, depth=3)
        assert len(groups["Music/Artist/Album"]) == 2


class TestClassify:
    def test_orphan_when_one_has_no_music(self):
        folders = [
            FolderInfo(path="a", file_count=2, media_count=0, image_count=2),
            FolderInfo(path="b", file_count=10, media_count=9, image_count=1),
        ]
        assert _classify(folders) == Category.ORPHAN

    def test_both_music_when_all_have_audio(self):
        folders = [
            FolderInfo(path="a", file_count=10, media_count=8),
            FolderInfo(path="b", file_count=12, media_count=11),
        ]
        assert _classify(folders) == Category.BOTH_MUSIC


class TestFindDuplicateFolders:
    def test_detects_orphan(self, conn):
        objects = [
            _obj("Music/Artist/Album/cover.jpg", size=50000),
            _obj("Music/Artist/Album/folder.jpg", size=50000),
            _obj("Music/Artist/Album [123] [2020]/01 - Track.flac", size=30000000),
            _obj("Music/Artist/Album [123] [2020]/02 - Track.flac", size=28000000),
            _obj("Music/Artist/Album [123] [2020]/cover.jpg", size=400000),
        ]
        upsert_objects(conn, objects)

        result = find_duplicate_folders(conn, prefix="Music/", depth=3)
        assert len(result.groups) == 1
        group = result.groups[0]
        assert group.category == Category.ORPHAN
        assert group.orphan.path == "Music/Artist/Album"
        assert group.complete.path == "Music/Artist/Album [123] [2020]"

    def test_detects_both_music(self, conn):
        objects = [
            _obj("Music/Artist/Album/01 - Track.mp3", size=5000000),
            _obj("Music/Artist/Album/02 - Track.mp3", size=4000000),
            _obj("Music/Artist/Album [456] [2019]/01 - Track.flac", size=30000000),
            _obj("Music/Artist/Album [456] [2019]/02 - Track.flac", size=28000000),
        ]
        upsert_objects(conn, objects)

        result = find_duplicate_folders(conn, prefix="Music/", depth=3)
        assert len(result.groups) == 1
        assert result.groups[0].category == Category.BOTH_MUSIC

    def test_no_duplicates(self, conn):
        objects = [
            _obj("Music/Artist/Album One/01 - Track.flac"),
            _obj("Music/Artist/Album Two [789] [2021]/01 - Track.flac"),
        ]
        upsert_objects(conn, objects)

        result = find_duplicate_folders(conn, prefix="Music/", depth=3)
        assert len(result.groups) == 0

    def test_prefix_filter(self, conn):
        objects = [
            _obj("Music/Artist/Album/cover.jpg"),
            _obj("Music/Artist/Album [1] [2020]/track.flac", size=5000000),
            _obj("Other/Artist/Album/cover.jpg"),
            _obj("Other/Artist/Album [2] [2020]/track.flac", size=5000000),
        ]
        upsert_objects(conn, objects)

        result = find_duplicate_folders(conn, prefix="Music/", depth=3)
        assert len(result.groups) == 1
        assert result.groups[0].base_name == "Music/Artist/Album"


class TestFormatReport:
    def test_table_format(self, conn):
        objects = [
            _obj("Music/Artist/Album/cover.jpg", size=50000),
            _obj("Music/Artist/Album [1] [2020]/track.flac", size=30000000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        output = format_report(result, fmt="table")
        assert "Catégorie A" in output
        assert "Music/Artist/Album/" in output

    def test_json_format(self, conn):
        import json

        objects = [
            _obj("Music/Artist/Album/cover.jpg", size=50000),
            _obj("Music/Artist/Album [1] [2020]/track.flac", size=30000000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        output = format_report(result, fmt="json")
        data = json.loads(output)
        assert data["duplicate_groups"] == 1
        assert data["groups"][0]["category"] == "orphan"

    def test_csv_format(self, conn):
        objects = [
            _obj("Music/Artist/Album/cover.jpg", size=50000),
            _obj("Music/Artist/Album [1] [2020]/track.flac", size=30000000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        output = format_report(result, fmt="csv")
        lines = output.strip().split("\n")
        assert lines[0].startswith("category,")
        assert len(lines) == 3  # header + 2 folders


class TestGenerateOrphanScript:
    def test_generates_script_for_orphans(self, conn, tmp_path):
        objects = [
            _obj("Music/Artist/Album/cover.jpg", size=50000),
            _obj("Music/Artist/Album/folder.jpg", size=40000),
            _obj("Music/Artist/Album [1] [2020]/01 - Track.flac", size=30000000),
            _obj("Music/Artist/Album [1] [2020]/cover.jpg", size=400000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        script_path = str(tmp_path / "delete.sh")
        content = generate_orphan_script(
            result, conn, "my-bucket", output=script_path,
        )

        assert "aws s3 rm" in content
        assert "Music/Artist/Album/cover.jpg" in content
        assert "Music/Artist/Album/folder.jpg" in content
        rm_lines = [x for x in content.splitlines() if x.startswith("aws s3 rm")]
        assert len(rm_lines) == 2
        for line in rm_lines:
            assert "Album [1] [2020]" not in line
        assert "my-bucket" in content
        assert "set -euo pipefail" in content

    def test_script_with_endpoint_url(self, conn, tmp_path):
        objects = [
            _obj("Music/Artist/Album/cover.jpg", size=50000),
            _obj("Music/Artist/Album [1] [2020]/track.flac", size=30000000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        script_path = str(tmp_path / "delete.sh")
        content = generate_orphan_script(
            result, conn, "my-bucket",
            output=script_path,
            endpoint_url="https://s3.example.com",
        )

        assert "https://s3.example.com" in content

    def test_no_orphans_produces_empty_script(self, conn, tmp_path):
        objects = [
            _obj("Music/Artist/Album/01 - Track.mp3", size=5000000),
            _obj("Music/Artist/Album [1] [2020]/01 - Track.flac", size=30000000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        script_path = str(tmp_path / "delete.sh")
        content = generate_orphan_script(
            result, conn, "my-bucket", output=script_path,
        )

        assert "aws s3 rm" not in content
        assert "Aucun dossier orphelin" in content

    def test_script_is_executable(self, conn, tmp_path):
        import os
        import stat

        objects = [
            _obj("Music/Artist/Album/cover.jpg", size=50000),
            _obj("Music/Artist/Album [1] [2020]/track.flac", size=30000000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        script_path = str(tmp_path / "delete.sh")
        generate_orphan_script(
            result, conn, "my-bucket", output=script_path,
        )

        mode = os.stat(script_path).st_mode
        assert mode & stat.S_IXUSR

    def test_escapes_single_quotes_in_keys(self, conn, tmp_path):
        objects = [
            _obj("Music/Artist/It's Album/cover.jpg", size=50000),
            _obj("Music/Artist/It's Album [1] [2020]/track.flac", size=30000000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        script_path = str(tmp_path / "delete.sh")
        content = generate_orphan_script(
            result, conn, "my-bucket", output=script_path,
        )

        assert "It'\\''s Album/cover.jpg" in content

    def test_dryrun_support(self, conn, tmp_path):
        objects = [
            _obj("Music/Artist/Album/cover.jpg", size=50000),
            _obj("Music/Artist/Album [1] [2020]/track.flac", size=30000000),
        ]
        upsert_objects(conn, objects)
        result = find_duplicate_folders(conn, prefix="Music/", depth=3)

        script_path = str(tmp_path / "delete.sh")
        content = generate_orphan_script(
            result, conn, "my-bucket", output=script_path,
        )

        assert "${DRY_RUN:-}" in content
        assert "--dryrun" in content
