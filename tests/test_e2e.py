"""Test end-to-end : scan → report → generate-script."""

import json

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws

from s3dedup.cli import cli

BUCKET = "media-library"


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def s3_with_duplicates():
    """Bucket S3 mock avec des doublons réalistes."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)

        # Doublons : même chanson copiée sous deux noms
        song = b"fake mp3 content" * 500
        s3.put_object(Bucket=BUCKET, Key="music/rock/song.mp3", Body=song)
        s3.put_object(Bucket=BUCKET, Key="music/best-of/song.mp3", Body=song)
        s3.put_object(
            Bucket=BUCKET, Key="music/backup/ song.mp3", Body=song,
        )

        # Doublons : même film
        movie = b"fake mkv content" * 1000
        s3.put_object(Bucket=BUCKET, Key="video/film.mkv", Body=movie)
        s3.put_object(Bucket=BUCKET, Key="video/old/film.mkv", Body=movie)

        # Fichiers uniques
        s3.put_object(Bucket=BUCKET, Key="music/jazz/unique.flac", Body=b"u1")
        s3.put_object(Bucket=BUCKET, Key="video/series/ep01.mkv", Body=b"u2")

        yield s3


class TestEndToEnd:
    def test_full_workflow(self, runner, s3_with_duplicates, tmp_path):
        """Workflow complet : scan → report → generate-script."""
        db_path = str(tmp_path / "e2e.duckdb")
        script_path = str(tmp_path / "delete.sh")

        # 1. Scan
        result = runner.invoke(cli, [
            "scan", "--bucket", BUCKET, "--db", db_path,
        ])
        assert result.exit_code == 0, result.output
        assert "7 objets indexés" in result.output
        assert "groupes de doublons" in result.output

        # 2. Report JSON
        result = runner.invoke(cli, [
            "report", "--format", "json", "--db", db_path,
        ])
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["stats"]["duplicate_groups"] == 2
        # 3 chansons identiques → 2 doublons, 2 films → 1 doublon
        assert data["stats"]["duplicate_objects"] == 3

        # 3. Report CSV
        result = runner.invoke(cli, [
            "report", "--format", "csv", "--db", db_path,
        ])
        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        # Header + 5 objets doublons (3 chansons + 2 films)
        assert len(lines) == 6

        # 4. Generate script
        result = runner.invoke(cli, [
            "generate-script",
            "--bucket", BUCKET,
            "--keep", "oldest",
            "--db", db_path,
            "--output", script_path,
        ])
        assert result.exit_code == 0
        assert "Script généré" in result.output

        with open(script_path) as f:
            script = f.read()
        assert "#!/usr/bin/env bash" in script
        assert f"s3://{BUCKET}/" in script
        assert "set -euo pipefail" in script

    def test_scan_with_prefix(self, runner, s3_with_duplicates, tmp_path):
        """Scan filtré par préfixe."""
        db_path = str(tmp_path / "prefix.duckdb")

        result = runner.invoke(cli, [
            "scan", "--bucket", BUCKET,
            "--prefix", "music/",
            "--db", db_path,
        ])
        assert result.exit_code == 0
        # 4 fichiers sous music/ (3 chansons + 1 unique)
        assert "4 objets indexés" in result.output

    def test_rescan_is_incremental(
        self, runner, s3_with_duplicates, tmp_path,
    ):
        """Un deuxième scan n'indexe que les nouveaux objets."""
        db_path = str(tmp_path / "rescan.duckdb")

        # Premier scan
        runner.invoke(cli, [
            "scan", "--bucket", BUCKET, "--db", db_path,
        ])
        # Deuxième scan : 0 nouveaux
        result = runner.invoke(cli, [
            "scan", "--bucket", BUCKET, "--db", db_path,
        ])
        assert "0 objets indexés" in result.output
