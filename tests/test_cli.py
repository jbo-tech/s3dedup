"""Tests des commandes CLI."""

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_aws

from s3dedup.cli import cli
from s3dedup.db import connect, upsert_objects
from s3dedup.models import ObjectInfo

BUCKET = "test-media"


@pytest.fixture
def runner():
    return CliRunner()


class TestScanCommand:
    def test_help(self, runner):
        result = runner.invoke(cli, ["scan", "--help"])
        assert result.exit_code == 0
        assert "--bucket" in result.output
        assert "--extract-metadata" in result.output

    def test_scan_bucket(self, runner, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=BUCKET)
            s3.put_object(Bucket=BUCKET, Key="a.mp3", Body=b"x" * 100)
            s3.put_object(Bucket=BUCKET, Key="b.mp3", Body=b"x" * 100)

            result = runner.invoke(cli, [
                "scan", "--bucket", BUCKET, "--db", db_path,
            ])

        assert result.exit_code == 0
        assert "2 nouveaux" in result.output


class TestReportCommand:
    def test_help(self, runner):
        result = runner.invoke(cli, ["report", "--help"])
        assert result.exit_code == 0
        assert "--format" in result.output

    def test_json_report(self, runner, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        conn = connect(db_path)
        from datetime import datetime
        now = datetime.now()
        upsert_objects(conn, [
            ObjectInfo("a.mp3", 100, "e1", False, now),
            ObjectInfo("b.mp3", 100, "e1", False, now),
        ])
        conn.close()

        result = runner.invoke(cli, [
            "report", "--format", "json", "--db", db_path,
        ])
        assert result.exit_code == 0
        assert '"stats"' in result.output

    def test_csv_report(self, runner, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        conn = connect(db_path)
        conn.close()

        result = runner.invoke(cli, [
            "report", "--format", "csv", "--db", db_path,
        ])
        assert result.exit_code == 0
        assert "group_id" in result.output


class TestGenerateScriptCommand:
    def test_help(self, runner):
        result = runner.invoke(cli, ["generate-script", "--help"])
        assert result.exit_code == 0
        assert "--keep" in result.output
        assert "cleanest" in result.output

    def test_generates_file(self, runner, tmp_path):
        db_path = str(tmp_path / "test.duckdb")
        output = str(tmp_path / "delete.sh")
        conn = connect(db_path)
        from datetime import datetime
        now = datetime.now()
        upsert_objects(conn, [
            ObjectInfo("a.mp3", 100, "e1", False, now),
            ObjectInfo("b.mp3", 100, "e1", False, now),
        ])
        conn.close()

        result = runner.invoke(cli, [
            "generate-script",
            "--bucket", BUCKET,
            "--db", db_path,
            "--output", output,
        ])
        assert result.exit_code == 0
        assert "Script généré" in result.output

        with open(output) as f:
            content = f.read()
        assert "#!/usr/bin/env bash" in content


class TestVersionOption:
    def test_version(self, runner):
        result = runner.invoke(cli, ["--version"])
        assert "0.1.0" in result.output
