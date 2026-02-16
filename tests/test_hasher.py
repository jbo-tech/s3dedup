"""Tests du hasher — passe 3 (SHA256 streaming)."""

import hashlib
import io

import boto3
import pytest
from moto import mock_aws

from s3dedup.db import connect, upsert_objects
from s3dedup.hasher import hash_multipart_candidates, hash_object, sha256_stream
from s3dedup.models import ObjectInfo

BUCKET = "test-media"


@pytest.fixture
def db():
    return connect(":memory:")


@pytest.fixture
def s3_env():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3


def _obj(key, size=100, etag="abc-2", multipart=True, sha256=None):
    from datetime import datetime
    return ObjectInfo(
        key=key, size=size, etag=etag,
        is_multipart=multipart, last_modified=datetime(2026, 1, 1),
        sha256=sha256,
    )


class TestSha256Stream:
    def test_known_hash(self):
        data = b"hello world"
        expected = hashlib.sha256(data).hexdigest()
        assert sha256_stream(io.BytesIO(data)) == expected

    def test_empty(self):
        expected = hashlib.sha256(b"").hexdigest()
        assert sha256_stream(io.BytesIO(b"")) == expected

    def test_large_data(self):
        """Vérifie que le streaming fonctionne sur un gros bloc."""
        data = b"x" * (5 * 1024 * 1024)
        expected = hashlib.sha256(data).hexdigest()
        assert sha256_stream(io.BytesIO(data), chunk_size=1024) == expected


class TestHashObject:
    def test_hashes_s3_object(self, s3_env):
        content = b"test content for hashing"
        s3_env.put_object(Bucket=BUCKET, Key="test.bin", Body=content)

        result = hash_object(s3_env, BUCKET, "test.bin")
        assert result == hashlib.sha256(content).hexdigest()


class TestHashMultipartCandidates:
    def test_hashes_candidates(self, s3_env, db):
        content_a = b"same content" * 100
        content_b = b"same content" * 100
        s3_env.put_object(Bucket=BUCKET, Key="a.mkv", Body=content_a)
        s3_env.put_object(Bucket=BUCKET, Key="b.mkv", Body=content_b)

        upsert_objects(db, [
            _obj("a.mkv", size=len(content_a), etag="aaa-2"),
            _obj("b.mkv", size=len(content_b), etag="bbb-3"),
        ])

        count = hash_multipart_candidates(BUCKET, db, s3_client=s3_env)
        assert count == 2

        # Vérifier que les SHA256 sont remplis
        rows = db.execute(
            "SELECT sha256 FROM objects ORDER BY key"
        ).fetchall()
        assert all(r[0] is not None for r in rows)
        # Même contenu → même hash
        assert rows[0][0] == rows[1][0]

    def test_skips_already_hashed(self, s3_env, db):
        s3_env.put_object(Bucket=BUCKET, Key="a.mkv", Body=b"content")
        s3_env.put_object(Bucket=BUCKET, Key="b.mkv", Body=b"content")

        upsert_objects(db, [
            _obj("a.mkv", size=7, sha256="already_done"),
            _obj("b.mkv", size=7, sha256="already_done"),
        ])

        count = hash_multipart_candidates(BUCKET, db, s3_client=s3_env)
        assert count == 0

    def test_no_candidates(self, s3_env, db):
        """Pas d'objets multipart → rien à hasher."""
        upsert_objects(db, [
            _obj("a.mp3", size=100, etag="simple", multipart=False),
            _obj("b.mp3", size=100, etag="simple", multipart=False),
        ])
        count = hash_multipart_candidates(BUCKET, db, s3_client=s3_env)
        assert count == 0

    def test_different_content_different_hash(self, s3_env, db):
        """Même taille mais contenu différent → SHA256 différent."""
        s3_env.put_object(Bucket=BUCKET, Key="a.bin", Body=b"aaaa")
        s3_env.put_object(Bucket=BUCKET, Key="b.bin", Body=b"bbbb")

        upsert_objects(db, [
            _obj("a.bin", size=4, etag="x-2"),
            _obj("b.bin", size=4, etag="y-3"),
        ])

        hash_multipart_candidates(BUCKET, db, s3_client=s3_env)

        rows = db.execute(
            "SELECT sha256 FROM objects ORDER BY key"
        ).fetchall()
        assert rows[0][0] != rows[1][0]
