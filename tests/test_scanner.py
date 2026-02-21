"""Tests du scanner S3."""

import boto3
import pytest
from moto import mock_aws

from s3dedup.db import connect
from s3dedup.scanner import is_multipart_etag, scan_bucket

BUCKET = "test-media"


@pytest.fixture
def s3_bucket():
    """Crée un bucket S3 mock avec des objets de test."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3


@pytest.fixture
def db():
    """Connexion DuckDB en mémoire."""
    return connect(":memory:")


class TestIsMultipartEtag:
    def test_simple_etag(self):
        assert is_multipart_etag('"d41d8cd98f00b204e9800998ecf8427e"') is False

    def test_multipart_etag(self):
        assert is_multipart_etag('"d41d8cd98f00b204e9800998ecf8427e-3"') is True

    def test_no_quotes(self):
        assert is_multipart_etag("abc123-5") is True

    def test_dash_not_number(self):
        assert is_multipart_etag('"my-file-name"') is False


class TestScanBucket:
    def test_indexes_objects(self, s3_bucket, db):
        s3_bucket.put_object(Bucket=BUCKET, Key="a.mp3", Body=b"x" * 100)
        s3_bucket.put_object(Bucket=BUCKET, Key="b.flac", Body=b"y" * 200)

        result = scan_bucket(BUCKET, db, s3_client=s3_bucket)
        assert result.new == 2
        assert result.updated == 0
        assert result.deleted == 0

        rows = db.execute("SELECT count(*) FROM objects").fetchone()
        assert rows[0] == 2

    def test_with_prefix(self, s3_bucket, db):
        s3_bucket.put_object(Bucket=BUCKET, Key="music/a.mp3", Body=b"x")
        s3_bucket.put_object(Bucket=BUCKET, Key="video/b.mkv", Body=b"y")

        result = scan_bucket(BUCKET, db, prefix="music/", s3_client=s3_bucket)
        assert result.new == 1

    def test_skips_unchanged_keys(self, s3_bucket, db):
        """La reprise ne ré-indexe pas les objets inchangés."""
        s3_bucket.put_object(Bucket=BUCKET, Key="a.mp3", Body=b"x" * 100)
        s3_bucket.put_object(Bucket=BUCKET, Key="b.mp3", Body=b"y" * 100)

        # Premier scan
        scan_bucket(BUCKET, db, s3_client=s3_bucket)
        # Ajout d'un nouvel objet
        s3_bucket.put_object(Bucket=BUCKET, Key="c.mp3", Body=b"z" * 100)
        # Deuxième scan : seul le nouveau est indexé
        result = scan_bucket(BUCKET, db, s3_client=s3_bucket)
        assert result.new == 1
        assert result.updated == 0

        total = db.execute("SELECT count(*) FROM objects").fetchone()
        assert total[0] == 3

    def test_detects_modified_objects(self, s3_bucket, db):
        """Un objet modifié sur S3 (ETag changé) est ré-indexé."""
        s3_bucket.put_object(Bucket=BUCKET, Key="a.mp3", Body=b"v1" * 100)
        scan_bucket(BUCKET, db, s3_client=s3_bucket)

        # Modifier l'objet (contenu différent → ETag différent)
        s3_bucket.put_object(Bucket=BUCKET, Key="a.mp3", Body=b"v2" * 100)
        result = scan_bucket(BUCKET, db, s3_client=s3_bucket)
        assert result.new == 0
        assert result.updated == 1
        assert result.deleted == 0

        # L'ETag en base doit refléter la nouvelle version
        row = db.execute(
            "SELECT etag FROM objects WHERE key = 'a.mp3'"
        ).fetchone()
        s3_obj = s3_bucket.head_object(Bucket=BUCKET, Key="a.mp3")
        assert row[0] == s3_obj["ETag"]

    def test_detects_deleted_objects(self, s3_bucket, db):
        """Un objet supprimé de S3 est retiré de la base."""
        s3_bucket.put_object(Bucket=BUCKET, Key="a.mp3", Body=b"x" * 100)
        s3_bucket.put_object(Bucket=BUCKET, Key="b.mp3", Body=b"y" * 100)
        scan_bucket(BUCKET, db, s3_client=s3_bucket)

        # Supprimer un objet sur S3
        s3_bucket.delete_object(Bucket=BUCKET, Key="b.mp3")
        result = scan_bucket(BUCKET, db, s3_client=s3_bucket)
        assert result.new == 0
        assert result.updated == 0
        assert result.deleted == 1

        total = db.execute("SELECT count(*) FROM objects").fetchone()
        assert total[0] == 1

    def test_deletion_scoped_to_prefix(self, s3_bucket, db):
        """Les suppressions ne touchent que les clés du préfixe scanné."""
        s3_bucket.put_object(Bucket=BUCKET, Key="music/a.mp3", Body=b"x")
        s3_bucket.put_object(Bucket=BUCKET, Key="video/b.mkv", Body=b"y")
        # Scanner tout
        scan_bucket(BUCKET, db, s3_client=s3_bucket)

        # Supprimer music/a.mp3 sur S3
        s3_bucket.delete_object(Bucket=BUCKET, Key="music/a.mp3")
        # Scanner uniquement music/ — ne doit pas toucher video/
        result = scan_bucket(BUCKET, db, prefix="music/", s3_client=s3_bucket)
        assert result.deleted == 1

        total = db.execute("SELECT count(*) FROM objects").fetchone()
        assert total[0] == 1  # video/b.mkv reste

    def test_skips_zero_byte_objects(self, s3_bucket, db):
        """Les marqueurs de dossier S3 (0 octets) sont ignorés."""
        s3_bucket.put_object(Bucket=BUCKET, Key="Music/", Body=b"")
        s3_bucket.put_object(Bucket=BUCKET, Key="Music/rock/", Body=b"")
        s3_bucket.put_object(Bucket=BUCKET, Key="Music/song.mp3", Body=b"x" * 100)

        result = scan_bucket(BUCKET, db, s3_client=s3_bucket)
        assert result.new == 1

        rows = db.execute("SELECT key FROM objects").fetchall()
        assert rows[0][0] == "Music/song.mp3"

    def test_empty_bucket(self, s3_bucket, db):
        result = scan_bucket(BUCKET, db, s3_client=s3_bucket)
        assert result.new == 0
        assert result.updated == 0
        assert result.deleted == 0

    def test_detects_duplicates_by_size(self, s3_bucket, db):
        """Des fichiers de même contenu ont la même taille et le même ETag."""
        content = b"duplicate content here" * 50
        s3_bucket.put_object(Bucket=BUCKET, Key="dir1/song.mp3", Body=content)
        s3_bucket.put_object(Bucket=BUCKET, Key="dir2/song.mp3", Body=content)
        s3_bucket.put_object(Bucket=BUCKET, Key="unique.flac", Body=b"other")

        scan_bucket(BUCKET, db, s3_client=s3_bucket)

        # Vérification via requête directe
        dups = db.execute(
            "SELECT size, count(*) as cnt FROM objects "
            "GROUP BY size HAVING cnt > 1"
        ).fetchall()
        assert len(dups) == 1
        assert dups[0][1] == 2

    def test_many_objects_batched(self, s3_bucket, db):
        """Vérifie que le batching fonctionne avec plus de BATCH_SIZE objets."""
        for i in range(50):
            s3_bucket.put_object(
                Bucket=BUCKET, Key=f"file_{i:04d}.txt", Body=f"content_{i}".encode()
            )

        result = scan_bucket(BUCKET, db, s3_client=s3_bucket)
        assert result.new == 50
