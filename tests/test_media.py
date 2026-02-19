"""Tests du module media — extraction de métadonnées."""

import tempfile

import boto3
import pytest
from moto import mock_aws
from mutagen.id3 import TALB, TIT2, TPE1
from mutagen.mp3 import MP3

from s3dedup.db import connect
from s3dedup.media import extract_metadata, is_media_file
from s3dedup.scanner import extract_all_media_metadata

BUCKET = "test-media"


def _make_mp3(artist=None, title=None, album=None) -> bytes:
    """Crée un fichier MP3 minimal avec des tags ID3."""
    # Frame header MPEG1 Layer3 128kbps 44100Hz stereo
    frame_header = bytes([0xFF, 0xFB, 0x90, 0x00])
    frame = frame_header + b"\x00" * 413
    data = frame * 10

    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as f:
        f.write(data)
        path = f.name

    audio = MP3(path)
    if title:
        audio["TIT2"] = TIT2(encoding=3, text=[title])
    if artist:
        audio["TPE1"] = TPE1(encoding=3, text=[artist])
    if album:
        audio["TALB"] = TALB(encoding=3, text=[album])
    audio.save()

    with open(path, "rb") as f:
        return f.read()


@pytest.fixture
def mp3_with_tags():
    """Contenu binaire d'un MP3 avec tags complets."""
    return _make_mp3(
        artist="Test Artist",
        title="Test Song",
        album="Test Album",
    )


@pytest.fixture
def mp3_no_tags():
    """Contenu binaire d'un MP3 sans tags."""
    return _make_mp3()


@pytest.fixture
def s3_bucket():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        yield s3


@pytest.fixture
def db():
    return connect(":memory:")


class TestIsMediaFile:
    def test_mp3(self):
        assert is_media_file("music/song.mp3") is True

    def test_flac(self):
        assert is_media_file("song.flac") is True

    def test_mp4(self):
        assert is_media_file("video.mp4") is True

    def test_mkv(self):
        assert is_media_file("film.mkv") is True

    def test_case_insensitive(self):
        assert is_media_file("Song.MP3") is True
        assert is_media_file("video.MKV") is True

    def test_non_media(self):
        assert is_media_file("doc.pdf") is False
        assert is_media_file("photo.jpg") is False
        assert is_media_file("readme.txt") is False

    def test_no_extension(self):
        assert is_media_file("README") is False


class TestExtractMetadata:
    def test_extracts_tags(self, s3_bucket, mp3_with_tags):
        s3_bucket.put_object(
            Bucket=BUCKET, Key="song.mp3", Body=mp3_with_tags,
        )
        meta = extract_metadata(s3_bucket, BUCKET, "song.mp3")
        assert meta is not None
        assert meta.key == "song.mp3"
        assert meta.artist == "Test Artist"
        assert meta.title == "Test Song"
        assert meta.album == "Test Album"

    def test_no_tags_returns_empty_metadata(
        self, s3_bucket, mp3_no_tags,
    ):
        s3_bucket.put_object(
            Bucket=BUCKET, Key="notag.mp3", Body=mp3_no_tags,
        )
        meta = extract_metadata(s3_bucket, BUCKET, "notag.mp3")
        assert meta is not None
        assert meta.key == "notag.mp3"
        assert meta.artist is None
        assert meta.title is None

    def test_non_media_content(self, s3_bucket):
        """Un fichier texte avec extension .mp3 → métadonnées vides."""
        s3_bucket.put_object(
            Bucket=BUCKET, Key="fake.mp3", Body=b"not audio",
        )
        meta = extract_metadata(s3_bucket, BUCKET, "fake.mp3")
        assert meta is not None
        assert meta.artist is None

    def test_codec_detected(self, s3_bucket, mp3_with_tags):
        s3_bucket.put_object(
            Bucket=BUCKET, Key="song.mp3", Body=mp3_with_tags,
        )
        meta = extract_metadata(s3_bucket, BUCKET, "song.mp3")
        assert meta.codec is not None
        assert "mp3" in meta.codec.lower()

    def test_duration_detected(self, s3_bucket, mp3_with_tags):
        s3_bucket.put_object(
            Bucket=BUCKET, Key="song.mp3", Body=mp3_with_tags,
        )
        meta = extract_metadata(s3_bucket, BUCKET, "song.mp3")
        assert meta.duration_s is not None
        assert meta.duration_s > 0


class TestExtractAllMediaMetadata:
    def test_extracts_for_media_files(
        self, s3_bucket, db, mp3_with_tags,
    ):
        """Extrait les métadonnées des fichiers média indexés."""
        from s3dedup.scanner import scan_bucket

        s3_bucket.put_object(
            Bucket=BUCKET, Key="song.mp3", Body=mp3_with_tags,
        )
        s3_bucket.put_object(
            Bucket=BUCKET, Key="doc.txt", Body=b"hello",
        )
        scan_bucket(BUCKET, db, s3_client=s3_bucket)

        count = extract_all_media_metadata(
            BUCKET, db, s3_client=s3_bucket,
        )
        assert count == 1  # Seul le .mp3

        rows = db.execute(
            "SELECT artist, title FROM media_metadata"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0] == ("Test Artist", "Test Song")

    def test_skips_already_enriched(
        self, s3_bucket, db, mp3_with_tags,
    ):
        """Ne ré-extrait pas les fichiers déjà dans media_metadata."""
        from s3dedup.scanner import scan_bucket

        s3_bucket.put_object(
            Bucket=BUCKET, Key="song.mp3", Body=mp3_with_tags,
        )
        scan_bucket(BUCKET, db, s3_client=s3_bucket)

        # Premier passage
        extract_all_media_metadata(
            BUCKET, db, s3_client=s3_bucket,
        )
        # Deuxième passage : rien à faire
        count = extract_all_media_metadata(
            BUCKET, db, s3_client=s3_bucket,
        )
        assert count == 0

    def test_no_media_files(self, s3_bucket, db):
        """Pas de fichiers média → retourne 0."""
        from s3dedup.scanner import scan_bucket

        s3_bucket.put_object(
            Bucket=BUCKET, Key="doc.txt", Body=b"hello",
        )
        scan_bucket(BUCKET, db, s3_client=s3_bucket)

        count = extract_all_media_metadata(
            BUCKET, db, s3_client=s3_bucket,
        )
        assert count == 0

    def test_empty_db(self, db):
        s3 = boto3.client("s3", region_name="us-east-1")
        count = extract_all_media_metadata(
            BUCKET, db, s3_client=s3,
        )
        assert count == 0
