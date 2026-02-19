"""Extraction de métadonnées média depuis S3 via mutagen."""

import posixpath
import tempfile

import mutagen

from s3dedup.models import MEDIA_EXTENSIONS, MediaMetadata

# Taille maximale du range GET pour extraire les tags (256 Ko).
RANGE_BYTES = 256 * 1024


def is_media_file(key: str) -> bool:
    """Vérifie si la clé S3 correspond à un fichier média reconnu."""
    ext = posixpath.splitext(key)[1].lower()
    return ext in MEDIA_EXTENSIONS


def extract_metadata(
    s3_client,
    bucket: str,
    key: str,
) -> MediaMetadata | None:
    """Extrait les métadonnées d'un fichier média via range GET + mutagen.

    Télécharge les premiers RANGE_BYTES du fichier dans un fichier
    temporaire, puis tente de lire les tags avec mutagen.
    Retourne None si le fichier n'est pas lisible par mutagen.
    """
    ext = posixpath.splitext(key)[1].lower()
    data = _download_range(s3_client, bucket, key)
    if data is None:
        return MediaMetadata(key=key)

    return _parse_tags(key, ext, data)


def _download_range(
    s3_client,
    bucket: str,
    key: str,
) -> bytes | None:
    """Télécharge les premiers octets d'un objet S3 via Range header."""
    try:
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key,
            Range=f"bytes=0-{RANGE_BYTES - 1}",
        )
        return response["Body"].read()
    except Exception:
        return None


def _parse_tags(
    key: str,
    ext: str,
    data: bytes,
) -> MediaMetadata:
    """Parse les tags média depuis les données brutes."""
    suffix = ext if ext else ".bin"
    try:
        with tempfile.NamedTemporaryFile(suffix=suffix) as tmp:
            tmp.write(data)
            tmp.flush()
            tags = mutagen.File(tmp.name, easy=True)
    except Exception:
        return MediaMetadata(key=key)

    if tags is None:
        return MediaMetadata(key=key)

    artist = _first_tag(tags, "artist", "albumartist")
    album = _first_tag(tags, "album")
    title = _first_tag(tags, "title")

    # Durée et infos techniques
    duration_s = None
    codec = None
    bitrate = None
    info = getattr(tags, "info", None)
    if info:
        duration_s = getattr(info, "length", None)
        bitrate = getattr(info, "bitrate", None)
        codec = type(tags).__name__.lower()

    return MediaMetadata(
        key=key,
        artist=artist,
        album=album,
        title=title,
        duration_s=duration_s,
        codec=codec,
        bitrate=bitrate,
    )


def _first_tag(tags, *names: str) -> str | None:
    """Retourne la première valeur trouvée parmi les noms de tag."""
    for name in names:
        val = tags.get(name)
        if val:
            # mutagen retourne des listes pour certains tags
            return val[0] if isinstance(val, list) else str(val)
    return None
