"""Structures de données partagées pour s3dedup."""

from dataclasses import dataclass
from datetime import datetime

# Extensions de fichiers média reconnus
MEDIA_EXTENSIONS_AUDIO = frozenset({
    ".mp3", ".flac", ".ogg", ".m4a", ".aac", ".wma", ".opus", ".wav",
})
MEDIA_EXTENSIONS_VIDEO = frozenset({
    ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".webm",
})
MEDIA_EXTENSIONS = MEDIA_EXTENSIONS_AUDIO | MEDIA_EXTENSIONS_VIDEO


@dataclass
class ObjectInfo:
    """Représente un objet S3 indexé."""

    key: str
    size: int
    etag: str
    is_multipart: bool
    last_modified: datetime
    sha256: str | None = None


@dataclass
class DuplicateGroup:
    """Groupe d'objets identiques (doublons)."""

    fingerprint: str
    size: int
    objects: list[ObjectInfo]

    @property
    def wasted_bytes(self) -> int:
        """Espace gaspillé (tout sauf un exemplaire)."""
        return self.size * (len(self.objects) - 1)


@dataclass
class MediaMetadata:
    """Métadonnées extraites d'un fichier média."""

    key: str
    artist: str | None = None
    album: str | None = None
    title: str | None = None
    duration_s: float | None = None
    codec: str | None = None
    bitrate: int | None = None


@dataclass
class ScanResult:
    """Résultat d'un scan incrémental."""

    new: int
    updated: int
    deleted: int


@dataclass
class ScanStats:
    """Statistiques d'un scan."""

    total_objects: int
    total_size: int
    duplicate_groups: int
    duplicate_objects: int
    wasted_bytes: int
