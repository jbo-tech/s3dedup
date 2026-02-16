"""Structures de données partagées pour s3dedup."""

from dataclasses import dataclass
from datetime import datetime


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
class ScanStats:
    """Statistiques d'un scan."""

    total_objects: int
    total_size: int
    duplicate_groups: int
    duplicate_objects: int
    wasted_bytes: int
