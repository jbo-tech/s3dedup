"""Module d'accès à la base DuckDB pour l'index des objets S3."""

from datetime import datetime

import duckdb

from s3dedup.models import DuplicateGroup, ObjectInfo, ScanStats

SCHEMA = """\
CREATE TABLE IF NOT EXISTS objects (
    key            VARCHAR NOT NULL PRIMARY KEY,
    size           BIGINT NOT NULL,
    etag           VARCHAR NOT NULL,
    is_multipart   BOOLEAN NOT NULL,
    sha256         VARCHAR,
    last_modified  TIMESTAMP NOT NULL,
    scanned_at     TIMESTAMP NOT NULL DEFAULT now()
);
"""


def connect(db_path: str = "s3dedup.duckdb") -> duckdb.DuckDBPyConnection:
    """Ouvre une connexion DuckDB et crée le schéma si nécessaire."""
    conn = duckdb.connect(db_path)
    conn.execute(SCHEMA)
    return conn


def upsert_objects(
    conn: duckdb.DuckDBPyConnection,
    objects: list[ObjectInfo],
) -> int:
    """Insère ou met à jour des objets dans l'index. Retourne le nombre inséré."""
    if not objects:
        return 0
    rows = [
        (o.key, o.size, o.etag, o.is_multipart, o.sha256, o.last_modified)
        for o in objects
    ]
    conn.executemany(
        """
        INSERT INTO objects (key, size, etag, is_multipart, sha256, last_modified)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (key) DO UPDATE SET
            size = excluded.size,
            etag = excluded.etag,
            is_multipart = excluded.is_multipart,
            sha256 = excluded.sha256,
            last_modified = excluded.last_modified,
            scanned_at = now()
        """,
        rows,
    )
    return len(rows)


def find_size_duplicates(
    conn: duckdb.DuckDBPyConnection,
) -> list[list[ObjectInfo]]:
    """Passe 1 : retourne les groupes d'objets ayant la même taille."""
    rows = conn.execute(
        """
        SELECT key, size, etag, is_multipart, sha256, last_modified
        FROM objects
        WHERE size IN (
            SELECT size FROM objects GROUP BY size HAVING count(*) > 1
        )
        ORDER BY size, key
        """
    ).fetchall()
    return _group_rows(rows, key_index=1)


def find_etag_duplicates(
    conn: duckdb.DuckDBPyConnection,
) -> list[DuplicateGroup]:
    """Passe 2 : parmi les objets de même taille, groupe par ETag."""
    rows = conn.execute(
        """
        WITH size_dups AS (
            SELECT size FROM objects GROUP BY size HAVING count(*) > 1
        )
        SELECT o.key, o.size, o.etag, o.is_multipart, o.sha256,
               o.last_modified
        FROM objects o
        JOIN size_dups s ON o.size = s.size
        WHERE o.etag IN (
            SELECT etag FROM objects o2
            JOIN size_dups s2 ON o2.size = s2.size
            GROUP BY etag HAVING count(*) > 1
        )
        ORDER BY o.etag, o.key
        """
    ).fetchall()
    groups = _group_rows(rows, key_index=2)
    return [
        DuplicateGroup(
            fingerprint=objs[0].etag,
            size=objs[0].size,
            objects=objs,
        )
        for objs in groups
    ]


def find_multipart_candidates(
    conn: duckdb.DuckDBPyConnection,
) -> list[list[ObjectInfo]]:
    """Retourne les groupes de même taille avec des multipart non hashés."""
    rows = conn.execute(
        """
        WITH candidates AS (
            SELECT size FROM objects
            WHERE is_multipart = true AND sha256 IS NULL
            GROUP BY size
            HAVING size IN (
                SELECT size FROM objects GROUP BY size HAVING count(*) > 1
            )
        )
        SELECT o.key, o.size, o.etag, o.is_multipart, o.sha256,
               o.last_modified
        FROM objects o
        JOIN candidates c ON o.size = c.size
        ORDER BY o.size, o.key
        """
    ).fetchall()
    return _group_rows(rows, key_index=1)


def update_sha256(
    conn: duckdb.DuckDBPyConnection,
    key: str,
    sha256: str,
) -> None:
    """Met à jour le hash SHA256 d'un objet après téléchargement."""
    conn.execute(
        "UPDATE objects SET sha256 = ? WHERE key = ?",
        [sha256, key],
    )


def find_hash_duplicates(
    conn: duckdb.DuckDBPyConnection,
) -> list[DuplicateGroup]:
    """Passe 3 : doublons par SHA256 (pour les multipart hashés)."""
    rows = conn.execute(
        """
        SELECT key, size, etag, is_multipart, sha256, last_modified
        FROM objects
        WHERE sha256 IN (
            SELECT sha256 FROM objects
            WHERE sha256 IS NOT NULL
            GROUP BY sha256 HAVING count(*) > 1
        )
        ORDER BY sha256, key
        """
    ).fetchall()
    groups = _group_rows(rows, key_index=4)
    return [
        DuplicateGroup(
            fingerprint=objs[0].sha256,
            size=objs[0].size,
            objects=objs,
        )
        for objs in groups
    ]


def get_all_duplicates(
    conn: duckdb.DuckDBPyConnection,
) -> list[DuplicateGroup]:
    """Retourne tous les groupes de doublons (ETag non-multipart + SHA256)."""
    etag_groups = find_etag_duplicates(conn)
    hash_groups = find_hash_duplicates(conn)

    # Les groupes ETag : garder uniquement ceux sans multipart
    non_multipart = [
        g for g in etag_groups
        if not any(o.is_multipart for o in g.objects)
    ]
    return non_multipart + hash_groups


def get_stats(conn: duckdb.DuckDBPyConnection) -> ScanStats:
    """Calcule les statistiques globales."""
    row = conn.execute(
        "SELECT count(*), coalesce(sum(size), 0) FROM objects"
    ).fetchone()
    total_objects, total_size = row

    groups = get_all_duplicates(conn)
    dup_objects = sum(len(g.objects) - 1 for g in groups)
    wasted = sum(g.wasted_bytes for g in groups)

    return ScanStats(
        total_objects=total_objects,
        total_size=total_size,
        duplicate_groups=len(groups),
        duplicate_objects=dup_objects,
        wasted_bytes=wasted,
    )


def _group_rows(
    rows: list[tuple],
    key_index: int,
) -> list[list[ObjectInfo]]:
    """Groupe des lignes SQL par la colonne à key_index."""
    groups: dict[str | int, list[ObjectInfo]] = {}
    for row in rows:
        obj = ObjectInfo(
            key=row[0],
            size=row[1],
            etag=row[2],
            is_multipart=row[3],
            sha256=row[4],
            last_modified=row[5] if isinstance(row[5], datetime)
            else datetime.fromisoformat(str(row[5])),
        )
        group_key = row[key_index]
        groups.setdefault(group_key, []).append(obj)
    return [objs for objs in groups.values() if len(objs) > 1]
