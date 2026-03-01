"""Module d'accès à la base DuckDB pour l'index des objets S3."""

from datetime import datetime

import duckdb

from s3dedup.models import DuplicateGroup, MediaMetadata, ObjectInfo, ScanStats

SCHEMA_OBJECTS = """\
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

SCHEMA_MEDIA = """\
CREATE TABLE IF NOT EXISTS media_metadata (
    key         VARCHAR NOT NULL PRIMARY KEY,
    artist      VARCHAR,
    album       VARCHAR,
    title       VARCHAR,
    duration_s  DOUBLE,
    codec       VARCHAR,
    bitrate     INTEGER
);
"""

SCHEMA_BUCKET_CONFIG = """\
CREATE TABLE IF NOT EXISTS bucket_config (
    bucket       VARCHAR NOT NULL PRIMARY KEY,
    endpoint_url VARCHAR,
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);
"""


def connect(db_path: str = "s3dedup.duckdb") -> duckdb.DuckDBPyConnection:
    """Ouvre une connexion DuckDB et crée le schéma si nécessaire."""
    conn = duckdb.connect(db_path)
    conn.execute(SCHEMA_OBJECTS)
    conn.execute(SCHEMA_MEDIA)
    conn.execute(SCHEMA_BUCKET_CONFIG)
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


def get_keys_with_prefix(
    conn: duckdb.DuckDBPyConnection,
    prefix: str,
) -> dict[str, str]:
    """Retourne les clés et ETags des objets dont la clé commence par prefix."""
    rows = conn.execute(
        "SELECT key, etag FROM objects WHERE key LIKE ?",
        [prefix + "%"],
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def delete_objects(
    conn: duckdb.DuckDBPyConnection,
    keys: list[str],
) -> None:
    """Supprime des objets et leurs métadonnées média associées."""
    if not keys:
        return
    conn.executemany(
        "DELETE FROM media_metadata WHERE key = ?",
        [(k,) for k in keys],
    )
    conn.executemany(
        "DELETE FROM objects WHERE key = ?",
        [(k,) for k in keys],
    )


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


def upsert_media_metadata(
    conn: duckdb.DuckDBPyConnection,
    metadata_list: list[MediaMetadata],
) -> int:
    """Insère ou met à jour des métadonnées média. Retourne le nombre inséré."""
    if not metadata_list:
        return 0
    rows = [
        (m.key, m.artist, m.album, m.title, m.duration_s, m.codec, m.bitrate)
        for m in metadata_list
    ]
    conn.executemany(
        """
        INSERT INTO media_metadata
            (key, artist, album, title, duration_s, codec, bitrate)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (key) DO UPDATE SET
            artist = excluded.artist,
            album = excluded.album,
            title = excluded.title,
            duration_s = excluded.duration_s,
            codec = excluded.codec,
            bitrate = excluded.bitrate
        """,
        rows,
    )
    return len(rows)


def find_metadata_groups(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Regroupe les fichiers par artiste+titre (même œuvre, encodages différents).

    Retourne une liste de groupes, chaque groupe est un dict :
    {"artist": str, "title": str, "files": [{"key", "codec", "bitrate", "size"}]}
    """
    rows = conn.execute(
        """
        SELECT m.artist, m.title, m.key, m.codec, m.bitrate, o.size
        FROM media_metadata m
        JOIN objects o ON m.key = o.key
        WHERE m.artist IS NOT NULL AND m.title IS NOT NULL
          AND (m.artist, m.title) IN (
              SELECT artist, title FROM media_metadata
              WHERE artist IS NOT NULL AND title IS NOT NULL
              GROUP BY artist, title
              HAVING count(*) > 1
          )
        ORDER BY m.artist, m.title, o.size DESC
        """
    ).fetchall()

    groups: dict[tuple[str, str], list[dict]] = {}
    for artist, title, key, codec, bitrate, size in rows:
        group_key = (artist, title)
        groups.setdefault(group_key, []).append({
            "key": key,
            "codec": codec,
            "bitrate": bitrate,
            "size": size,
        })

    return [
        {"artist": artist, "title": title, "files": files}
        for (artist, title), files in groups.items()
    ]


def set_bucket_config(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    endpoint_url: str | None,
) -> str | None:
    """Enregistre l'endpoint d'un bucket. Retourne l'ancien endpoint si changé."""
    row = conn.execute(
        "SELECT endpoint_url FROM bucket_config WHERE bucket = ?",
        [bucket],
    ).fetchone()
    previous = row[0] if row else None

    conn.execute(
        """
        INSERT INTO bucket_config (bucket, endpoint_url)
        VALUES (?, ?)
        ON CONFLICT (bucket) DO UPDATE SET
            endpoint_url = excluded.endpoint_url,
            updated_at = now()
        """,
        [bucket, endpoint_url],
    )

    if previous is not None and previous != endpoint_url:
        return previous
    return None


def get_all_keys(
    conn: duckdb.DuckDBPyConnection,
    prefix: str = "",
) -> list[str]:
    """Retourne toutes les clés stockées, filtrées par préfixe optionnel."""
    rows = conn.execute(
        "SELECT key FROM objects WHERE key LIKE ? ORDER BY key",
        [prefix + "%"],
    ).fetchall()
    return [r[0] for r in rows]


def get_bucket_config(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
) -> str | None:
    """Retourne l'endpoint URL stocké pour un bucket, ou None."""
    row = conn.execute(
        "SELECT endpoint_url FROM bucket_config WHERE bucket = ?",
        [bucket],
    ).fetchone()
    return row[0] if row else None


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
