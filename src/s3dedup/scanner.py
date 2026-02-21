"""Scanner S3 — listing paginé et indexation dans DuckDB."""

import boto3
import duckdb
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
)

from s3dedup.db import (
    delete_objects,
    get_keys_with_prefix,
    upsert_media_metadata,
    upsert_objects,
)
from s3dedup.media import extract_metadata, is_media_file
from s3dedup.models import ObjectInfo, ScanResult

# Taille du batch pour l'upsert en base
BATCH_SIZE = 1000


def is_multipart_etag(etag: str) -> bool:
    """Détecte un ETag multipart (format 'hash-N')."""
    clean = etag.strip('"')
    return "-" in clean and clean.rsplit("-", 1)[-1].isdigit()


def scan_bucket(
    bucket: str,
    conn: duckdb.DuckDBPyConnection,
    prefix: str = "",
    s3_client=None,
) -> ScanResult:
    """Scanne un bucket S3 et indexe les objets dans DuckDB.

    Détecte les nouveaux objets, les modifications (ETag changé)
    et les suppressions (clés absentes du listing S3).
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    existing_etags = get_keys_with_prefix(conn, prefix)
    new_count = 0
    updated_count = 0
    seen_keys: set[str] = set()
    batch: list[ObjectInfo] = []

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("{task.fields[status]}"),
    ) as progress:
        task = progress.add_task(
            f"Scan s3://{bucket}/{prefix}",
            status="0 nouveaux, 0 modifiés",
        )

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Ignorer les objets vides (marqueurs de dossier S3)
                if obj["Size"] == 0:
                    continue

                seen_keys.add(key)
                etag = obj["ETag"]

                # Skip si déjà en base avec le même ETag
                if key in existing_etags and existing_etags[key] == etag:
                    continue

                is_update = key in existing_etags
                if is_update:
                    updated_count += 1
                else:
                    new_count += 1

                info = ObjectInfo(
                    key=key,
                    size=obj["Size"],
                    etag=etag,
                    is_multipart=is_multipart_etag(etag),
                    last_modified=obj["LastModified"],
                )
                batch.append(info)

                if len(batch) >= BATCH_SIZE:
                    upsert_objects(conn, batch)
                    progress.update(
                        task,
                        status=f"{new_count} nouveaux, {updated_count} modifiés",
                    )
                    batch.clear()

        # Dernier batch
        if batch:
            upsert_objects(conn, batch)

    # Détecter les suppressions : clés en base absentes du listing S3
    deleted_keys = [k for k in existing_etags if k not in seen_keys]
    if deleted_keys:
        delete_objects(conn, deleted_keys)

    return ScanResult(
        new=new_count,
        updated=updated_count,
        deleted=len(deleted_keys),
    )


def extract_all_media_metadata(
    bucket: str,
    conn: duckdb.DuckDBPyConnection,
    s3_client=None,
) -> int:
    """Extrait les métadonnées des fichiers média non encore enrichis.

    Retourne le nombre de fichiers traités.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    # Fichiers média sans métadonnées existantes
    rows = conn.execute(
        """
        SELECT o.key FROM objects o
        LEFT JOIN media_metadata m ON o.key = m.key
        WHERE m.key IS NULL
        ORDER BY o.key
        """
    ).fetchall()
    media_keys = [r[0] for r in rows if is_media_file(r[0])]

    if not media_keys:
        return 0

    processed = 0
    batch = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
    ) as progress:
        task = progress.add_task(
            "Extraction métadonnées",
            total=len(media_keys),
        )

        for key in media_keys:
            meta = extract_metadata(s3_client, bucket, key)
            if meta is not None:
                batch.append(meta)

            if len(batch) >= BATCH_SIZE:
                upsert_media_metadata(conn, batch)
                batch.clear()

            processed += 1
            progress.advance(task)

        if batch:
            upsert_media_metadata(conn, batch)

    return processed
