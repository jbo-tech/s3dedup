"""Scanner S3 — listing paginé et indexation dans DuckDB."""

import boto3
import duckdb
from rich.progress import Progress, SpinnerColumn, TextColumn

from s3dedup.db import upsert_objects
from s3dedup.models import ObjectInfo

# Taille du batch pour l'upsert en base
BATCH_SIZE = 1000


def is_multipart_etag(etag: str) -> bool:
    """Détecte un ETag multipart (format 'hash-N')."""
    clean = etag.strip('"')
    return "-" in clean and clean.rsplit("-", 1)[-1].isdigit()


def _get_existing_keys(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Récupère les clés déjà indexées pour la reprise."""
    rows = conn.execute("SELECT key FROM objects").fetchall()
    return {r[0] for r in rows}


def scan_bucket(
    bucket: str,
    conn: duckdb.DuckDBPyConnection,
    prefix: str = "",
    s3_client=None,
) -> int:
    """Scanne un bucket S3 et indexe les objets dans DuckDB.

    Retourne le nombre d'objets indexés.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    existing_keys = _get_existing_keys(conn)
    total_indexed = 0
    batch: list[ObjectInfo] = []

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("{task.fields[indexed]} objets indexés"),
    ) as progress:
        task = progress.add_task(
            f"Scan s3://{bucket}/{prefix}",
            indexed=0,
        )

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key in existing_keys:
                    continue
                # Ignorer les objets vides (marqueurs de dossier S3)
                if obj["Size"] == 0:
                    continue

                etag = obj["ETag"]
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
                    total_indexed += len(batch)
                    progress.update(task, indexed=total_indexed)
                    batch.clear()

        # Dernier batch
        if batch:
            upsert_objects(conn, batch)
            total_indexed += len(batch)

    return total_indexed
