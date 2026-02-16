"""Hasher — téléchargement streaming et SHA256 (passe 3)."""

import hashlib

import boto3
import duckdb
from rich.progress import (
    BarColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
)

from s3dedup.db import find_multipart_candidates, update_sha256

# Taille des chunks pour le téléchargement streaming (1 Mo)
CHUNK_SIZE = 1024 * 1024


def sha256_stream(stream, chunk_size: int = CHUNK_SIZE) -> str:
    """Calcule le SHA256 d'un flux en streaming (mémoire constante)."""
    h = hashlib.sha256()
    for chunk in iter(lambda: stream.read(chunk_size), b""):
        h.update(chunk)
    return h.hexdigest()


def hash_object(s3_client, bucket: str, key: str) -> str:
    """Télécharge un objet S3 en streaming et retourne son SHA256."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return sha256_stream(response["Body"])


def hash_multipart_candidates(
    bucket: str,
    conn: duckdb.DuckDBPyConnection,
    s3_client=None,
) -> int:
    """Hashe les objets multipart candidats (passe 3).

    Retourne le nombre d'objets hashés.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    groups = find_multipart_candidates(conn)
    if not groups:
        return 0

    # Compter le total d'objets à hasher
    all_objects = [obj for group in groups for obj in group]
    total = len(all_objects)

    hashed = 0
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("{task.fields[current]}"),
    ) as progress:
        task = progress.add_task(
            "Hash passe 3",
            total=total,
            current="",
        )

        for obj in all_objects:
            progress.update(task, current=obj.key)
            digest = hash_object(s3_client, bucket, obj.key)
            update_sha256(conn, obj.key, digest)
            hashed += 1
            progress.advance(task)

    return hashed
