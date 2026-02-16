"""Interface CLI pour s3dedup."""

import sys

import boto3
import click
from rich.console import Console

from s3dedup import db as database
from s3dedup.hasher import hash_multipart_candidates
from s3dedup.reporter import generate_report
from s3dedup.scanner import scan_bucket
from s3dedup.script_generator import generate_delete_script

console = Console(stderr=True)


@click.group()
@click.version_option(version="0.1.0")
@click.option(
    "--endpoint-url",
    envvar="AWS_ENDPOINT_URL",
    default=None,
    help="URL du endpoint S3 (pour les services S3-compatibles).",
)
@click.pass_context
def cli(ctx, endpoint_url):
    """Détection de doublons dans un bucket S3."""
    ctx.ensure_object(dict)
    ctx.obj["endpoint_url"] = endpoint_url


@cli.command()
@click.option(
    "--bucket", required=True, help="Nom du bucket S3.",
)
@click.option("--prefix", default="", help="Préfixe pour filtrer.")
@click.option(
    "--db", "db_path",
    default="s3dedup.duckdb",
    help="Chemin vers la base DuckDB.",
)
@click.pass_context
def scan(ctx, bucket, prefix, db_path):
    """Scanner un bucket S3 et indexer les objets."""
    try:
        conn = database.connect(db_path)
    except Exception as e:
        console.print(f"[red]Erreur DB :[/red] {e}")
        sys.exit(1)

    s3_client = _make_s3_client(ctx.obj["endpoint_url"])

    try:
        count = scan_bucket(bucket, conn, prefix=prefix, s3_client=s3_client)
        console.print(
            f"[green]Scan terminé :[/green] {count} objets indexés."
        )

        # Passe 3 : hash des candidats multipart
        hashed = hash_multipart_candidates(
            bucket, conn, s3_client=s3_client,
        )
        if hashed:
            console.print(
                f"[green]Hash passe 3 :[/green] {hashed} objets hashés."
            )

        stats = database.get_stats(conn)
        console.print(
            f"\n{stats.total_objects} objets, "
            f"{stats.duplicate_groups} groupes de doublons, "
            f"{stats.duplicate_objects} doublons."
        )
        console.print(
            "\n[dim]Étapes suivantes :[/dim]\n"
            f"  s3dedup report --format json|csv --db {db_path}\n"
            f"  s3dedup generate-script --bucket {bucket}"
            f" --keep oldest|newest|largest --db {db_path}"
        )
    except Exception as e:
        console.print(f"[red]Erreur scan :[/red] {e}")
        sys.exit(1)
    finally:
        conn.close()


def _make_s3_client(endpoint_url=None):
    """Crée un client S3 boto3, avec endpoint custom si fourni."""
    kwargs = {}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)


@cli.command()
@click.option(
    "--format", "fmt",
    type=click.Choice(["json", "csv"]),
    default="json",
    help="Format du rapport.",
)
@click.option(
    "--db", "db_path",
    default="s3dedup.duckdb",
    help="Chemin vers la base DuckDB.",
)
def report(fmt, db_path):
    """Générer un rapport des doublons détectés."""
    try:
        conn = database.connect(db_path)
    except Exception as e:
        console.print(f"[red]Erreur DB :[/red] {e}")
        sys.exit(1)

    try:
        output = generate_report(conn, fmt=fmt)
        click.echo(output)
    finally:
        conn.close()


@cli.command("generate-script")
@click.option(
    "--bucket", required=True, help="Nom du bucket S3.",
)
@click.option(
    "--keep",
    type=click.Choice(["oldest", "newest", "largest"]),
    default="oldest",
    help="Politique de rétention.",
)
@click.option(
    "--db", "db_path",
    default="s3dedup.duckdb",
    help="Chemin vers la base DuckDB.",
)
@click.option(
    "--output",
    default="delete_duplicates.sh",
    help="Fichier de sortie.",
)
def generate_script(bucket, keep, db_path, output):
    """Générer un script de suppression des doublons."""
    try:
        conn = database.connect(db_path)
    except Exception as e:
        console.print(f"[red]Erreur DB :[/red] {e}")
        sys.exit(1)

    try:
        generate_delete_script(conn, bucket, keep=keep, output=output)
        stats = database.get_stats(conn)
        console.print(
            f"[green]Script généré :[/green] {output}\n"
            f"{stats.duplicate_objects} objets à supprimer."
        )
    except Exception as e:
        console.print(f"[red]Erreur :[/red] {e}")
        sys.exit(1)
    finally:
        conn.close()
