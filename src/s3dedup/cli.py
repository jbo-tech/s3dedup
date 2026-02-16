"""Interface CLI pour s3dedup."""

import sys

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
def cli():
    """Détection de doublons dans un bucket S3."""


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
def scan(bucket, prefix, db_path):
    """Scanner un bucket S3 et indexer les objets."""
    try:
        conn = database.connect(db_path)
    except Exception as e:
        console.print(f"[red]Erreur DB :[/red] {e}")
        sys.exit(1)

    try:
        count = scan_bucket(bucket, conn, prefix=prefix)
        console.print(
            f"[green]Scan terminé :[/green] {count} objets indexés."
        )

        # Passe 3 : hash des candidats multipart
        hashed = hash_multipart_candidates(bucket, conn)
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
    except Exception as e:
        console.print(f"[red]Erreur scan :[/red] {e}")
        sys.exit(1)
    finally:
        conn.close()


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
