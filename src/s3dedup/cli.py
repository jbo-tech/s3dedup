"""Interface CLI pour s3dedup."""

import sys

import boto3
import click
from rich.console import Console

from s3dedup import db as database
from s3dedup.cleaner import generate_clean_script
from s3dedup.hasher import hash_multipart_candidates
from s3dedup.reporter import generate_report
from s3dedup.scanner import extract_all_media_metadata, scan_bucket
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
@click.option(
    "--extract-metadata", is_flag=True, default=False,
    help="Extraire les métadonnées des fichiers média (audio/vidéo).",
)
@click.option(
    "--endpoint-url",
    envvar="AWS_ENDPOINT_URL",
    default=None,
    help="URL du endpoint S3 (pour les services S3-compatibles).",
)
def scan(bucket, prefix, db_path, extract_metadata, endpoint_url):
    """Scanner un bucket S3 et indexer les objets."""
    try:
        conn = database.connect(db_path)
    except Exception as e:
        console.print(f"[red]Erreur DB :[/red] {e}")
        sys.exit(1)

    s3_client = _make_s3_client(endpoint_url)

    try:
        result = scan_bucket(bucket, conn, prefix=prefix, s3_client=s3_client)

        # Persister l'endpoint pour ce bucket
        old_endpoint = database.set_bucket_config(
            conn, bucket, endpoint_url,
        )
        if old_endpoint:
            console.print(
                f"[yellow]Attention :[/yellow] endpoint changé"
                f" ({old_endpoint} → {endpoint_url})"
            )

        parts = []
        if result.new:
            parts.append(f"{result.new} nouveaux")
        if result.updated:
            parts.append(f"{result.updated} modifiés")
        if result.deleted:
            parts.append(f"{result.deleted} supprimés")
        summary = ", ".join(parts) if parts else "aucun changement"
        console.print(f"[green]Scan terminé :[/green] {summary}.")

        # Passe 3 : hash des candidats multipart
        hashed = hash_multipart_candidates(
            bucket, conn, s3_client=s3_client,
        )
        if hashed:
            console.print(
                f"[green]Hash passe 3 :[/green] {hashed} objets hashés."
            )

        # Extraction métadonnées média (optionnel)
        if extract_metadata:
            enriched = extract_all_media_metadata(
                bucket, conn, s3_client=s3_client,
            )
            if enriched:
                console.print(
                    f"[green]Métadonnées :[/green]"
                    f" {enriched} fichiers média enrichis."
                )

        stats = database.get_stats(conn)
        console.print(
            f"\n{stats.total_objects} objets, "
            f"{stats.duplicate_groups} groupes de doublons, "
            f"{stats.duplicate_objects} doublons."
        )
        console.print(
            "\n[dim]Étapes suivantes :[/dim]\n"
            f"  s3dedup report [--format table|json|csv|markdown]"
            f" [--output rapport.md] --db {db_path}\n"
            f"  s3dedup generate-script --bucket {bucket}"
            f" --keep cleanest,shortest,oldest --db {db_path}"
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
    type=click.Choice(["table", "json", "csv", "markdown"]),
    default="table",
    help="Format du rapport.",
)
@click.option(
    "--db", "db_path",
    default="s3dedup.duckdb",
    help="Chemin vers la base DuckDB.",
)
@click.option(
    "--output", "-o",
    default=None,
    help="Fichier de sortie (défaut : stdout).",
)
def report(fmt, db_path, output):
    """Générer un rapport des doublons détectés."""
    try:
        conn = database.connect(db_path)
    except Exception as e:
        console.print(f"[red]Erreur DB :[/red] {e}")
        sys.exit(1)

    try:
        content = generate_report(conn, fmt=fmt)

        if output:
            with open(output, "w", encoding="utf-8") as f:
                f.write(content)
            console.print(f"[green]Rapport écrit :[/green] {output}")
        else:
            click.echo(content)

        if fmt == "table":
            stats = database.get_stats(conn)
            if stats.duplicate_groups:
                console.print(
                    "\n[dim]Étape suivante :[/dim]\n"
                    f"  s3dedup generate-script --bucket BUCKET"
                    f" --keep cleanest,shortest,oldest --db {db_path}"
                )
    finally:
        conn.close()


@cli.command("generate-script")
@click.option(
    "--bucket", required=True, help="Nom du bucket S3.",
)
@click.option(
    "--keep",
    default="cleanest,shortest,oldest",
    help="Critères de rétention (cleanest,shortest,oldest,newest).",
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
@click.option(
    "--endpoint-url",
    envvar="AWS_ENDPOINT_URL",
    default=None,
    help="URL du endpoint S3 (pour les services S3-compatibles).",
)
def generate_script(bucket, keep, db_path, output, endpoint_url):
    """Générer un script de suppression des doublons."""
    try:
        conn = database.connect(db_path)
    except Exception as e:
        console.print(f"[red]Erreur DB :[/red] {e}")
        sys.exit(1)

    try:
        # Fallback sur l'endpoint stocké lors du scan
        if not endpoint_url:
            stored = database.get_bucket_config(conn, bucket)
            if stored:
                endpoint_url = stored
                console.print(
                    f"[dim]Endpoint depuis le scan :[/dim] {endpoint_url}"
                )

        generate_delete_script(
            conn, bucket, keep=keep, output=output,
            endpoint_url=endpoint_url,
        )
        stats = database.get_stats(conn)
        console.print(
            f"[green]Script généré :[/green] {output}\n"
            f"{stats.duplicate_objects} objets à supprimer.\n"
        )
        console.print(
            "[dim]Vérifier puis lancer :[/dim]\n"
            f"  cat {output}        # relire le script\n"
            f"  bash {output}       # exécuter les suppressions"
        )
    except Exception as e:
        console.print(f"[red]Erreur :[/red] {e}")
        sys.exit(1)
    finally:
        conn.close()


@cli.command()
@click.option(
    "--bucket", required=True, help="Nom du bucket S3.",
)
@click.option("--prefix", default="", help="Préfixe pour filtrer les clés.")
@click.option(
    "--rules",
    default="strip-spaces",
    help="Règles de nettoyage, séparées par virgules.",
)
@click.option(
    "--db", "db_path",
    default="s3dedup.duckdb",
    help="Chemin vers la base DuckDB.",
)
@click.option(
    "--output",
    default="clean.sh",
    help="Fichier de sortie.",
)
@click.option(
    "--endpoint-url",
    envvar="AWS_ENDPOINT_URL",
    default=None,
    help="URL du endpoint S3 (pour les services S3-compatibles).",
)
def clean(bucket, prefix, rules, db_path, output, endpoint_url):
    """Générer un script de renommage pour nettoyer les clés S3."""
    try:
        conn = database.connect(db_path)
    except Exception as e:
        console.print(f"[red]Erreur DB :[/red] {e}")
        sys.exit(1)

    try:
        # Fallback sur l'endpoint stocké lors du scan
        if not endpoint_url:
            stored = database.get_bucket_config(conn, bucket)
            if stored:
                endpoint_url = stored
                console.print(
                    f"[dim]Endpoint depuis le scan :[/dim] {endpoint_url}"
                )

        rule_list = [r.strip() for r in rules.split(",")]
        generate_clean_script(
            conn, bucket,
            rules=rule_list,
            prefix=prefix,
            output=output,
            endpoint_url=endpoint_url,
        )
        console.print(f"[green]Script généré :[/green] {output}")
        console.print(
            "[dim]Vérifier puis lancer :[/dim]\n"
            f"  cat {output}        # relire le script\n"
            f"  bash {output}       # exécuter les renommages"
        )
    except Exception as e:
        console.print(f"[red]Erreur :[/red] {e}")
        sys.exit(1)
    finally:
        conn.close()
