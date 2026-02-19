"""Reporter — génération de rapports de doublons."""

import csv
import json
from collections import defaultdict
from io import StringIO

import duckdb
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from s3dedup.db import find_metadata_groups, get_all_duplicates, get_stats
from s3dedup.normalizer import normalize_name
from s3dedup.utils import human_size


def generate_report(
    conn: duckdb.DuckDBPyConnection,
    fmt: str = "table",
) -> str:
    """Génère un rapport de doublons au format demandé."""
    groups = get_all_duplicates(conn)
    stats = get_stats(conn)
    suspect_groups = find_suspect_names(conn)
    media_groups = find_metadata_groups(conn)

    if fmt == "json":
        return _to_json(groups, stats, suspect_groups, media_groups)
    if fmt == "csv":
        return _to_csv(groups, stats, suspect_groups, media_groups)
    return _to_table(groups, stats, suspect_groups, media_groups)


def find_suspect_names(
    conn: duckdb.DuckDBPyConnection,
) -> list[dict]:
    """Trouve les fichiers aux noms normalisés identiques, contenus différents.

    Retourne une liste de groupes :
    [{"normalized": str, "files": [{"key": str, "size": int, "etag": str}]}]
    """
    rows = conn.execute(
        "SELECT key, size, etag FROM objects ORDER BY key"
    ).fetchall()

    # Regrouper par nom normalisé
    by_name: dict[str, list[dict]] = defaultdict(list)
    for key, size, etag in rows:
        normalized = normalize_name(key)
        by_name[normalized].append({
            "key": key, "size": size, "etag": etag,
        })

    # Garder uniquement les groupes avec des contenus différents
    # (même nom normalisé mais au moins 2 etags distincts)
    result = []
    for normalized, files in sorted(by_name.items()):
        if len(files) < 2:
            continue
        etags = {f["etag"] for f in files}
        if len(etags) < 2:
            continue
        result.append({
            "normalized": normalized,
            "files": files,
        })
    return result


def _to_table(groups, stats, suspect_groups=None,
              media_groups=None) -> str:
    """Rapport formaté pour le terminal avec rich."""
    console = Console(file=StringIO(), force_terminal=True)

    # Résumé statistique
    summary = (
        f"[bold]Objets scannés :[/bold] {stats.total_objects}\n"
        f"[bold]Taille totale  :[/bold] {human_size(stats.total_size)}\n"
        f"[bold]Groupes de doublons :[/bold] {stats.duplicate_groups}\n"
        f"[bold]Objets en double    :[/bold] {stats.duplicate_objects}\n"
        f"[bold]Espace récupérable  :[/bold] "
        f"[red]{human_size(stats.wasted_bytes)}[/red]"
    )
    console.print(Panel(summary, title="Résumé", border_style="blue"))

    if not groups:
        console.print("[green]Aucun doublon détecté.[/green]")
    else:
        # Trier par espace gaspillé décroissant
        sorted_groups = sorted(
            groups, key=lambda g: g.wasted_bytes, reverse=True,
        )

        # Tableau des groupes
        table = Table(
            title="Groupes de doublons",
            show_lines=True,
        )
        table.add_column("#", style="dim", width=4)
        table.add_column("Copies", justify="right")
        table.add_column("Taille fichier", justify="right")
        table.add_column("Espace perdu", justify="right", style="red")
        table.add_column("Fichiers")

        for i, g in enumerate(sorted_groups, 1):
            files = "\n".join(o.key for o in g.objects)
            table.add_row(
                str(i),
                str(len(g.objects)),
                human_size(g.size),
                human_size(g.wasted_bytes),
                files,
            )

        console.print(table)

    # Section noms suspects
    if suspect_groups:
        table = Table(
            title="Noms suspects (même nom, contenu différent)",
            show_lines=True,
        )
        table.add_column("#", style="dim", width=4)
        table.add_column("Nom normalisé")
        table.add_column("Fichiers")
        table.add_column("Tailles", justify="right")

        for i, sg in enumerate(suspect_groups, 1):
            files = "\n".join(f["key"] for f in sg["files"])
            sizes = "\n".join(
                human_size(f["size"]) for f in sg["files"]
            )
            table.add_row(
                str(i), sg["normalized"], files, sizes,
            )

        console.print(table)

    # Section même œuvre, encodage différent
    if media_groups:
        table = Table(
            title="Même œuvre, encodage différent",
            show_lines=True,
        )
        table.add_column("#", style="dim", width=4)
        table.add_column("Artiste")
        table.add_column("Titre")
        table.add_column("Fichiers")
        table.add_column("Codec")
        table.add_column("Taille", justify="right")

        for i, mg in enumerate(media_groups, 1):
            files = "\n".join(f["key"] for f in mg["files"])
            codecs = "\n".join(
                f["codec"] or "?" for f in mg["files"]
            )
            sizes = "\n".join(
                human_size(f["size"]) for f in mg["files"]
            )
            table.add_row(
                str(i),
                mg["artist"],
                mg["title"],
                files,
                codecs,
                sizes,
            )

        console.print(table)

    return console.file.getvalue()


def _to_json(groups, stats, suspect_groups=None,
             media_groups=None) -> str:
    """Sérialise le rapport en JSON."""
    data = {
        "stats": {
            "total_objects": stats.total_objects,
            "total_size": stats.total_size,
            "duplicate_groups": stats.duplicate_groups,
            "duplicate_objects": stats.duplicate_objects,
            "wasted_bytes": stats.wasted_bytes,
        },
        "groups": [
            {
                "fingerprint": g.fingerprint,
                "size": g.size,
                "wasted_bytes": g.wasted_bytes,
                "objects": [
                    {
                        "key": o.key,
                        "last_modified": o.last_modified.isoformat(),
                    }
                    for o in g.objects
                ],
            }
            for g in groups
        ],
    }
    if suspect_groups:
        data["suspect_names"] = suspect_groups
    if media_groups:
        data["same_work"] = media_groups
    return json.dumps(data, indent=2, ensure_ascii=False)


def _to_csv(groups, stats, suspect_groups=None,
            media_groups=None) -> str:
    """Sérialise le rapport en CSV (une ligne par objet doublon)."""
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "section",
        "group_id",
        "group_size",
        "object_key",
        "detail",
    ])
    for g in groups:
        for o in g.objects:
            writer.writerow([
                "duplicate",
                g.fingerprint,
                g.size,
                o.key,
                o.last_modified.isoformat(),
            ])
    if suspect_groups:
        for sg in suspect_groups:
            for f in sg["files"]:
                writer.writerow([
                    "suspect_name",
                    sg["normalized"],
                    f["size"],
                    f["key"],
                    f["etag"],
                ])
    if media_groups:
        for mg in media_groups:
            group_id = f"{mg['artist']} - {mg['title']}"
            for f in mg["files"]:
                writer.writerow([
                    "same_work",
                    group_id,
                    f["size"],
                    f["key"],
                    f["codec"] or "",
                ])
    return output.getvalue()
