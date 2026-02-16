"""Reporter — génération de rapports de doublons."""

import csv
import json
from io import StringIO

import duckdb
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from s3dedup.db import get_all_duplicates, get_stats
from s3dedup.utils import human_size


def generate_report(
    conn: duckdb.DuckDBPyConnection,
    fmt: str = "table",
) -> str:
    """Génère un rapport de doublons au format demandé."""
    groups = get_all_duplicates(conn)
    stats = get_stats(conn)

    if fmt == "json":
        return _to_json(groups, stats)
    if fmt == "csv":
        return _to_csv(groups, stats)
    return _to_table(groups, stats)


def _to_table(groups, stats) -> str:
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
        return console.file.getvalue()

    # Trier par espace gaspillé décroissant
    sorted_groups = sorted(groups, key=lambda g: g.wasted_bytes, reverse=True)

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
    return console.file.getvalue()


def _to_json(groups, stats) -> str:
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
    return json.dumps(data, indent=2, ensure_ascii=False)


def _to_csv(groups, stats) -> str:
    """Sérialise le rapport en CSV (une ligne par objet doublon)."""
    output = StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "group_fingerprint",
        "group_size",
        "group_wasted_bytes",
        "object_key",
        "last_modified",
    ])
    for g in groups:
        for o in g.objects:
            writer.writerow([
                g.fingerprint,
                g.size,
                g.wasted_bytes,
                o.key,
                o.last_modified.isoformat(),
            ])
    return output.getvalue()
