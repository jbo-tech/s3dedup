"""Reporter — génération de rapports de doublons (JSON/CSV)."""

import csv
import json
from io import StringIO

import duckdb

from s3dedup.db import get_all_duplicates, get_stats


def generate_report(
    conn: duckdb.DuckDBPyConnection,
    fmt: str = "json",
) -> str:
    """Génère un rapport de doublons au format JSON ou CSV."""
    groups = get_all_duplicates(conn)
    stats = get_stats(conn)

    if fmt == "json":
        return _to_json(groups, stats)
    return _to_csv(groups, stats)


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
