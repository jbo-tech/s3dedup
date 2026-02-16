"""ScriptGenerator — génération du script bash de suppression."""

from datetime import datetime

import duckdb

from s3dedup.db import get_all_duplicates, get_stats
from s3dedup.models import DuplicateGroup, ObjectInfo

# Politiques de rétention : quelle copie garder
KEEP_POLICIES = {
    "oldest": lambda objs: min(objs, key=lambda o: o.last_modified),
    "newest": lambda objs: max(objs, key=lambda o: o.last_modified),
    "largest": lambda objs: max(objs, key=lambda o: o.size),
}


def _select_to_delete(
    group: DuplicateGroup,
    keep: str,
) -> tuple[ObjectInfo, list[ObjectInfo]]:
    """Sélectionne l'objet à garder et ceux à supprimer."""
    policy = KEEP_POLICIES[keep]
    keeper = policy(group.objects)
    to_delete = [o for o in group.objects if o.key != keeper.key]
    return keeper, to_delete


def generate_delete_script(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    keep: str = "oldest",
    output: str = "delete_duplicates.sh",
) -> str:
    """Génère un script bash de suppression des doublons.

    Retourne le contenu du script.
    """
    groups = get_all_duplicates(conn)
    stats = get_stats(conn)

    lines: list[str] = []

    # En-tête
    lines.append("#!/usr/bin/env bash")
    lines.append("# Script de suppression des doublons S3")
    lines.append(f"# Bucket : {bucket}")
    lines.append(f"# Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"# Politique de rétention : --keep {keep}")
    lines.append(f"# Groupes de doublons : {stats.duplicate_groups}")
    lines.append(f"# Objets à supprimer : {stats.duplicate_objects}")
    lines.append(f"# Espace récupérable : {_human_size(stats.wasted_bytes)}")
    lines.append("#")
    lines.append("# ATTENTION : Vérifiez ce script avant exécution !")
    lines.append("# Les suppressions S3 sont IRRÉVERSIBLES.")
    lines.append("#")
    lines.append("# Pour un dry-run, décommentez la ligne suivante :")
    lines.append('# DRY_RUN="--dryrun"')
    lines.append("")
    lines.append('set -euo pipefail')
    lines.append("")

    if not groups:
        lines.append("echo 'Aucun doublon détecté.'")
        content = "\n".join(lines) + "\n"
        _write_file(output, content)
        return content

    for i, group in enumerate(groups, 1):
        keeper, to_delete = _select_to_delete(group, keep)

        lines.append(f"# --- Groupe {i} ({len(group.objects)} copies,"
                      f" {_human_size(group.wasted_bytes)} récupérables)")
        lines.append(f"# Fingerprint : {group.fingerprint}")
        lines.append(f"# Conservé    : {keeper.key}")

        for obj in to_delete:
            key_escaped = obj.key.replace("'", "'\\''")
            lines.append(
                f"aws s3 rm ${{DRY_RUN:-}}"
                f" 's3://{bucket}/{key_escaped}'"
            )
        lines.append("")

    lines.append(f"echo 'Terminé : {stats.duplicate_objects}"
                  f" objets supprimés,"
                  f" {_human_size(stats.wasted_bytes)} récupérés.'")

    content = "\n".join(lines) + "\n"
    _write_file(output, content)
    return content


def _write_file(path: str, content: str) -> None:
    """Écrit le script et le rend exécutable."""
    import os
    with open(path, "w") as f:
        f.write(content)
    os.chmod(path, 0o755)


def _human_size(size_bytes: int) -> str:
    """Convertit des bytes en format lisible."""
    for unit in ("o", "Ko", "Mo", "Go", "To"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} Po"
