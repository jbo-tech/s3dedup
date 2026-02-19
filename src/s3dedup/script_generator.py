"""ScriptGenerator — génération du script bash de suppression."""

import posixpath
from datetime import datetime

import duckdb

from s3dedup.db import get_all_duplicates, get_stats
from s3dedup.models import DuplicateGroup, ObjectInfo
from s3dedup.normalizer import name_quality_score
from s3dedup.utils import human_size

# Critères de rétention disponibles.
# Chaque critère retourne une clé de tri (le min gagne).
KEEP_CRITERIA = {
    "shortest": lambda o: len(posixpath.basename(o.key)),
    "oldest": lambda o: o.last_modified,
    "newest": lambda o: -o.last_modified.timestamp(),
    "cleanest": lambda o: name_quality_score(o.key),
}

VALID_CRITERIA = set(KEEP_CRITERIA.keys())
DEFAULT_KEEP = "shortest,oldest"


def parse_keep(keep: str) -> list[str]:
    """Parse et valide une chaîne de critères séparés par des virgules."""
    criteria = [c.strip() for c in keep.split(",")]
    invalid = [c for c in criteria if c not in VALID_CRITERIA]
    if invalid:
        valid = ", ".join(sorted(VALID_CRITERIA))
        raise click_bad_param(
            f"Critères invalides : {', '.join(invalid)}. "
            f"Valides : {valid}"
        )
    return criteria


def click_bad_param(msg: str) -> Exception:
    """Crée une exception click pour un paramètre invalide."""
    import click
    return click.BadParameter(msg)


def _select_to_delete(
    group: DuplicateGroup,
    criteria: list[str],
) -> tuple[ObjectInfo, list[ObjectInfo]]:
    """Sélectionne l'objet à garder via tri multi-critères."""
    sort_key = _build_sort_key(criteria)
    keeper = min(group.objects, key=sort_key)
    to_delete = [o for o in group.objects if o.key != keeper.key]
    return keeper, to_delete


def _build_sort_key(criteria: list[str]):
    """Construit une fonction de tri composite."""
    fns = [KEEP_CRITERIA[c] for c in criteria]
    return lambda o: tuple(fn(o) for fn in fns)


def generate_delete_script(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    keep: str = DEFAULT_KEEP,
    output: str = "delete_duplicates.sh",
    endpoint_url: str | None = None,
) -> str:
    """Génère un script bash de suppression des doublons.

    Retourne le contenu du script.
    """
    criteria = parse_keep(keep)
    groups = get_all_duplicates(conn)
    stats = get_stats(conn)

    lines: list[str] = []

    # En-tête
    lines.append("#!/usr/bin/env bash")
    lines.append("# Script de suppression des doublons S3")
    lines.append(f"# Bucket : {bucket}")
    lines.append(
        f"# Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    lines.append(f"# Politique de rétention : --keep {keep}")
    lines.append(f"# Groupes de doublons : {stats.duplicate_groups}")
    lines.append(f"# Objets à supprimer : {stats.duplicate_objects}")
    lines.append(
        f"# Espace récupérable : {human_size(stats.wasted_bytes)}"
    )
    lines.append("#")
    lines.append("# ATTENTION : Vérifiez ce script avant exécution !")
    lines.append("# Les suppressions S3 sont IRRÉVERSIBLES.")
    lines.append("#")
    lines.append("# Pour un dry-run, décommentez la ligne suivante :")
    lines.append('# DRY_RUN="--dryrun"')
    lines.append("")
    lines.append('set -euo pipefail')
    if endpoint_url:
        lines.append(f'ENDPOINT="--endpoint-url {endpoint_url}"')
    else:
        lines.append('ENDPOINT=""')
    lines.append("")

    if not groups:
        lines.append("echo 'Aucun doublon détecté.'")
        content = "\n".join(lines) + "\n"
        _write_file(output, content)
        return content

    for i, group in enumerate(groups, 1):
        keeper, to_delete = _select_to_delete(group, criteria)

        lines.append(
            f"# --- Groupe {i} ({len(group.objects)} copies,"
            f" {human_size(group.wasted_bytes)} récupérables)"
        )
        lines.append(f"# Fingerprint : {group.fingerprint}")
        lines.append(f"# Conservé    : {keeper.key}")

        for obj in to_delete:
            key_escaped = obj.key.replace("'", "'\\''")
            lines.append(
                f"aws s3 rm ${{DRY_RUN:-}} $ENDPOINT"
                f" 's3://{bucket}/{key_escaped}'"
            )
        lines.append("")

    lines.append(
        f"echo 'Terminé : {stats.duplicate_objects}"
        f" objets supprimés,"
        f" {human_size(stats.wasted_bytes)} récupérés.'"
    )

    content = "\n".join(lines) + "\n"
    _write_file(output, content)
    return content


def _write_file(path: str, content: str) -> None:
    """Écrit le script et le rend exécutable."""
    import os
    with open(path, "w") as f:
        f.write(content)
    os.chmod(path, 0o755)
