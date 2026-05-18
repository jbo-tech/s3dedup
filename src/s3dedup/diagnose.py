"""Diagnostic des dossiers en doublon (même album, nommage différent)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

import duckdb

from s3dedup.utils import human_size

MEDIA_EXTENSIONS = frozenset({
    ".flac", ".mp3", ".ogg", ".m4a", ".wav", ".opus", ".aac", ".wma",
})
IMAGE_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp",
})

# Pattern : nom d'album suivi de [ID] et optionnellement [année]
_ID_SUFFIX_RE = re.compile(r"^(.*?)\s*\[\d+\](?:\s*\[\d{4}\])?$")


class Category(Enum):
    """Classification d'un groupe de dossiers en doublon."""

    ORPHAN = "orphan"
    BOTH_MUSIC = "both_music"


@dataclass
class FolderInfo:
    """Statistiques d'un dossier."""

    path: str
    file_count: int = 0
    media_count: int = 0
    image_count: int = 0
    total_size: int = 0


@dataclass
class DuplicateFolderGroup:
    """Groupe de dossiers pointant vers le même album."""

    base_name: str
    category: Category
    folders: list[FolderInfo] = field(default_factory=list)

    @property
    def orphan(self) -> FolderInfo | None:
        """Retourne le dossier orphelin (sans musique), ou None."""
        if self.category != Category.ORPHAN:
            return None
        for f in self.folders:
            if f.media_count == 0:
                return f
        return None

    @property
    def complete(self) -> FolderInfo | None:
        """Retourne le dossier complet (avec musique), ou None."""
        if self.category != Category.ORPHAN:
            return None
        for f in self.folders:
            if f.media_count > 0:
                return f
        return None


@dataclass
class DiagnoseResult:
    """Résultat complet du diagnostic."""

    total_folders: int = 0
    groups: list[DuplicateFolderGroup] = field(default_factory=list)

    @property
    def orphan_groups(self) -> list[DuplicateFolderGroup]:
        return [g for g in self.groups if g.category == Category.ORPHAN]

    @property
    def both_music_groups(self) -> list[DuplicateFolderGroup]:
        return [g for g in self.groups if g.category == Category.BOTH_MUSIC]


def find_duplicate_folders(
    conn: duckdb.DuckDBPyConnection,
    prefix: str = "Music/",
    depth: int = 3,
) -> DiagnoseResult:
    """Détecte les dossiers en doublon (même album, nommage différent).

    Analyse les dossiers au niveau `depth` segments de profondeur
    sous le préfixe donné.
    """
    folders = _get_distinct_folders(conn, prefix, depth)
    result = DiagnoseResult(total_folders=len(folders))

    groups_by_base = _group_by_base_name(folders, depth)

    for base, variants in sorted(groups_by_base.items()):
        if len(variants) < 2:
            continue

        folder_infos = [_analyze_folder(conn, v) for v in variants]
        category = _classify(folder_infos)
        result.groups.append(DuplicateFolderGroup(
            base_name=base,
            category=category,
            folders=folder_infos,
        ))

    return result


def format_report(result: DiagnoseResult, fmt: str = "table") -> str:
    """Génère le rapport de diagnostic."""
    if fmt == "table":
        return _format_table(result)
    if fmt == "json":
        return _format_json(result)
    if fmt == "csv":
        return _format_csv(result)
    return _format_table(result)


def _get_distinct_folders(
    conn: duckdb.DuckDBPyConnection,
    prefix: str,
    depth: int,
) -> list[str]:
    """Récupère les dossiers distincts à la profondeur donnée."""
    parts = " || '/' || ".join(
        f"split_part(key, '/', {i})" for i in range(1, depth + 1)
    )
    rows = conn.execute(
        f"SELECT DISTINCT {parts} as folder FROM objects "  # noqa: S608
        f"WHERE key LIKE ? AND split_part(key, '/', {depth}) != ''",
        [prefix + "%"],
    ).fetchall()
    return sorted(r[0] for r in rows)


def _group_by_base_name(
    folders: list[str],
    depth: int,
) -> dict[str, list[str]]:
    """Groupe les dossiers par nom de base (sans suffixe [ID] [année])."""
    groups: dict[str, list[str]] = {}
    for f in folders:
        parts = f.split("/")
        album_part = parts[depth - 1] if len(parts) >= depth else ""
        m = _ID_SUFFIX_RE.match(album_part)
        if m:
            base = "/".join(parts[: depth - 1]) + "/" + m.group(1).rstrip()
        else:
            base = f
        groups.setdefault(base, []).append(f)
    return groups


def _analyze_folder(
    conn: duckdb.DuckDBPyConnection,
    folder: str,
) -> FolderInfo:
    """Calcule les statistiques d'un dossier."""
    prefix = folder + "/"
    rows = conn.execute(
        "SELECT key, size FROM objects WHERE key LIKE ? || '%'",
        [prefix],
    ).fetchall()

    info = FolderInfo(path=folder, file_count=len(rows))
    for key, size in rows:
        lower = key.lower()
        info.total_size += size
        if any(lower.endswith(ext) for ext in MEDIA_EXTENSIONS):
            info.media_count += 1
        elif any(lower.endswith(ext) for ext in IMAGE_EXTENSIONS):
            info.image_count += 1
    return info


def _classify(folders: list[FolderInfo]) -> Category:
    """Détermine la catégorie d'un groupe de dossiers."""
    has_music = [f for f in folders if f.media_count > 0]
    no_music = [f for f in folders if f.media_count == 0]
    if no_music and has_music:
        return Category.ORPHAN
    return Category.BOTH_MUSIC


def _format_table(result: DiagnoseResult) -> str:
    """Format tableau lisible."""
    lines: list[str] = []
    lines.append(
        f"Diagnostic : {len(result.groups)} groupes de dossiers en doublon"
        f" sur {result.total_folders} dossiers analysés"
    )
    lines.append("")

    orphans = result.orphan_groups
    if orphans:
        lines.append(
            f"## Catégorie A — Orphelins (covers seulement) : {len(orphans)}"
        )
        lines.append("  Suppression safe : le dossier complet existe à côté.")
        lines.append("")
        for g in orphans:
            o = g.orphan
            c = g.complete
            if o and c:
                lines.append(
                    f"  ✗ {o.path}/ ({o.file_count} fichiers,"
                    f" {o.total_size // 1024}Ko)"
                )
                lines.append(
                    f"  ✓ {c.path}/ ({c.file_count} fichiers,"
                    f" {c.total_size // 1024 // 1024}Mo)"
                )
                lines.append("")

    both = result.both_music_groups
    if both:
        lines.append(
            f"## Catégorie B — Les deux contiennent de la musique :"
            f" {len(both)}"
        )
        lines.append("  Nécessite une analyse manuelle.")
        lines.append("")
        for g in both:
            lines.append(f"  {g.base_name}")
            for f in sorted(g.folders, key=lambda x: x.media_count, reverse=True):
                size_mo = f.total_size // 1024 // 1024
                lines.append(
                    f"    {f.path}/ ({f.media_count} audio,"
                    f" {f.file_count} total, {size_mo}Mo)"
                )
            lines.append("")

    return "\n".join(lines)


def _format_json(result: DiagnoseResult) -> str:
    """Format JSON."""
    import json

    data = {
        "total_folders": result.total_folders,
        "duplicate_groups": len(result.groups),
        "groups": [],
    }
    for g in result.groups:
        data["groups"].append({
            "base_name": g.base_name,
            "category": g.category.value,
            "folders": [
                {
                    "path": f.path,
                    "file_count": f.file_count,
                    "media_count": f.media_count,
                    "image_count": f.image_count,
                    "total_size": f.total_size,
                }
                for f in g.folders
            ],
        })
    return json.dumps(data, indent=2, ensure_ascii=False)


def generate_orphan_script(
    result: DiagnoseResult,
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    output: str = "delete_orphans.sh",
    endpoint_url: str | None = None,
) -> str:
    """Génère un script bash de suppression des dossiers dupliqués.

    Catégorie A (orphelins) : commandes actives.
    Catégorie B (les deux contiennent de la musique) : commandes commentées.
    Retourne le contenu du script.
    """
    orphans = result.orphan_groups
    both_music = result.both_music_groups
    lines: list[str] = []

    total_files = 0
    total_size = 0
    for g in orphans:
        o = g.orphan
        if o:
            total_files += o.file_count
            total_size += o.total_size

    lines.append("#!/usr/bin/env bash")
    lines.append("# Script de suppression des dossiers dupliqués")
    lines.append(f"# Bucket : {bucket}")
    lines.append(
        f"# Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    lines.append(f"# Dossiers orphelins (catégorie A) : {len(orphans)}")
    lines.append(
        f"# Doublons avec musique (catégorie B) : {len(both_music)}"
        " — commentés, à vérifier manuellement"
    )
    lines.append(f"# Fichiers à supprimer (cat. A) : {total_files}")
    lines.append(f"# Espace récupérable (cat. A) : {human_size(total_size)}")
    lines.append("#")
    lines.append("# ATTENTION : Vérifiez ce script avant exécution !")
    lines.append("# Les suppressions S3 sont IRRÉVERSIBLES.")
    lines.append("#")
    lines.append("")
    lines.append("set -euo pipefail")
    lines.append("")
    lines.append("# Dry-run : bash delete_orphans.sh --dryrun")
    lines.append('DRY_RUN=""')
    lines.append('if [[ "${1:-}" == "--dryrun" ]]; then')
    lines.append('  DRY_RUN="--dryrun"')
    lines.append('  echo "Mode dry-run : aucune suppression effective."')
    lines.append("fi")
    if endpoint_url:
        lines.append(f'ENDPOINT="--endpoint-url {endpoint_url}"')
    else:
        lines.append('ENDPOINT=""')
    lines.append("")

    if not orphans and not both_music:
        lines.append("echo 'Aucun dossier dupliqué détecté.'")
        content = "\n".join(lines) + "\n"
        _write_script(output, content)
        return content

    group_num = 0

    for group in orphans:
        orphan = group.orphan
        complete = group.complete
        if not orphan or not complete:
            continue

        group_num += 1
        path_escaped = orphan.path.replace("'", "'\\''")
        lines.append(f"# --- Groupe {group_num} : {group.base_name}")
        lines.append(
            f"# Orphelin  : {orphan.path}/"
            f" ({orphan.file_count} fichiers, {human_size(orphan.total_size)})"
        )
        lines.append(
            f"# Complet   : {complete.path}/"
            f" ({complete.file_count} fichiers,"
            f" {human_size(complete.total_size)})"
        )
        lines.append(
            f"aws s3 rm ${{DRY_RUN:-}} $ENDPOINT --recursive"
            f" 's3://{bucket}/{path_escaped}/'"
        )
        lines.append("")

    if both_music:
        lines.append(
            "# " + "=" * 60
        )
        lines.append(
            "# CATÉGORIE B — Les deux dossiers contiennent de la musique."
        )
        lines.append(
            "# Décommentez le dossier à supprimer après vérification."
        )
        lines.append(
            "# " + "=" * 60
        )
        lines.append("")

        for group in both_music:
            group_num += 1
            lines.append(f"# --- Groupe {group_num} : {group.base_name}")
            for f in sorted(
                group.folders, key=lambda x: x.media_count, reverse=True,
            ):
                size = human_size(f.total_size)
                path_escaped = f.path.replace("'", "'\\''")
                lines.append(
                    f"#   {f.path}/"
                    f" ({f.media_count} audio,"
                    f" {f.file_count} total, {size})"
                )
                lines.append(
                    f"# aws s3 rm ${{DRY_RUN:-}} $ENDPOINT --recursive"
                    f" 's3://{bucket}/{path_escaped}/'"
                )
            lines.append("")

    lines.append('if [[ -n "$DRY_RUN" ]]; then')
    lines.append("  echo 'Dry-run terminé.'")
    lines.append("else")
    lines.append("  echo 'Terminé.'")
    lines.append("fi")

    content = "\n".join(lines) + "\n"
    _write_script(output, content)
    return content


def _write_script(path: str, content: str) -> None:
    """Écrit le script et le rend exécutable."""
    with open(path, "w") as f:
        f.write(content)
    os.chmod(path, 0o755)


def _format_csv(result: DiagnoseResult) -> str:
    """Format CSV."""
    lines = ["category,base_name,folder_path,file_count,media_count,total_size"]
    for g in result.groups:
        for f in g.folders:
            lines.append(
                f"{g.category.value},{g.base_name},{f.path},"
                f"{f.file_count},{f.media_count},{f.total_size}"
            )
    return "\n".join(lines) + "\n"
