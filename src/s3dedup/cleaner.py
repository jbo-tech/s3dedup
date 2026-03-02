"""Cleaner — génération de scripts de renommage pour nettoyer les clés S3."""

from __future__ import annotations

import os
import posixpath
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

import duckdb

from s3dedup.db import get_all_keys


@dataclass
class CleanStats:
    """Bilan d'une opération de nettoyage."""

    total_keys: int = 0
    rename_count: int = 0
    per_rule: dict[str, int] = field(default_factory=dict)


class CleanRule(ABC):
    """Interface pour une règle de nettoyage de clés S3."""

    name: str
    description: str

    @abstractmethod
    def apply(self, key: str) -> str | None:
        """Retourne la clé nettoyée, ou None si rien à changer."""


class StripSpacesRule(CleanRule):
    """Supprime les espaces en début/fin de chaque segment du chemin."""

    name = "strip-spaces"
    description = "Supprime les espaces début/fin dans chaque segment du chemin"

    def apply(self, key: str) -> str | None:
        """Retourne la clé nettoyée, ou None si rien à changer."""
        segments = key.split("/")
        cleaned = [s.strip() for s in segments]
        # Filtrer les segments vides (causés par des espaces-seulement)
        cleaned = [s for s in cleaned if s]
        result = "/".join(cleaned)
        if result != key:
            return result
        return None


# Registre des règles disponibles
AVAILABLE_RULES: dict[str, type[CleanRule]] = {
    "strip-spaces": StripSpacesRule,
}


def _resolve_conflicts(
    renames: dict[str, str],
    existing_keys: set[str],
) -> dict[str, str]:
    """Résout les conflits quand plusieurs clés pointent vers la même cible.

    Suffixe les cibles en conflit avec _2, _3, etc.
    Retourne un dict source → (cible, commentaire ou None).
    """
    # Compter les cibles déjà prises (clés existantes qui ne sont pas renommées)
    taken: set[str] = existing_keys - set(renames.keys())
    result: dict[str, str] = {}
    # Grouper par cible
    target_sources: dict[str, list[str]] = {}
    for src, tgt in renames.items():
        target_sources.setdefault(tgt, []).append(src)

    for target, sources in target_sources.items():
        for i, src in enumerate(sorted(sources)):
            candidate = target
            if i > 0 or candidate in taken:
                # Besoin de suffixer
                candidate = _suffixed(target, taken)
            taken.add(candidate)
            result[src] = candidate

    return result


def _suffixed(target: str, taken: set[str]) -> str:
    """Ajoute un suffixe _2, _3, etc. au nom de fichier jusqu'à trouver un libre."""
    root, ext = posixpath.splitext(target)
    n = 2
    while True:
        candidate = f"{root}_{n}{ext}"
        if candidate not in taken:
            return candidate
        n += 1


def generate_clean_script(
    conn: duckdb.DuckDBPyConnection,
    bucket: str,
    rules: list[str] | None = None,
    prefix: str = "",
    output: str = "clean.sh",
    endpoint_url: str | None = None,
) -> CleanStats:
    """Génère un script bash de renommage des clés S3.

    Retourne les statistiques de l'opération.
    """
    if rules is None:
        rules = ["strip-spaces"]

    # Instancier les règles
    active_rules = _instantiate_rules(rules)

    # Récupérer toutes les clés
    all_keys = get_all_keys(conn, prefix=prefix)
    existing_keys = set(all_keys)

    stats = CleanStats(total_keys=len(all_keys))

    # Appliquer les règles et compter par règle
    renames: dict[str, str] = {}
    for key in all_keys:
        cleaned = key
        for rule in active_rules:
            result = rule.apply(cleaned)
            if result is not None:
                cleaned = result
                stats.per_rule[rule.name] = stats.per_rule.get(rule.name, 0) + 1
        if cleaned != key:
            renames[key] = cleaned

    stats.rename_count = len(renames)

    if not renames:
        content = _build_script_no_rename(bucket, endpoint_url)
        _write_file(output, content)
        return stats

    # Résoudre les conflits
    resolved = _resolve_conflicts(renames, existing_keys)

    # Générer le script
    content = _build_script(bucket, resolved, renames, endpoint_url, existing_keys)
    _write_file(output, content)
    return stats


def _instantiate_rules(rule_names: list[str]) -> list[CleanRule]:
    """Instancie les règles par nom, lève une erreur si inconnue."""
    import click

    result = []
    for name in rule_names:
        cls = AVAILABLE_RULES.get(name)
        if cls is None:
            valid = ", ".join(sorted(AVAILABLE_RULES.keys()))
            raise click.BadParameter(
                f"Règle inconnue : {name}. Valides : {valid}"
            )
        result.append(cls())
    return result


def _build_script_no_rename(
    bucket: str,
    endpoint_url: str | None,
) -> str:
    """Génère un script minimal quand aucun renommage n'est nécessaire."""
    lines = _header(bucket, 0, endpoint_url)
    lines.append("echo 'Aucun renommage nécessaire.'")
    return "\n".join(lines) + "\n"


def _collect_empty_dirs(
    resolved: dict[str, str],
    all_keys: set[str],
) -> list[str]:
    """Identifie les préfixes (dossiers) qui deviennent vides après les renommages.

    Retourne la liste triée du plus profond au moins profond.
    """
    # Simuler l'état final : clés non déplacées + nouvelles cibles
    moved_sources = set(resolved.keys())
    remaining = (all_keys - moved_sources) | set(resolved.values())

    # Collecter tous les répertoires parents des clés sources déplacées
    source_dirs: set[str] = set()
    for src in resolved:
        parts = src.split("/")
        # Accumuler tous les niveaux de répertoire
        for i in range(1, len(parts)):
            source_dirs.add("/".join(parts[:i]))

    # Ne garder que ceux qui n'ont plus aucun fichier en-dessous
    empty_dirs = []
    for d in source_dirs:
        prefix = d + "/"
        if not any(k.startswith(prefix) for k in remaining):
            empty_dirs.append(d)

    # Trier du plus profond au moins profond
    empty_dirs.sort(key=lambda d: d.count("/"), reverse=True)
    return empty_dirs


def _build_script(
    bucket: str,
    resolved: dict[str, str],
    original_renames: dict[str, str],
    endpoint_url: str | None,
    all_keys: set[str] | None = None,
) -> str:
    """Génère le script bash complet de renommage."""
    lines = _header(bucket, len(resolved), endpoint_url)

    total = len(resolved)
    lines.append(f"TOTAL={total}")
    lines.append("COUNT=0")
    lines.append("")

    for src in sorted(resolved.keys()):
        tgt = resolved[src]
        original_tgt = original_renames[src]
        src_escaped = src.replace("'", "'\\''")
        tgt_escaped = tgt.replace("'", "'\\''")

        # Commenter si le conflit a été résolu (cible différente de l'attendue)
        if tgt != original_tgt:
            lines.append(
                f"# Conflit résolu : '{original_tgt}' existe déjà"
                f" → renommé en '{tgt}'"
            )

        lines.append("COUNT=$((COUNT + 1))")
        lines.append(
            f"echo \"[$COUNT/$TOTAL] '{src_escaped}'"
            f" → '{tgt_escaped}'\""
        )
        lines.append(
            f"aws s3 mv --copy-props metadata-directive"
            f" ${{DRY_RUN:-}} $ENDPOINT"
            f" 's3://{bucket}/{src_escaped}'"
            f" 's3://{bucket}/{tgt_escaped}'"
        )

    # Nettoyage des dossiers vides
    if all_keys is not None:
        empty_dirs = _collect_empty_dirs(resolved, all_keys)
        if empty_dirs:
            lines.append("")
            lines.append("# Suppression des dossiers vides")
            for d in empty_dirs:
                d_escaped = d.replace("'", "'\\''")
                lines.append(
                    f"aws s3 rm ${{DRY_RUN:-}} $ENDPOINT"
                    f" 's3://{bucket}/{d_escaped}/'"
                )

    lines.append("")
    lines.append('if [[ -n "$DRY_RUN" ]]; then')
    lines.append(
        f"  echo 'Dry-run terminé : {len(resolved)} objets à renommer.'"
    )
    lines.append("else")
    lines.append(
        f"  echo 'Terminé : {len(resolved)} objets renommés.'"
    )
    lines.append("fi")

    return "\n".join(lines) + "\n"


def _header(
    bucket: str,
    rename_count: int,
    endpoint_url: str | None,
) -> list[str]:
    """Génère l'en-tête commune du script."""
    lines = [
        "#!/usr/bin/env bash",
        "# Script de nettoyage des clés S3",
        f"# Bucket : {bucket}",
        f"# Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"# Renommages : {rename_count}",
        "#",
        "# ATTENTION : Vérifiez ce script avant exécution !",
        "#",
        "",
        "set -euo pipefail",
        "",
        f"# Dry-run : bash {_script_name(rename_count)} --dryrun",
        'DRY_RUN=""',
        'if [[ "${1:-}" == "--dryrun" ]]; then',
        '  DRY_RUN="--dryrun"',
        '  echo "Mode dry-run : aucun renommage effectif."',
        "fi",
    ]
    if endpoint_url:
        lines.append(f'ENDPOINT="--endpoint-url {endpoint_url}"')
    else:
        lines.append('ENDPOINT=""')
    lines.append("")
    return lines


def _script_name(rename_count: int) -> str:
    """Nom par défaut du script (pour le commentaire dry-run)."""
    return "clean.sh"


def _write_file(path: str, content: str) -> None:
    """Écrit le script et le rend exécutable."""
    with open(path, "w") as f:
        f.write(content)
    os.chmod(path, 0o755)
