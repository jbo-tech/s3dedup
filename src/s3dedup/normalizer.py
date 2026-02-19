"""Normalisation des noms de fichiers S3 et scoring de qualité."""

import posixpath
import re
import unicodedata

# Suffixes de copie courants (insensible à la casse).
# Chaque pattern matche le suffixe *avant* l'extension.
_COPY_SUFFIX_PATTERNS = [
    re.compile(r"\s*\(\d+\)$"),          # " (1)", "(2)"
    re.compile(r"\s*-\s*Copie$", re.I),  # " - Copie", "- copie"
    re.compile(r"\s*-\s*Copy$", re.I),   # " - Copy"
    re.compile(r"[_ ]copy$", re.I),      # "_copy", " copy"
    re.compile(r"_\d+$"),                # "_1", "_2"
]

# Séquences UTF-8 mal décodées (mojibake) typiques du latin-1→UTF-8.
# Ex: "Ã©" = é, "Ã¨" = è, "Ã " = à, "Ã´" = ô
_MOJIBAKE_PATTERN = re.compile(
    r"Ã[\x80-\xbf]|Ã[©¨ ´¹²³¼½¾]|"
    r"Â[\xa0-\xbf]|"
    r"Ã\x83Â"
)


def normalize_name(key: str) -> str:
    """Normalise un nom de fichier S3 pour comparaison.

    Opérations :
    - Extrait le basename (sans le chemin)
    - Sépare nom et extension
    - Lowercase
    - Supprime accents (NFD → strip combining marks)
    - Supprime les suffixes de copie
    - Strip espaces
    - Recombine nom + extension normalisée
    """
    basename = posixpath.basename(key)
    stem, ext = _split_ext(basename)

    # Lowercase
    stem = stem.lower()
    ext = ext.lower()

    # Supprimer accents : NFD → retirer les combining marks
    stem = _strip_accents(stem)

    # Supprimer les suffixes de copie
    stem = _strip_copy_suffixes(stem)

    # Strip espaces
    stem = stem.strip()

    # Normaliser les espaces multiples
    stem = re.sub(r"\s+", " ", stem)

    return stem + ext


def name_quality_score(key: str) -> int:
    """Score de qualité d'un nom (0 = parfait, plus élevé = pire).

    Pénalités :
    - +10 : encodage cassé (mojibake)
    - +5  : suffixe de copie détecté
    - +2  : espaces en début ou fin de nom
    - +1  : espaces multiples consécutifs
    """
    basename = posixpath.basename(key)
    stem, _ = _split_ext(basename)
    score = 0

    # Mojibake
    if _MOJIBAKE_PATTERN.search(stem):
        score += 10

    # Suffixes de copie
    if _has_copy_suffix(stem):
        score += 5

    # Espaces en début/fin
    if stem != stem.strip():
        score += 2

    # Espaces multiples consécutifs
    if re.search(r"  +", stem):
        score += 1

    return score


def _split_ext(basename: str) -> tuple[str, str]:
    """Sépare le nom de l'extension (gère les doubles extensions)."""
    # posixpath.splitext gère déjà bien les cas standards
    stem, ext = posixpath.splitext(basename)
    return stem, ext


def _strip_accents(text: str) -> str:
    """Retire les accents via décomposition Unicode NFD."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if unicodedata.category(c) != "Mn")


def _strip_copy_suffixes(stem: str) -> str:
    """Retire les suffixes de copie du nom (avant extension)."""
    for pattern in _COPY_SUFFIX_PATTERNS:
        stem = pattern.sub("", stem)
    return stem


def _has_copy_suffix(stem: str) -> bool:
    """Détecte si le nom contient un suffixe de copie."""
    for pattern in _COPY_SUFFIX_PATTERNS:
        if pattern.search(stem):
            return True
    return False
