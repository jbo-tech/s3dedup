"""Utilitaires partagés."""


def human_size(size_bytes: int) -> str:
    """Convertit des bytes en format lisible."""
    for unit in ("o", "Ko", "Mo", "Go", "To"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} Po"
