# Anti-patterns

Errors encountered and how to avoid them. Added via `/retro`.

<!-- Format:
### [Short title]
**Problem**: What went wrong
**Cause**: Why it happened
**Solution**: How to fix/avoid
**Date**: YYYY-MM-DD
-->

### Ordre des arguments positionnels dans les dataclasses
**Problem**: Tests CLI échouaient avec `NOT NULL constraint failed: objects.last_modified` — `sha256` et `last_modified` étaient inversés
**Cause**: `ObjectInfo` a `sha256` comme dernier champ (avec default), mais les tests passaient les args positionnellement dans le mauvais ordre
**Solution**: Toujours utiliser des keyword arguments pour les dataclasses avec des champs optionnels, ou vérifier l'ordre dans la définition
**Date**: 2026-02-16
