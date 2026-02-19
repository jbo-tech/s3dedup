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

### Objets S3 de taille 0 (marqueurs de dossier)
**Problem**: Le rapport contenait 138 faux doublons — tous les marqueurs de dossier (size=0, ETag=MD5 vide)
**Cause**: S3-compatible services créent des objets 0 octets avec `/` final pour représenter les dossiers. Même taille + même ETag → détectés comme doublons.
**Solution**: Filtrer `obj["Size"] == 0` au scan. Les objets vides ne sont jamais des doublons pertinents.
**Date**: 2026-02-16

### Script généré sans endpoint-url pour S3-compatible
**Problem**: `aws s3 rm` dans le script échouait avec `InvalidAccessKeyId` sur Mega.io
**Cause**: Le script bash généré n'incluait pas `--endpoint-url`, donc `aws` essayait de contacter AWS au lieu de Mega.io
**Solution**: Propager `endpoint_url` du CLI vers `generate_delete_script()` et l'inclure comme variable `$ENDPOINT` dans le script
**Date**: 2026-02-16

### Options click globales vs options de commande
**Problem**: L'utilisateur passait `--endpoint-url` après la commande (`generate-script --endpoint-url ...`) → erreur "No such option"
**Cause**: Avec click, les options du groupe parent doivent être placées **avant** le nom de la sous-commande
**Solution**: Documenter l'ordre : `s3dedup --endpoint-url URL command --options`. Considérer à terme de dupliquer l'option sur chaque commande.
**Date**: 2026-02-16

### mutagen.File() se fie à l'extension du fichier
**Problem**: Un fichier MP3 renommé en `.flac` provoquait `FLACNoHeaderError` dans mutagen, cassant silencieusement l'extraction de métadonnées
**Cause**: `mutagen.File()` utilise l'extension pour choisir le parser. Si l'extension ne correspond pas au contenu réel, le parsing échoue.
**Solution**: Attraper l'exception dans `_parse_tags()` et retourner un `MediaMetadata` vide. En test, utiliser des extensions cohérentes avec le contenu.
**Date**: 2026-02-19

### Changement de format CSV = tests cassés en cascade
**Problem**: Renommer les colonnes CSV (`group_fingerprint` → `group_id`) a cassé des tests dans `test_cli.py` en plus de `test_reporter.py`
**Cause**: Les tests CLI vérifiaient le contenu du CSV par assertion sur les noms de colonnes, couplage entre tests et format de sortie
**Solution**: Penser à grep tous les tests qui dépendent du format de sortie avant un changement de schema. Un `rg "group_fingerprint" tests/` aurait suffi.
**Date**: 2026-02-19
