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
**Solution**: ~~Documenter l'ordre~~ → Résolu en session 6 : option déplacée du groupe vers les sous-commandes. Plus de contrainte d'ordre.
**Date**: 2026-02-16 (résolu 2026-02-21)

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

### Script bash généré avec dry-run non fonctionnel
**Problem**: `bash delete.sh --dryrun` supprimait réellement les fichiers au lieu de simuler. Le message final disait "supprimés" dans tous les cas.
**Cause**: Le dry-run reposait sur une variable commentée à décommenter manuellement (`# DRY_RUN="--dryrun"`). L'argument `$1` du script n'était pas parsé. Personne ne va éditer le script pour activer le dry-run.
**Solution**: Parser `$1` dans le script (`if [[ "${1:-}" == "--dryrun" ]]`), adapter le message final au mode. L'UX doit être `bash script.sh --dryrun`, pas "éditez le fichier".
**Date**: 2026-02-21

### Paginateur boto3 et services S3-compatibles
**Problem**: Scan crashait à ~1000 objets avec `The same next token was received twice` sur Mega.io (2.7 To, prefix Music-Various-Artists/).
**Cause**: Le paginateur boto3 a une protection anti-boucle infinie : si le serveur renvoie le même `NextContinuationToken` deux fois, il lève une erreur. Mega.io fait exactement ça.
**Solution**: Pagination manuelle (`_list_objects_pages`) au lieu de `paginator.paginate()`. Détecte le token dupliqué et s'arrête proprement. Les scans suivants complèteront les manquants (incrémental).
**Date**: 2026-02-21

### aws s3 mv et GetObjectTagging sur S3-compatible
**Problem**: `aws s3 mv` échoue avec `NotImplemented` sur `GetObjectTagging` (Mega S4)
**Cause**: Par défaut, `aws s3 mv` copie toutes les propriétés (métadonnées + tags). L'opération `GetObjectTagging` n'est pas implémentée par tous les services S3-compatible.
**Solution**: Ajouter `--copy-props metadata-directive` pour copier les métadonnées (Content-Type, dates) sans les tags. Ne pas utiliser `--copy-props none` (trop agressif, perd les métadonnées).
**Date**: 2026-03-01

### Types numériques DuckDB trop étroits
**Problem**: Crash à mi-scan avec `Conversion Error: Type INT64 with value 6039889936 can't be cast because the value is out of range for the destination type INT32`
**Cause**: La colonne `bitrate` était typée `INTEGER` (INT32, max ~2.1G). Des fichiers vidéo haute qualité ont des bitrates > 4 Gbps.
**Solution**: Utiliser `BIGINT` par défaut pour toute colonne numérique susceptible de dépasser 2G. Prévoir une migration `ALTER TABLE ... ALTER ... TYPE BIGINT` pour les bases existantes (`CREATE TABLE IF NOT EXISTS` ne modifie pas le schema).
**Date**: 2026-03-03
