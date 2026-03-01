# Status

## Objective
Outil CLI Python pour détecter les objets dupliqués dans un bucket S3. Au-delà de la déduplication byte-identique : normalisation des noms, extraction de métadonnées média, politique de rétention enrichie, nettoyage des clés.

## Current focus
Commande `clean` testée sur le bucket réel. Fix `--copy-props metadata-directive` appliqué pour compatibilité Mega S4.

## Log

### 2026-03-01 (session 11)
- Done:
  - Diagnostic erreur `GetObjectTagging` sur Mega S4 lors de `aws s3 mv`
  - Test réel avec `--copy-props metadata-directive` sur fichier `TV/ test.txt` → succès
  - Fix appliqué dans `cleaner.py` : ajout `--copy-props metadata-directive` aux commandes `aws s3 mv` générées
  - 20 tests cleaner OK
- Next:
  - Régénérer `clean.sh` et relancer sur le bucket réel
  - Workflow complet : `scan` → `clean` → `scan` → `report` / `generate-script`
  - Ajouter d'autres règles de nettoyage si besoin (unicode normalization, etc.)

### 2026-03-01 (session 10)
- Done:
  - Commande `clean` : génère un script bash de renommage (`aws s3 mv`) pour nettoyer les clés S3
  - Architecture extensible par règles : `CleanRule` ABC + `StripSpacesRule` (espaces début/fin par segment)
  - Détection et résolution de conflits : suffixage automatique (`_2`, `_3`...) si la cible existe déjà
  - `get_all_keys()` ajouté à `db.py`
  - 20 tests dédiés dans `test_cleaner.py`
  - CLAUDE.md mis à jour (section Commands)
  - 175 tests OK, ruff clean
- Next:
  - Tester `clean` sur le bucket réel (Mega.io)
  - Workflow complet : `scan` → `clean` → `scan` → `report` / `generate-script`
  - Ajouter d'autres règles de nettoyage si besoin (unicode normalization, etc.)

### 2026-02-21 (session 9)
- Done:
  - Persistance endpoint URL : table `bucket_config`, auto-fallback dans `generate-script`
  - Fix dry-run : parse `$1` au lieu de variable commentée
  - Fix pagination Mega.io : pagination manuelle remplaçant le paginateur boto3 (détection token dupliqué)
  - README mis à jour : section Reset, Database, dry-run simplifié
  - 2 commits poussés (feat + chore context)
  - 160 tests OK, ruff clean
- Next:
  - Relancer le scan sur Music-Various-Artists/ avec le fix pagination
  - Tester `--extract-metadata` sur le bucket réel
  - Mettre à jour scope.md

### 2026-02-21 (session 8)
- Done:
  - Fix dry-run : le script parse maintenant `$1` (`bash delete.sh --dryrun`), plus besoin de décommenter une variable
  - Message final adapté au mode (dry-run vs réel)
  - README mis à jour : section Reset, section Database (3 tables), doc endpoint persisté, dry-run simplifié
  - 160 tests OK, ruff clean

### 2026-02-21 (session 7)
- Done:
  - Table `bucket_config` dans DuckDB (bucket → endpoint_url)
  - `set_bucket_config()` (upsert, retourne l'ancien endpoint si changé) et `get_bucket_config()`
  - `scan` persiste automatiquement l'endpoint après le scan
  - `generate-script` fallback sur l'endpoint stocké si `--endpoint-url` non fourni
  - Warning CLI si l'endpoint change entre deux scans
  - 6 tests unitaires + 1 test e2e
  - 160 tests OK, ruff clean

### 2026-02-21 (session 6)
- Done:
  - Refactoring CLI : `--endpoint-url` déplacé du groupe vers les sous-commandes `scan` et `generate-script`
  - Suppression de `@click.pass_context` / `ctx` (plus nécessaire)
  - Les options sont maintenant librement ordonnées après la sous-commande
  - 150 tests OK, ruff clean

### 2026-02-19 (session 5)
- Done:
  - T0 : Schema `media_metadata` dans DuckDB, dataclass `MediaMetadata`, `MEDIA_EXTENSIONS`, fonctions `upsert_media_metadata()` et `find_metadata_groups()`
  - T1 : Module `normalizer.py` (`normalize_name()`, `name_quality_score()`), rapport "noms suspects" dans les 3 formats, critère `--keep cleanest`
  - T2 : Module `media.py` (extraction tags via range GET 256Ko + mutagen), `extract_all_media_metadata()` dans scanner, dépendance `mutagen>=1.47`
  - T3 : Section "Même œuvre, encodage différent" dans les 3 formats de rapport (table, JSON, CSV)
  - T4 : `scan --extract-metadata`, aide `--keep` mise à jour, test e2e complet
  - Format CSV refactoré : colonnes `section, group_id, group_size, object_key, detail`
  - 150 tests (+71), ruff clean

### 2026-02-16 (session 4)
- Done:
  - Fix : `--endpoint-url` propagé dans le script bash généré (les `aws s3 rm` utilisent maintenant `$ENDPOINT`)
  - README mis à jour : documentation du dry-run, syntaxe --keep, formats de rapport
- Note: `--endpoint-url` est une option globale (avant la commande, pas après)
- Next: Scan complet du bucket media-center, dry-run du script, puis suppression

### 2026-02-16 (session 3)
- Done: Fix objets 0 octets, rapport table rich, politique multi-critères `--keep shortest,oldest`
- 79 tests, ruff clean

### 2026-02-16 (session 2)
- Done: --endpoint-url, README, suggestions d'étapes suivantes

### 2026-02-16 (session 1)
- Done: MVP complet (T1→T6), 65 tests
