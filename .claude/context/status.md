# Status

## Objective
Outil CLI Python pour détecter les objets dupliqués dans un bucket S3. Au-delà de la déduplication byte-identique : normalisation des noms, extraction de métadonnées média, politique de rétention enrichie.

## Current focus
Features T0→T4 implémentées et testées (150 tests). Prêt pour test sur données réelles et mise à jour du README.

## Log

### 2026-02-19 (session 5)
- Done:
  - T0 : Schema `media_metadata` dans DuckDB, dataclass `MediaMetadata`, `MEDIA_EXTENSIONS`, fonctions `upsert_media_metadata()` et `find_metadata_groups()`
  - T1 : Module `normalizer.py` (`normalize_name()`, `name_quality_score()`), rapport "noms suspects" dans les 3 formats, critère `--keep cleanest`
  - T2 : Module `media.py` (extraction tags via range GET 256Ko + mutagen), `extract_all_media_metadata()` dans scanner, dépendance `mutagen>=1.47`
  - T3 : Section "Même œuvre, encodage différent" dans les 3 formats de rapport (table, JSON, CSV)
  - T4 : `scan --extract-metadata`, aide `--keep` mise à jour, test e2e complet
  - Format CSV refactoré : colonnes `section, group_id, group_size, object_key, detail`
  - 150 tests (+71), ruff clean
- Next:
  - Mettre à jour le README avec les nouvelles features
  - Tester `--extract-metadata` sur le bucket réel
  - Évaluer la couverture des métadonnées sur la médiathèque
  - Mettre à jour scope.md (normalisation et métadonnées sont désormais in-scope)

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
