# Scope — s3dedup

## Vision
CLI Python pour détecter les doublons dans un bucket S3 (médiathèque ~9 To). Déduplication byte-identique + normalisation des noms + métadonnées média. Rapport + script de suppression reviewable.

## Scope
**In**: scan 3 passes (taille → ETag → hash multipart), index DuckDB, rapport JSON/CSV/table, script bash de suppression, CLI (scan/report/generate-script), normalisation des noms, rapport noms suspects, extraction métadonnées média (opt-in), rapport "même œuvre", critère `--keep cleanest`
**Out**: suppression directe, inter-buckets, fingerprinting perceptuel (Chromaprint), versioning S3, GUI

## Components
- Scanner: listing S3 paginé → DuckDB + extraction métadonnées média (opt-in)
- Analyzer: détection doublons (GROUP BY size → etag → hash)
- Hasher: SHA256 streaming (passe 3, multipart uniquement)
- Normalizer: normalisation des noms, scoring de qualité
- Media: extraction tags ID3/MP4 via range GET + mutagen
- Reporter: rapport JSON/CSV/table (doublons + noms suspects + même œuvre)
- ScriptGenerator: script bash reviewable

## CLI
- `s3dedup scan --bucket <name> [--prefix <prefix>] [--extract-metadata]`
- `s3dedup report --format table|json|csv`
- `s3dedup generate-script --keep shortest|oldest|newest|cleanest`

## Open questions
1. Couverture métadonnées sur la médiathèque réelle (combien de fichiers ont des tags ?)
2. Faut-il un rapport de couverture des métadonnées ?
