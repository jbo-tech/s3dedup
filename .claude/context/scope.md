# Scope — s3dedup

## Vision
CLI Python pour détecter les doublons byte-identiques dans un bucket S3 (médiathèque ~9 To). Rapport + script de suppression reviewable.

## Scope
**In**: scan 3 passes (taille → ETag → hash multipart), index DuckDB, rapport JSON/CSV, script bash de suppression, CLI (scan/report/generate-script)
**Out**: suppression directe, inter-buckets, fingerprinting média, normalisation noms, versioning S3, GUI

## Components
- Scanner: listing S3 paginé → DuckDB
- Analyzer: détection doublons (GROUP BY size → etag → hash)
- Hasher: SHA256 streaming (passe 3, multipart uniquement)
- Reporter: rapport JSON/CSV
- ScriptGenerator: script bash reviewable

## CLI
- `s3dedup scan --bucket <name> [--prefix <prefix>]`
- `s3dedup report --format json|csv`
- `s3dedup generate-script --keep oldest|newest|largest`

## Open questions
1. Préfixe par défaut (tout le bucket ?)
2. Politiques de rétention supplémentaires (--keep-prefix ?)
3. Format script (bash seul ou aussi JSON ?)
