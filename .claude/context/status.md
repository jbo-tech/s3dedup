# Status

## Objective
Outil CLI Python pour détecter les objets dupliqués (byte-identique) dans un bucket S3. Cas d'usage : médiathèque ~9 To sur Mega.io (S3-compatible).

## Current focus
MVP complet. Support endpoint S3-compatible ajouté. Prêt pour test réel sur Mega.io.

## Log

### 2026-02-16 (session 2)
- Done: Ajout --endpoint-url (option globale + envvar AWS_ENDPOINT_URL) pour les services S3-compatibles
- Context: Le bucket cible est sur Mega.io, pas AWS — nécessite un endpoint custom
- Next: Tester `uv run s3dedup --endpoint-url https://s3.eu-central-1.s4.mega.io scan --bucket media-center --prefix Music/`

### 2026-02-16 (session 1)
- Done: Implémentation complète du MVP (T1→T6)
  - Project setup, interfaces DuckDB, scanner, hasher, reporter, script generator, CLI
  - 65 tests passants, ruff clean
- Bootstrap, explore, scope, decompose
- Commits: `66c16f9` init, `0665417` feat: implement S3 deduplication CLI
