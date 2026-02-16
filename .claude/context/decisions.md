# Decisions

Technical decisions and their context. Added via `/retro`.

### CLI Python avec boto3
**Decision**: Python + boto3 pour l'accès S3, outil CLI
**Context**: Choix utilisateur — écosystème Python bien adapté pour le scripting AWS
**Alternatives considered**: Go, Rust
**Date**: 2026-02-16

### Déduplication byte-identique uniquement (MVP)
**Decision**: Pas de fingerprinting média (audio/vidéo), uniquement comparaison binaire
**Context**: Le fingerprinting (Chromaprint, perceptual hash) multiplie la complexité par 10. Le byte-identique couvre le cas principal (copies exactes sous des noms différents).
**Alternatives considered**: Fingerprinting audio/vidéo — reporté en extension future
**Date**: 2026-02-16

### Stratégie 3 passes : taille → ETag → hash
**Decision**: Filtrer par taille (gratuit), puis ETag (gratuit), puis hash complet uniquement pour les ETag multipart ambigus
**Context**: 9 To de données — impossible de tout télécharger. L'ETag non-multipart = MD5, fiable pour la comparaison. Optimise le coût et le temps.
**Alternatives considered**: Hash systématique (trop coûteux), ETag seul (pas fiable pour multipart)
**Date**: 2026-02-16

### DuckDB pour l'index local
**Decision**: DuckDB au lieu de SQLite pour stocker l'index des objets
**Context**: Meilleure perf analytique (GROUP BY sur millions de lignes), objectif d'apprentissage
**Alternatives considered**: SQLite (plus simple mais moins performant pour l'analytique)
**Date**: 2026-02-16

### Rapport + script de suppression (jamais de suppression directe)
**Decision**: L'outil génère un rapport (JSON/CSV) et un script bash de suppression reviewable. Aucune suppression automatique.
**Context**: 9 To de médias — la suppression est irréversible. Séparation détection/action.
**Alternatives considered**: Suppression interactive, suppression automatique avec policy
**Date**: 2026-02-16

### Authentification : chaîne boto3 par défaut
**Decision**: S'appuyer sur la résolution de credentials boto3 (env → config → SSO → instance profile). Pas de paramètres --access-key dans le CLI.
**Context**: Approche la plus sécurisée et la plus simple. Pas de cross-account.
**Alternatives considered**: Gestion custom des credentials
**Date**: 2026-02-16
