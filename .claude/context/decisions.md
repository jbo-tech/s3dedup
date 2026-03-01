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

### Support endpoint S3-compatible (--endpoint-url)
**Decision**: Option `--endpoint-url` sur chaque sous-commande qui accède à S3 (`scan`, `generate-script`) + support envvar `AWS_ENDPOINT_URL`
**Context**: Le bucket cible est sur Mega.io (S3-compatible), pas AWS. Initialement placée sur le groupe CLI, déplacée sur les sous-commandes pour permettre un ordre libre des options (session 6).
**Alternatives considered**: Forcer l'usage de LocalStack pour les tests locaux uniquement ; garder sur le groupe + dupliquer sur les sous-commandes (risque de conflit)
**Date**: 2026-02-16 (mise à jour 2026-02-21)

### Politique de rétention multi-critères
**Decision**: `--keep` accepte une liste de critères séparés par virgules (ex: `shortest,oldest`). Tri multi-clés, le premier critère est prioritaire.
**Context**: Les copies de conflit (`file_1.jpg`, `file_2.jpg`) ne sont pas bien gérées par `oldest` seul. Le nom le plus court est presque toujours l'original. Défaut: `shortest,oldest`.
**Alternatives considered**: `--keep shortest-name` comme critère unique, regex sur suffixe `_N`
**Date**: 2026-02-16

### Format table rich comme défaut du rapport
**Decision**: `--format table` (défaut) avec panneau résumé + tableau rich. JSON et CSV restent disponibles.
**Context**: Le rapport CSV/JSON brut est illisible dans le terminal. Rich est déjà en dépendance.
**Alternatives considered**: HTML standalone (surengineering), résumé seul sans tableau
**Date**: 2026-02-16

### Normalisation des noms : rapport consultatif, pas un filtre
**Decision**: La normalisation génère un rapport "noms suspects" (même nom normalisé, contenu différent). Ce n'est pas un critère de déduplication.
**Context**: Faux positifs inévitables (`vacances_2024.jpg` ≠ `vacances_2025.jpg`). Un rapport consultatif laisse l'humain décider.
**Alternatives considered**: Pré-filtre avant comparaison binaire (risque de faux positifs automatisés)
**Date**: 2026-02-19

### mutagen pour les métadonnées média
**Decision**: `mutagen>=1.47` pour lire les tags ID3/MP4/FLAC/OGG. Pur Python, pas de dépendance système.
**Context**: Pas besoin de ffmpeg. mutagen couvre tous les formats cibles. Extraction via range GET des premiers 256 Ko.
**Alternatives considered**: ffprobe (nécessite ffmpeg installé), tinytag (moins de formats supportés)
**Date**: 2026-02-19

### Extraction métadonnées opt-in (--extract-metadata)
**Decision**: L'extraction des tags média est activée par flag `--extract-metadata` sur la commande `scan`, pas par défaut.
**Context**: Télécharger 256 Ko par fichier média sur 9 To est coûteux en bande passante. Séparation scan rapide (listing) vs enrichissement.
**Alternatives considered**: Commande séparée `s3dedup enrich` (plus explicite mais plus de code CLI), extraction par défaut (trop coûteux)
**Date**: 2026-02-19

### Table media_metadata séparée de objects
**Decision**: Table DuckDB `media_metadata` avec FK sur `objects.key`, plutôt que des colonnes nullable sur `objects`.
**Context**: Seuls les fichiers média ont des métadonnées. Des colonnes nullable sur `objects` pollueraient le schema pour tous les fichiers.
**Alternatives considered**: Colonnes nullable sur `objects` (plus simple mais schema dilué)
**Date**: 2026-02-19

### Persistance de l'endpoint URL par bucket
**Decision**: Table `bucket_config` (bucket → endpoint_url) remplie au `scan`, réutilisée en fallback par `generate-script`
**Context**: L'utilisateur oubliait de repasser `--endpoint-url` à `generate-script`, ce qui causait des erreurs `InvalidAccessKeyId` sur S3-compatible. L'endpoint est une propriété du bucket, pas de la commande.
**Alternatives considered**: Variable d'environnement obligatoire (pas persistant entre sessions), fichier de config YAML (surengineering pour un seul champ)
**Date**: 2026-02-21

### Pagination manuelle pour S3-compatible
**Decision**: Remplacer `paginator.paginate()` par une boucle manuelle `_list_objects_pages()` avec détection de token dupliqué.
**Context**: Mega.io renvoie le même `NextContinuationToken` deux fois, crashant le paginateur boto3. La pagination manuelle permet de s'arrêter proprement et de compléter via scans incrémentaux.
**Alternatives considered**: Catch de l'exception du paginateur (perd les objets du dernier batch non flushé), patch du paginateur boto3 (trop invasif)
**Date**: 2026-02-21

### Commande clean : architecture extensible par règles
**Decision**: Architecture à base de `CleanRule` ABC avec registre de règles nommées. Première règle : `StripSpacesRule`. Script bash de renommage (`aws s3 mv`), jamais de renommage direct.
**Context**: Les clés S3 avec espaces parasites causent des confusions. Le workflow est `scan` → `clean` → `scan` → `report`/`generate-script`. L'architecture par règles permet d'ajouter facilement d'autres nettoyages (unicode normalization, etc.) sans modifier le moteur.
**Alternatives considered**: Commande monolithique avec logique en dur (pas extensible), renommage direct via l'API S3 (irréversible, pas reviewable)
**Date**: 2026-03-01

### Critère --keep cleanest basé sur name_quality_score
**Decision**: Score de qualité du nom (0=parfait), avec pénalités : mojibake (+10), suffixe de copie (+5), espaces parasites (+2).
**Context**: Sur une médiathèque, les copies dégradées ont souvent des noms cassés. Le score permet de garder automatiquement le "meilleur" nom.
**Alternatives considered**: Regex unique pour détecter les copies (trop rigide), critère binaire bon/mauvais (perd la nuance)
**Date**: 2026-02-19
