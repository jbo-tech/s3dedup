# Status

## Objective
Outil CLI Python pour dédupliquer des objets S3. Au-delà de la déduplication byte-identique : normalisation des noms, extraction de métadonnées média, politique de rétention enrichie, nettoyage des clés, diagnostic de dossiers en doublon.

## Current focus
Diagnostic et nettoyage des dossiers en doublon (même album, nommage différent). Commande `diagnose` implémentée, prochaine étape : traitement automatisé des cas identifiés.

## Log

### 2026-05-17 (session 16)
- Done:
  - Nouveau module `diagnose.py` : détection de dossiers en doublon (même album avec/sans suffixe `[ID] [année]`)
  - Classification automatique : catégorie A (orphelins, covers seulement) vs B (les deux ont de la musique)
  - Commande CLI `s3dedup diagnose` avec options `--prefix`, `--depth`, `--format` (table/json/csv), `--output`
  - 13 tests dédiés, 200 tests OK, ruff clean
  - Résultat sur le bucket réel : 92 groupes (7 orphelins safe, 85 à analyser)
  - Changement `--copy-props metadata-directive` → `--copy-props none` dans cleaner (non commité, session 15)
  - Règle `strip-backslashes` ajoutée dans cleaner (non commitée, session 15)
- Context: L'utilisateur observe encore des dossiers en doublon après les passes de clean/dedup. Le problème est un niveau au-dessus : même album importé depuis deux sources (ex: Deezer avec `[ID] [année]` dans le nom vs rip sans).
- Next:
  - Affiner la catégorie B : comparer etags/tailles pour distinguer "même album FLAC vs MP3" de "vrais doublons"
  - Générer un script de suppression pour les 7 orphelins catégorie A
  - Commiter les changements (strip-backslashes, --copy-props none, diagnose)
  - Workflow complet : `scan` → `clean` → `diagnose` → `generate-script`

### 2026-03-16 (session 15)
- Done:
  - Nouvelle règle `StripBackslashesRule` dans `cleaner.py` : supprime les `\` des clés S3
  - Collapse les espaces multiples résultants (ex: `\\` → double espace → simple espace)
  - Enregistrée dans `AVAILABLE_RULES` comme `strip-backslashes`
  - 5 tests unitaires dédiés, 27 tests cleaner OK, ruff clean
- Context: 1052 erreurs rclone (non bloquantes) causées par 2 fichiers avec `\` dans le nom
- Next:
  - Régénérer `clean.sh` avec `--rules strip-spaces,strip-backslashes`
  - Exécuter sur le bucket réel
  - Workflow complet : `scan` → `clean` → `scan` → `report` / `generate-script`

### 2026-03-03 (session 14)
- Done:
  - Fix `bitrate INTEGER` → `BIGINT` dans `media_metadata` (dépassement INT32 sur fichier vidéo à ~6 Gbps)
  - Migration automatique `_migrate()` dans `db.py` : détecte et corrige les bases existantes via `ALTER TABLE`
  - Audit des autres colonnes numériques : `size` (BIGINT) et `duration_s` (DOUBLE) déjà corrects
  - 182 tests OK, ruff clean
- Next:
  - Relancer `scan --extract-metadata --workers 32` (reprendra là où il s'est arrêté)
  - Workflow complet : `scan` → `clean` → `scan` → `report` / `generate-script`

### 2026-03-02 (session 13)
- Done:
  - Fix dossiers vides après `clean.sh` : nouvelle fonction `_collect_empty_dirs()` dans `cleaner.py`
  - Le script généré inclut maintenant des `aws s3 rm` pour supprimer les marqueurs de dossiers vides
  - Suppression du plus profond au moins profond, respecte `--dryrun`
  - Pas de suppression si le dossier contient encore des fichiers
  - 2 nouveaux tests, 22 tests cleaner OK, ruff clean
- Next:
  - Régénérer `clean.sh` et relancer sur le bucket réel
  - Relancer `scan --extract-metadata` avec le parallélisme (32+ threads)
  - Workflow complet : `scan` → `clean` → `scan` → `report` / `generate-script`

### 2026-03-02 (session 12)
- Done:
  - Parallélisation de `extract_all_media_metadata` avec `ThreadPoolExecutor` (32 threads par défaut)
  - Nouvelle option `--workers` sur `scan` + variable d'env `S3DEDUP_WORKERS`
  - Les écritures DuckDB restent sur le thread principal (thread-safety)
  - 180 tests OK, ruff clean
- Context: scan réel avec `--extract-metadata` → 12374/473632 fichiers en 4h (séquentiel). Estimation : ~160h. Avec 32 threads → ~5h.
- Next:
  - Relancer `scan --extract-metadata` sur le bucket réel avec le parallélisme
  - Ajuster `--workers` selon la perf observée (32 → 64 si le réseau le permet)
  - Workflow complet : `scan` → `clean` → `scan` → `report` / `generate-script`

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
