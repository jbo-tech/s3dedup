# Decomposition — s3dedup

## Tasks (6)

| Task | Name | Effort | Parallel | Depends |
|------|------|--------|----------|---------|
| T1 | Project Setup | S | non | - |
| T2 | Interfaces & DB Schema | S | non | T1 |
| T3 | Scanner (S3 + passe 1&2) | M | oui | T2 |
| T4 | Hasher (passe 3) | M | oui | T2 |
| T5 | Reporter + ScriptGenerator | M | oui | T2 |
| T6 | CLI Integration & E2E | M | non | T3,T4,T5 |

## Dependency graph

```
T1 → T2 → T3 (scanner)
          → T4 (hasher)     ← parallel
          → T5 (reporter)
     T3,T4,T5 → T6 (integration)
```

## Shared interfaces
- DuckDB schema: table `objects` (key, size, etag, is_multipart, sha256, last_modified, scanned_at)
- Dataclasses: ObjectInfo, DuplicateGroup, ScanStats
- DB module: create_tables, upsert_objects, find_size_duplicates, find_etag_duplicates, mark_hash
