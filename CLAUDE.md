# s3dedup

Outil CLI Python pour dédupliquer des objets S3.

## Commands
```bash
uv run s3dedup scan --bucket BUCKET [--prefix PREFIX] [--endpoint-url URL] [--extract-metadata] [--db PATH]
uv run s3dedup report [--format table|json|csv|markdown] [--output PATH] [--db PATH]
uv run s3dedup generate-script --bucket BUCKET [--keep shortest,oldest|cleanest,...] [--endpoint-url URL] [--db PATH] [--output PATH]
uv run s3dedup clean --bucket BUCKET [--prefix PREFIX] [--rules strip-spaces] [--endpoint-url URL] [--db PATH] [--output clean.sh]
uv run pytest              # tests
uv run ruff check .        # lint
```

## Stack
- Python 3.12+
- boto3 (AWS S3)
- click (CLI)
- DuckDB (index local)
- rich (progression)
- mutagen (métadonnées média)
- Build : pyproject.toml + uv

## Conventions
- Code style : ruff
- Tests : pytest
- Comments/docstrings : français
- Commit messages : anglais (conventional commits)

## Context
When relevant, read:
- Current work: `.claude/context/status.md`
- Past mistakes: `.claude/context/anti-patterns.md`
- Technical decisions: `.claude/context/decisions.md`
- Scope: `.claude/context/scope.md`
- Task decomposition: `.claude/context/decomposition.md`

## End of session
Run `/retro` before stopping to update context files.
