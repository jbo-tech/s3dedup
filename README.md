# s3dedup

CLI tool to detect and report byte-identical duplicates in S3 buckets.

Works with AWS S3 and S3-compatible services (Mega.io, MinIO, LocalStack, etc.).

## How it works

3-pass detection strategy, optimized to minimize S3 costs:

1. **Size** — Group objects by size (free, from `ListObjectsV2`)
2. **ETag** — Among same-size objects, group by ETag (free, already in listing)
3. **SHA256** — For ambiguous multipart ETags, download and hash (targeted)

Results are stored in a local DuckDB index. Scans are incremental — only new objects are indexed.

## Install

```bash
# Requires Python 3.12+ and uv
git clone https://github.com/YOUR_USER/s3dedup.git
cd s3dedup
uv sync
```

## Usage

```bash
# 1. Scan a bucket (or a prefix)
uv run s3dedup scan --bucket my-bucket --prefix Music/

# For S3-compatible services, add --endpoint-url
uv run s3dedup --endpoint-url https://s3.example.com scan --bucket my-bucket

# 2. View duplicate report
uv run s3dedup report --format json    # or csv

# 3. Generate a reviewable deletion script
uv run s3dedup generate-script --bucket my-bucket --keep oldest
# Options: --keep oldest | newest | largest
```

The scan is incremental: run it on multiple prefixes, results accumulate in the same DuckDB index.

```bash
uv run s3dedup scan --bucket media --prefix Music/
uv run s3dedup scan --bucket media --prefix Movies/
uv run s3dedup report  # includes duplicates across both prefixes
```

To start fresh (e.g. after cleaning up duplicates), delete the index before rescanning:

```bash
rm s3dedup.duckdb
uv run s3dedup scan --bucket media --prefix Music/
```

## Output

- `report` outputs to stdout (JSON or CSV) — pipe or redirect as needed
- `generate-script` creates an executable bash script with `aws s3 rm` commands
  - Each deletion is commented with the duplicate group info
  - Includes a dry-run option
  - **Review the script before running it — deletions are irreversible**

## Authentication

Uses the standard boto3 credential chain (environment variables, `~/.aws/credentials`, SSO, instance profile). No credentials are handled by s3dedup itself.

For S3-compatible services, set `--endpoint-url` or the `AWS_ENDPOINT_URL` environment variable.

## Development

```bash
uv run pytest           # run tests
uv run ruff check .     # lint
```
