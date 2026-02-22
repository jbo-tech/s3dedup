# s3dedup

CLI tool to detect and report duplicates in S3 buckets.

Works with AWS S3 and S3-compatible services (Mega.io, MinIO, LocalStack, etc.).

## Features

- **Byte-identical deduplication** — 3-pass detection (size → ETag → SHA256), incremental scans
- **Suspect name detection** — Find files with similar names but different content (encoding issues, copy suffixes, extra spaces)
- **Media metadata grouping** — Extract audio/video tags to find same-work-different-encoding duplicates (e.g. FLAC vs MP3)
- **Smart retention policy** — `--keep cleanest,shortest,oldest` to automatically prefer clean filenames

## How it works

### Byte-identical detection

3-pass strategy, optimized to minimize S3 costs:

1. **Size** — Group objects by size (free, from `ListObjectsV2`)
2. **ETag** — Among same-size objects, group by ETag (free, already in listing)
3. **SHA256** — For ambiguous multipart ETags, download and hash (targeted)

### Suspect names

Normalizes filenames (lowercase, strip accents, remove copy suffixes like `(1)`, `_copy`, `- Copie`, strip whitespace) and reports groups where the normalized name matches but the content differs.

### Media metadata

With `--extract-metadata`, downloads the first 256 KB of audio/video files to read tags (ID3, MP4, FLAC, OGG via mutagen). Groups files sharing the same artist + title but with different encodings.

Results are stored in a local DuckDB index. Scans are incremental — only new objects are indexed.

## Install

```bash
# Requires Python 3.12+ and uv
git clone https://github.com/jbo-tech/s3dedup.git
cd s3dedup
uv sync
```

## Usage

```bash
# 1. Scan a bucket (or a prefix)
uv run s3dedup scan --bucket my-bucket --prefix Music/

# Scan with media metadata extraction (audio/video tags)
uv run s3dedup scan --bucket my-bucket --extract-metadata

# For S3-compatible services, add --endpoint-url
uv run s3dedup scan --bucket my-bucket --endpoint-url https://s3.example.com

# 2. View report (table by default, or json/csv/markdown)
uv run s3dedup report
uv run s3dedup report --format json
uv run s3dedup report --format csv
uv run s3dedup report --format markdown --output report.md

# 3. Generate a reviewable deletion script
uv run s3dedup generate-script --bucket my-bucket
```

The scan is incremental: run it on multiple prefixes, results accumulate in the same DuckDB index.

```bash
uv run s3dedup scan --bucket media --prefix Music/
uv run s3dedup scan --bucket media --prefix Movies/
uv run s3dedup report  # includes duplicates across both prefixes
```

### Reset

To start fresh (e.g. after cleaning up duplicates), delete the DuckDB index and rescan:

```bash
rm s3dedup.duckdb
uv run s3dedup scan --bucket media --prefix Music/
```

To use a different database path, pass `--db` to any command:

```bash
uv run s3dedup scan --bucket media --db /tmp/media.duckdb
uv run s3dedup report --db /tmp/media.duckdb
```

## Retention policy (`--keep`)

The `--keep` option controls which file to preserve when duplicates are found. Accepts a comma-separated list of criteria (first criterion has priority, next ones break ties):

| Criterion | Keeps the file with... |
|---|---|
| `shortest` | the shortest filename |
| `oldest` | the oldest `LastModified` date |
| `newest` | the newest `LastModified` date |
| `cleanest` | the cleanest filename (no mojibake, no copy suffix, no extra spaces) |

Default: `--keep cleanest,shortest,oldest`

Examples:

```bash
uv run s3dedup generate-script --bucket my-bucket                              # défaut: cleanest,shortest,oldest
uv run s3dedup generate-script --bucket my-bucket --keep shortest,oldest       # ignorer la propreté du nom
uv run s3dedup generate-script --bucket my-bucket --keep cleanest,newest       # préférer les plus récents
```

The `cleanest` criterion penalizes:
- Mojibake encoding (`Ã©tÃ©` instead of `été`) — +10
- Copy suffixes (`(1)`, `_copy`, `_1`, `- Copie`) — +5
- Leading/trailing whitespace — +2
- Multiple consecutive spaces — +1

## Output

### Report

`report` displays three sections (each omitted if empty):

1. **Duplicate groups** — Byte-identical files, sorted by wasted space
2. **Suspect names** — Files with similar normalized names but different content
3. **Same work, different encoding** — Media files sharing artist + title (requires `--extract-metadata` during scan)

Use `--format json` or `--format csv` for machine-readable output, or `--format markdown` for a file-friendly report. Add `--output report.md` to write directly to a file instead of stdout. All three sections are included in every format.

### Deletion script

`generate-script` creates an executable bash script with `aws s3 rm` commands:
- Each deletion is commented with the duplicate group info
- The `--endpoint-url` is automatically retrieved from the database (saved during scan) if not provided
- **Review the script before running it — deletions are irreversible**

### Dry-run

To preview which files would be deleted without actually deleting them:

```bash
bash delete_duplicates.sh --dryrun
```

## Authentication

Uses the standard boto3 credential chain (environment variables, `~/.aws/credentials`, SSO, instance profile). No credentials are handled by s3dedup itself.

For S3-compatible services, set `--endpoint-url` or the `AWS_ENDPOINT_URL` environment variable.

The endpoint URL is saved in the database during `scan`. Subsequent commands (`generate-script`) will reuse it automatically — no need to pass `--endpoint-url` again.

## Development

```bash
uv run pytest           # run tests (150)
uv run ruff check .     # lint
```

## Database

s3dedup uses a local DuckDB file (`s3dedup.duckdb` by default) to store:

| Table | Content |
|---|---|
| `objects` | S3 object index (key, size, ETag, SHA256, last_modified) |
| `media_metadata` | Audio/video tags (artist, album, title, codec, bitrate) |
| `bucket_config` | Per-bucket settings (endpoint URL) |

To reset everything, simply delete the file: `rm s3dedup.duckdb`
