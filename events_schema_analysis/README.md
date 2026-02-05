# Events Schema Analysis

Analyze Polymarket events data from Azure ADLS to determine consistent schemas for Events, Markets, Series, Tags, and other nested entities.

## Requirements

```bash
pip install azure-storage-file-datalake azure-identity python-dotenv
```

Requires `../.env` with:
```
ADLS_ACCOUNT_NAME=your_storage_account
```

Also requires Azure CLI login: `az login`

## Usage

### Full analysis from ADLS (all modes)
```bash
python analyze_schema.py --adls --mode all --ingest-date 2026-02-05 --sql --output events_schema.txt
```

### Smoke test (5 files per folder)
```bash
python analyze_schema.py --adls --mode all --ingest-date 2026-02-05 --max-files 5 --sql
```

### Backfill only
```bash
python analyze_schema.py --adls --mode backfill --ingest-date 2026-02-05 --sql
```

### Refresh only
```bash
python analyze_schema.py --adls --mode refresh_new --ingest-date 2026-02-05 --sql
```

### Multiple ingest dates
```bash
python analyze_schema.py --adls --mode all --ingest-date 2026-02-05 --ingest-date 2026-02-04 --sql
```

### Local files
```bash
python analyze_schema.py --local ../data_explore/*.jsonl --sql
```

## Options

| Option | Description |
|--------|-------------|
| `--adls` | Read from Azure ADLS |
| `--local FILES` | Read from local JSONL files |
| `--mode {backfill,refresh_new,all}` | Which ADLS folders to scan (default: all) |
| `--ingest-date DATE` | Ingest date(s) to scan (repeatable) |
| `--max-files N` | Limit files per folder (for smoke testing) |
| `--sql` | Generate SQL CREATE TABLE statements |
| `--threshold PCT` | Min field presence % for SQL (default: 90) |
| `--output FILE` | Write results to file |

## Output

- Field presence percentages grouped by frequency (100%, 90-99%, 50-89%, <50%)
- Type distribution per field
- Sample values
- Entity counts with unique slug/name counts
- SQL schemas for Databricks Spark (when `--sql` is used)
