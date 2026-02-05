## Endpoint

```
curl --request GET --url "https://gamma-api.polymarket.com/events?ascending=true&limit=20&offset=<offset>"
```

API limits: 500 requests / 10s. Throttle requests over the maximum configured rate.

The output will be a succession of json arrays of size at most 100.

## Bronze data

Save the output to:

`abfss://bronze@datalakeprod.dfs.core.windows.net/polymarket/events/mode=refresh_new/ingest_date=YYYY-MM-DD/offset=<offset written on 5 digits>_limit=20_<HHMMSS>.jsonl`

Timestamp ensures append-only semantics—re-querying the same offset creates a new file.

---

## State Management

Delta table at `abfss://polypoly@polybotdlso94zgo.dfs.core.windows.net/bronze/_meta/refresh_state_events` with one row per offset:

| Column | Type | Description |
|--------|------|-------------|
| `offset` | BIGINT | Offset queried (PK) |
| `limit` | INT | Limit used (20) |
| `first_run_at` | TIMESTAMP | When this offset was first queried |
| `last_run_at` | TIMESTAMP | When this offset was last queried |
| `records_fetched` | INT | Number of records returned (last run) |
| `last_bronze_path` | STRING | Most recent file written for this offset |
| `last_error` | STRING | Most recent error message (NULL if none) |
| `last_error_at` | TIMESTAMP | When the error occurred |

**Setup seeds the table** with one row: `(offset=188448, records_fetched=NULL)` — first offset not covered by backfill.

**Finding next offset**:
```sql
SELECT MIN(offset) FROM state WHERE records_fetched IS NULL OR records_fetched < 20
-- If NULL (all complete): SELECT MAX(offset) + 20 FROM state
```

---

## Job Logic (runs every 5 minutes)

```
1. Compute next_offset:
   a. SELECT MIN(offset) FROM state WHERE records_fetched IS NULL OR records_fetched < 20
   b. If NULL (all complete): SELECT MAX(offset) + 20 FROM state
2. Loop:
   a. Fetch events at next_offset with limit=20
   b. If API error → upsert row (offset, last_error, last_error_at), break
   c. count = len(records)
   d. If count == 0 → caught up, break (don't write file, don't upsert row)
   e. Write JSONL to bronze (only if count > 0)
   f. Upsert row: (offset, limit=20, first_run_at=COALESCE(existing, now()), last_run_at=now(), records_fetched=count, last_bronze_path=path)
   g. If count == 20 → next_offset += 20, continue loop
   h. If count < 20 → caught up, break
```

**Key**: Only advance offset when we get a full page (20 records). A partial page means we're at the end—next run re-queries the same offset to catch new records.

No retries needed—next scheduled run handles failures naturally.

---

## Implementation

### Commands

```bash
# Create state table and seed with offset=188448 (first offset not covered by backfill)
python run_events_refresh.py setup

# Show state: next offset, recent fetches, errors
python run_events_refresh.py status

# Run one refresh cycle (for testing or manual trigger)
python run_events_refresh.py run

# Run with verbose logging
python run_events_refresh.py run -v
```

### Configuration (events_refresh/config.py)

- `INITIAL_OFFSET`: 188448 (first record not covered by backfill)
- `LIMIT`: 20
- `REQUESTS_PER_SECOND`: 40

---

## Azure Function Deployment

### Project Structure

```
polypoly_func/                    # Generic function app (multiple functions)
├── function_app.py               # Main entry point (all functions defined here)
├── events_refresh_new/           # Events refresh logic
│   ├── __init__.py
│   ├── config.py
│   ├── api.py
│   ├── db_client.py
│   ├── delta_state.py
│   ├── bronze_writer.py
│   └── runner.py
├── requirements.txt
├── host.json
└── deploy.sh
```

CLI imports from function folder (no code duplication):
```python
# run_events_refresh.py
sys.path.insert(0, str(Path(__file__).parent / "polypoly_func"))
from events_refresh_new.runner import run_refresh
```

### function_app.py

```python
import azure.functions as func
import logging
from events_refresh_new.runner import run_refresh

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=False)
def events_refresh_timer(timer: func.TimerRequest) -> None:
    logging.info("=== EVENTS REFRESH START ===")

    if timer.past_due:
        logging.warning("Timer is past due, running anyway")

    try:
        result = run_refresh(verbose=True)
        logging.info(f"=== EVENTS REFRESH COMPLETE === records_fetched={result['total_records']} offsets_processed={result['offsets_processed']}")
    except Exception as e:
        logging.exception(f"=== EVENTS REFRESH FAILED === error={e}")
        raise
```

### host.json (verbose logging)

```json
{
  "version": "2.0",
  "logging": {
    "logLevel": {
      "default": "Information",
      "Host.Results": "Information",
      "Function.events_refresh_timer": "Information",
      "events_refresh": "Debug"
    },
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": false
      }
    }
  },
  "functionTimeout": "00:10:00"
}
```

### requirements.txt

```
azure-functions
azure-identity
azure-storage-file-datalake
deltalake
requests
```

### Environment Variables (App Settings)

| Setting | Description |
|---------|-------------|
| `AZURE_STORAGE_ACCOUNT_NAME` | `polybotdlso94zgo` |
| `AZURE_STORAGE_ACCOUNT_NAME_BRONZE` | `datalakeprod` |
| `DATABRICKS_HOST` | Databricks workspace URL |
| `DATABRICKS_TOKEN` | PAT or managed identity |

### Deployment

```bash
# Deploy using the deploy script
cd polypoly_func
./deploy.sh

# Or manually:
func azure functionapp publish polypoly-func --build remote
```

### Viewing Logs

Filter logs by function name using `operation_Name`:

```bash
# Health check: last run, status, duration
az monitor app-insights query \
  --app polypoly-func \
  --resource-group polybot-rg \
  --analytics-query "requests | where name == 'events_refresh_timer' | order by timestamp desc | take 1 | project LastRun=timestamp, Status=iif(success=='True','Success','Failed'), DurationSec=round(duration/1000,1)"
# Note: Timer runs every 5 minutes (schedule: 0 */5 * * * *)

# Logs for events_refresh_timer only
az monitor app-insights query \
  --app polypoly-func \
  --resource-group polybot-rg \
  --analytics-query "traces | where operation_Name == 'events_refresh_timer' | order by timestamp desc | project timestamp, message | take 50"

# Recent executions history
az monitor app-insights query \
  --app polypoly-func \
  --resource-group polybot-rg \
  --analytics-query "requests | where name == 'events_refresh_timer' | order by timestamp desc | take 10 | project timestamp, success, duration"

# Or via Azure Portal:
# Function App → polypoly-func → Functions → events_refresh_timer → Monitor
```

### Log Output Example

```
2026-02-05T14:30:00Z [Information] === EVENTS REFRESH START ===
2026-02-05T14:30:00Z [Debug] Computing next offset...
2026-02-05T14:30:00Z [Debug] No incomplete offsets found
2026-02-05T14:30:00Z [Debug] Next offset: 188460
2026-02-05T14:30:01Z [Debug] Fetching offset=188460 limit=20
2026-02-05T14:30:01Z [Debug] API returned 20 records
2026-02-05T14:30:02Z [Debug] Wrote bronze file: .../offset=188460_limit=20_143001.jsonl
2026-02-05T14:30:02Z [Debug] Upserted state for offset=188460
2026-02-05T14:30:02Z [Debug] Fetching offset=188480 limit=20
2026-02-05T14:30:02Z [Debug] API returned 8 records (partial page, caught up)
2026-02-05T14:30:03Z [Debug] Wrote bronze file: .../offset=188480_limit=20_143002.jsonl
2026-02-05T14:30:03Z [Debug] Upserted state for offset=188480
2026-02-05T14:30:03Z [Information] === EVENTS REFRESH COMPLETE === records_fetched=28 offsets_processed=2
```