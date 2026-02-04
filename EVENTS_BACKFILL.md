## Endpoint

```
curl --request GET --url "https://gamma-api.polymarket.com/events?ascending=true&limit=500&offset=<offset>"
```

API limits: 500 requests / 10s. Throttle requests over the maximum configured rate.

The output will be a succession of json arrays of size at most 100.

## Bronze data

Save the output to:

`abfss://bronze@datalakeprod.dfs.core.windows.net/polymarket/events/mode=backfill/ingest_date=2026-02-04/offset=<offset written on 10 digits>_limit=500.jsonl`

## Polling management

We'll have a delta table: "meta.backfill_jobs_events" with the following schema:

- job_id STRING
Deterministic: "offset=12500&limit=500" 
- offset BIGINT
- limit INT (500)
- state STRING — "pending" | "leased" | "done" | "failed"
- leased_by STRING — worker identifier
- lease_expires_at TIMESTAMP
- attempts INT
- next_attempt_at TIMESTAMP — for backoff / retry later
- last_error_code STRING — e.g. "HTTP_503", "TIMEOUT"
- last_error_message STRING — short text
- last_error_at TIMESTAMP
- done_at TIMESTAMP
- updated_at TIMESTAMP
- created_at TIMESTAMP
- payload_path STRING or NULL
- records_written INT or NULL

Which we will prefill with jobs increasing offset by 500 up to 188,000.

### How workers “grab” jobs safely (concurrency)

Key idea: leasing. A worker may take a job if:
- it’s pending and next_attempt_at <= now()
- OR it’s leased but the lease expired (lease_expires_at < now())

Then it updates the row to leased, sets leased_by, and sets lease_expires_at = now() + lease_duration.

### Grab query (conceptually)

You do:

1. pick a small batch of candidate jobs (e.g. 50)
2. try to lease them with a conditional update
3. the update affects 0 or 1 row per job; whichever you successfully updated is yours

On Delta, this is typically done with a MERGE or UPDATE ... WHERE state IN (...) AND lease_expires_at < now() pattern. (Exact syntax varies with your environment, but the logic is the same.)

### Worker lifecycle

A) Success

After writing the bronze file:
- state = 'done'
- done_at = now()
- lease_expires_at = null
- leased_by = null
- updated_at = now()
- set payload_path and records_written (total number of events fetched)

B) Failure (retry later)

On error:

- attempts = attempts + 1
- last_error_* = ...
- last_error_at = now()
- compute next_attempt_at = now() + backoff(attempts) (+ jitter)
- set state = 'pending' 
- clear lease fields

C) Permanent failure (DLQ-ish)

If attempts >= MAX (say 10):
- state = 'failed'
- keep last error info

### Backoff (simple)

Example schedule for next_attempt_at after failure:
- attempt 1 → +30s
- attempt 2 → +2m
- attempt 3 → +10m
- attempt 4 → +1h
- attempt 5+ → +6h (cap)

Add jitter ±10–20% so all workers don’t retry in lockstep.

Also: if error is 429, respect Retry-After if you have it.

---

## Implementation

### Commands

```bash
# Create Delta table and prefill with 377 jobs (offset 0 to 188,000, step 500)
python run_events_backfill.py setup

# Check job status (counts by state, error summary, active leases)
python run_events_backfill.py status

# Run a single worker (processes jobs until done or interrupted)
python run_events_backfill.py worker

# Run with options
python run_events_backfill.py worker --max-jobs 10      # Process only 10 jobs
python run_events_backfill.py worker -v                 # Verbose logging
python run_events_backfill.py worker --worker-id w1     # Custom worker ID
python run_events_backfill.py worker --idle-sleep 60    # Wait 60s when no jobs

# Reset stuck leased jobs back to pending (safe for done jobs)
python run_events_backfill.py reset_leased
```

### Architecture

- **Delta table**: `abfss://polypoly@polybotdlso94zgo.dfs.core.windows.net/bronze/_meta/backfill_jobs_events`
- **Bronze output**: `bronze/polymarket/events/mode=backfill/ingest_date=YYYY-MM-DD/offset=XXXXX_limit=500.jsonl`
- **Databricks**: Uses command execution API to run Spark/Delta operations on cluster
- **ADLS writes**: Uses `DefaultAzureCredential` (requires Storage Blob Data Contributor role)

### Worker flow

1. Lease batch of pending jobs (up to 10) via Delta update
2. For each job:
   - Fetch events from Polymarket API
   - Write JSONL to ADLS bronze layer
   - Mark job as done in Delta
3. On failure: increment attempts, set backoff, mark as pending (or failed if max attempts)
4. Loop until no jobs or max_jobs reached

### Configuration (events_backfill/config.py)

- `MAX_OFFSET`: 188,000
- `LIMIT`: 500 events per request
- `LEASE_DURATION_SECONDS`: 300 (5 min)
- `MAX_ATTEMPTS`: 10
- `REQUESTS_PER_SECOND`: 40
- `BACKOFF_SCHEDULE`: 30s → 2m → 10m → 1h → 6h cap

