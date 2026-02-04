"""
Events backfill worker that processes jobs from the Delta table queue.

Each worker:
1. Leases available jobs
2. Fetches events from Polymarket API
3. Writes to ADLS bronze layer
4. Updates job status
"""
import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import requests
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from events_backfill.config import (
    ADLS_ACCOUNT_NAME,
    POLYMARKET_API_BASE,
    LEASE_DURATION_SECONDS,
    MAX_ATTEMPTS,
    REQUESTS_PER_SECOND,
    CONTAINER,
    BRONZE_PATH_TEMPLATE,
    JOBS_TABLE_PATH,
    BACKOFF_SCHEDULE,
    BACKOFF_CAP,
    JITTER_PERCENT,
)
from events_backfill.db_client import DeltaClient


class EventsBackfillWorker:
    def __init__(self, worker_id: Optional[str] = None, verbose: bool = False):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.verbose = verbose
        self._log(f"Initializing worker...")
        self._log(f"  Creating DeltaClient...")
        self.delta_client = DeltaClient()
        self._log(f"  Creating ADLS client...")
        self.datalake_client = self._get_datalake_client()
        self.session = requests.Session()
        self.min_request_interval = 1.0 / REQUESTS_PER_SECOND
        self.last_request_time = 0.0
        self._log(f"  Worker initialized")

    def _log(self, msg: str):
        """Print with worker prefix and timestamp."""
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[{ts}][{self.worker_id}] {msg}")

    def _vlog(self, msg: str):
        """Verbose log - only prints if verbose mode is on."""
        if self.verbose:
            self._log(msg)

    def _get_datalake_client(self) -> DataLakeServiceClient:
        """Get ADLS client using DefaultAzureCredential."""
        credential = DefaultAzureCredential()
        return DataLakeServiceClient(
            account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=credential,
        )

    def _calculate_backoff(self, attempts: int) -> int:
        """Calculate backoff delay with jitter."""
        base_delay = BACKOFF_SCHEDULE.get(attempts, BACKOFF_CAP)
        base_delay = min(base_delay, BACKOFF_CAP)
        jitter = random.uniform(-JITTER_PERCENT, JITTER_PERCENT) * base_delay
        return int(base_delay + jitter)

    def lease_jobs(self, batch_size: int = 10) -> list[dict]:
        """Attempt to lease available jobs using Delta operations."""
        self._vlog(f"Leasing up to {batch_size} jobs...")
        start = time.time()
        code = f'''
import json
from datetime import datetime, timedelta
from delta.tables import DeltaTable

try:
    path = "{JOBS_TABLE_PATH}"
    worker_id = "{self.worker_id}"
    lease_duration = {LEASE_DURATION_SECONDS}
    now = datetime.utcnow()
    lease_until = now + timedelta(seconds=lease_duration)

    # Read candidates
    df = spark.read.format("delta").load(path)
    candidates = df.filter(
        ((df.state == "pending") & (df.next_attempt_at <= now)) |
        ((df.state == "leased") & (df.lease_expires_at < now))
    ).orderBy("next_attempt_at").limit({batch_size}).collect()

    if not candidates:
        print(json.dumps({{"status": "ok", "jobs": []}}))
    else:
        # Use Delta merge to atomically lease jobs
        delta_table = DeltaTable.forPath(spark, path)
        leased = []

        for row in candidates:
            job_id = row.job_id
            # Try to lease this job
            delta_table.update(
                condition=f"job_id = '{{job_id}}' AND ((state = 'pending' AND next_attempt_at <= current_timestamp()) OR (state = 'leased' AND lease_expires_at < current_timestamp()))",
                set={{
                    "state": "'leased'",
                    "leased_by": f"'{{worker_id}}'",
                    "lease_expires_at": f"timestamp'{{lease_until}}'",
                    "updated_at": "current_timestamp()"
                }}
            )
            leased.append({{
                "job_id": row.job_id,
                "offset": row.offset,
                "limit": row.limit,
                "attempts": row.attempts
            }})

        print(json.dumps({{"status": "ok", "jobs": leased}}))
except Exception as e:
    print(json.dumps({{"status": "error", "error": str(e)}}))
'''
        result = self.delta_client.run_delta_operation(code, timeout_seconds=120)
        elapsed = time.time() - start
        self._vlog(f"Lease operation took {elapsed:.1f}s, raw result: {result[:500] if result else 'None'}")
        if result:
            try:
                # Handle result that might have extra output before JSON
                # Find the JSON object in the output
                json_start = result.find('{')
                if json_start == -1:
                    json_start = result.find('[')
                if json_start > 0:
                    self._vlog(f"Skipping {json_start} chars of non-JSON prefix: {result[:json_start]}")
                    result = result[json_start:]

                data = json.loads(result)

                # Handle new format with status
                if isinstance(data, dict):
                    if data.get("status") == "error":
                        self._log(f"ERROR from Databricks: {data.get('error')}")
                        return []
                    jobs = data.get("jobs", [])
                else:
                    # Old format - just a list
                    jobs = data

                self._log(f"Leased {len(jobs)} jobs in {elapsed:.1f}s")
                return jobs
            except json.JSONDecodeError as e:
                self._log(f"ERROR: Failed to parse lease result: {e}")
                self._log(f"Raw result was: {result[:500]}")
                return []
        self._vlog(f"No jobs returned from lease operation")
        return []

    def fetch_events(self, offset: int, limit: int) -> tuple[list[dict], Optional[str]]:
        """Fetch events from Polymarket API with rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            wait_time = self.min_request_interval - elapsed
            self._vlog(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)

        url = f"{POLYMARKET_API_BASE}/events"
        params = {"ascending": "true", "limit": limit, "offset": offset}

        self._vlog(f"Fetching {url} with offset={offset}, limit={limit}")
        start = time.time()
        self.last_request_time = time.time()

        try:
            response = self.session.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            self._log(f"ERROR: Request failed: {e}")
            return [], f"REQUEST_ERROR:{str(e)[:100]}"

        fetch_time = time.time() - start
        self._vlog(f"API response: status={response.status_code}, time={fetch_time:.2f}s, size={len(response.content)} bytes")

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "60")
            self._log(f"Rate limited (429), retry after {retry_after}s")
            return [], f"HTTP_429:retry_after={retry_after}"

        if response.status_code != 200:
            self._log(f"HTTP error: {response.status_code}")
            return [], f"HTTP_{response.status_code}"

        try:
            data = response.json()
            events = data if isinstance(data, list) else []
            self._vlog(f"Parsed {len(events)} events from response")
            return events, None
        except json.JSONDecodeError as e:
            self._log(f"ERROR: JSON decode failed: {e}")
            return [], f"JSON_ERROR:{str(e)[:100]}"

    def write_to_bronze(self, events: list[dict], offset: int, limit: int) -> str:
        """Write events to ADLS bronze layer as JSONL."""
        ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = BRONZE_PATH_TEMPLATE.format(
            ingest_date=ingest_date, offset=offset, limit=limit
        )

        self._vlog(f"Writing {len(events)} events to ADLS path: {path}")
        start = time.time()

        jsonl_content = "\\n".join(json.dumps(event) for event in events)
        content_bytes = jsonl_content.encode("utf-8")
        self._vlog(f"Serialized {len(content_bytes)} bytes")

        try:
            file_system_client = self.datalake_client.get_file_system_client(CONTAINER)
            file_client = file_system_client.get_file_client(path)
            file_client.upload_data(content_bytes, overwrite=True)
        except Exception as e:
            self._log(f"ERROR: ADLS write failed: {e}")
            raise

        elapsed = time.time() - start
        self._vlog(f"ADLS write completed in {elapsed:.2f}s")

        full_path = f"abfss://{CONTAINER}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/{path}"
        return full_path

    def mark_success(self, job_id: str, payload_path: str, records_written: int):
        """Mark job as done."""
        self._vlog(f"Marking job {job_id} as done with {records_written} records")
        start = time.time()
        # Escape single quotes in payload_path
        payload_path_escaped = payload_path.replace("'", "''")
        code = f'''
from delta.tables import DeltaTable

path = "{JOBS_TABLE_PATH}"
delta_table = DeltaTable.forPath(spark, path)
delta_table.update(
    condition="job_id = '{job_id}'",
    set={{
        "state": "'done'",
        "done_at": "current_timestamp()",
        "lease_expires_at": "NULL",
        "leased_by": "NULL",
        "payload_path": "'{payload_path_escaped}'",
        "records_written": "{records_written}",
        "updated_at": "current_timestamp()"
    }}
)
print("OK")
'''
        result = self.delta_client.run_delta_operation(code)
        elapsed = time.time() - start
        self._vlog(f"mark_success completed in {elapsed:.1f}s, result: {result}")

    def mark_failure(self, job_id: str, attempts: int, error_code: str, error_message: str):
        """Mark job as failed or pending retry."""
        new_attempts = attempts + 1
        # Escape single quotes and remove newlines (which break SQL strings)
        error_message_escaped = error_message.replace("'", "''").replace("\n", " ").replace("\r", "")[:500]
        self._vlog(f"Marking job {job_id} as failed (attempt {new_attempts}): {error_code}")
        start = time.time()

        if new_attempts >= MAX_ATTEMPTS:
            self._log(f"Job {job_id} exhausted all {MAX_ATTEMPTS} attempts, marking as failed")
            code = f'''
from delta.tables import DeltaTable
try:
    path = "{JOBS_TABLE_PATH}"
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.update(
        condition="job_id = '{job_id}'",
        set={{
            "state": "'failed'",
            "attempts": "{new_attempts}",
            "last_error_code": "'{error_code}'",
            "last_error_message": "'{error_message_escaped}'",
            "last_error_at": "current_timestamp()",
            "lease_expires_at": "NULL",
            "leased_by": "NULL",
            "updated_at": "current_timestamp()"
        }}
    )
    print("FAILED")
except Exception as e:
    print(f"ERROR: {{e}}")
'''
        else:
            backoff_seconds = self._calculate_backoff(new_attempts)
            self._vlog(f"Job {job_id} will retry in {backoff_seconds}s")
            code = f'''
from delta.tables import DeltaTable
from datetime import datetime, timedelta
try:
    path = "{JOBS_TABLE_PATH}"
    next_attempt = datetime.utcnow() + timedelta(seconds={backoff_seconds})

    delta_table = DeltaTable.forPath(spark, path)
    delta_table.update(
        condition="job_id = '{job_id}'",
        set={{
            "state": "'pending'",
            "attempts": "{new_attempts}",
            "next_attempt_at": f"timestamp'{{next_attempt}}'",
            "last_error_code": "'{error_code}'",
            "last_error_message": "'{error_message_escaped}'",
            "last_error_at": "current_timestamp()",
            "lease_expires_at": "NULL",
            "leased_by": "NULL",
            "updated_at": "current_timestamp()"
        }}
    )
    print("RETRY")
except Exception as e:
    print(f"ERROR: {{e}}")
'''
        result = self.delta_client.run_delta_operation(code)
        elapsed = time.time() - start
        if result and result.startswith("ERROR"):
            self._log(f"mark_failure FAILED: {result}")
        else:
            self._vlog(f"mark_failure completed in {elapsed:.1f}s, result: {result}")

    def process_job(self, job: dict) -> bool:
        """Process a single job. Returns True on success."""
        job_id = job["job_id"]
        offset = job["offset"]
        limit = job["limit"]
        attempts = job["attempts"]

        self._log(f"Processing {job_id} (offset={offset}, attempt={attempts})...")
        job_start = time.time()

        try:
            # Step 1: Fetch events
            self._vlog(f"Step 1: Fetching events from API...")
            events, error = self.fetch_events(offset, limit)

            if error:
                self._log(f"Fetch error for {job_id}: {error}")
                self._vlog(f"Step 2: Marking failure...")
                self.mark_failure(job_id, attempts, error.split(":")[0], error)
                return False

            # Step 2: Write to ADLS
            self._vlog(f"Step 2: Writing {len(events)} events to ADLS...")
            payload_path = self.write_to_bronze(events, offset, limit)

            # Step 3: Mark success in Delta
            self._vlog(f"Step 3: Marking job as done in Delta...")
            self.mark_success(job_id, payload_path, len(events))

            job_elapsed = time.time() - job_start
            self._log(f"DONE {job_id}: {len(events)} events in {job_elapsed:.1f}s")
            return True

        except Exception as e:
            self._log(f"EXCEPTION for {job_id}: {type(e).__name__}: {e}")
            import traceback
            self._vlog(f"Traceback:\n{traceback.format_exc()}")
            self.mark_failure(job_id, attempts, "EXCEPTION", str(e))
            return False

    def run(self, max_jobs: Optional[int] = None, idle_sleep: int = 30):
        """Run the worker loop."""
        self._log(f"Starting worker loop (max_jobs={max_jobs}, idle_sleep={idle_sleep}s)...")
        jobs_processed = 0
        jobs_succeeded = 0
        consecutive_empty = 0

        while max_jobs is None or jobs_processed < max_jobs:
            self._vlog(f"Attempting to lease jobs (processed so far: {jobs_processed})...")
            # Only lease as many jobs as we need
            remaining = (max_jobs - jobs_processed) if max_jobs else 10
            batch_size = min(10, remaining)
            jobs = self.lease_jobs(batch_size=batch_size)

            if not jobs:
                consecutive_empty += 1
                self._vlog(f"No jobs returned (consecutive empty: {consecutive_empty})")
                if consecutive_empty >= 3:
                    self._log(f"No jobs available, sleeping {idle_sleep}s...")
                    time.sleep(idle_sleep)
                continue

            consecutive_empty = 0
            self._log(f"Processing batch of {len(jobs)} jobs...")
            for job in jobs:
                success = self.process_job(job)
                jobs_processed += 1
                if success:
                    jobs_succeeded += 1
                if max_jobs and jobs_processed >= max_jobs:
                    break

        self._log(f"Finished. Processed {jobs_processed} jobs ({jobs_succeeded} succeeded, {jobs_processed - jobs_succeeded} failed).")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Events backfill worker")
    parser.add_argument("--worker-id", help="Worker identifier")
    parser.add_argument("--max-jobs", type=int, help="Max jobs to process (default: unlimited)")
    parser.add_argument("--idle-sleep", type=int, default=30, help="Sleep seconds when no jobs")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    worker = EventsBackfillWorker(worker_id=args.worker_id, verbose=args.verbose)
    worker.run(max_jobs=args.max_jobs, idle_sleep=args.idle_sleep)


if __name__ == "__main__":
    main()
