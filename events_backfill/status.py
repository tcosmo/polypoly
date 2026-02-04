"""
Check events backfill job status.
"""
import json
from events_backfill.config import JOBS_TABLE_PATH
from events_backfill.db_client import get_delta_client


def main():
    client = get_delta_client()

    # Get counts by state
    code = f'''
import json
df = spark.read.format("delta").load("{JOBS_TABLE_PATH}")
result = df.groupBy("state").agg(
    {{"*": "count", "records_written": "sum"}}
).collect()
output = [[r.state, r["count(1)"], r["sum(records_written)"] or 0] for r in result]
print(json.dumps(output))
'''
    result = client.run_delta_operation(code)

    print("\n=== Events Backfill Job Status ===\n")
    print(f"{'State':<12} {'Count':>8} {'Records':>12}")
    print("-" * 34)

    total_jobs = 0
    total_records = 0

    if result:
        try:
            rows = json.loads(result)
            for row in sorted(rows, key=lambda x: x[0]):
                state, count, records = row[0], int(row[1]), int(row[2] or 0)
                print(f"{state:<12} {count:>8} {records:>12}")
                total_jobs += count
                total_records += records
        except json.JSONDecodeError:
            print(f"  Error parsing: {result}")

    print("-" * 34)
    print(f"{'Total':<12} {total_jobs:>8} {total_records:>12}")

    # Get error counts by error code
    code = f'''
import json
df = spark.read.format("delta").load("{JOBS_TABLE_PATH}")
error_counts = df.filter(df.last_error_code.isNotNull()).groupBy("last_error_code").count().collect()
total_with_errors = df.filter(df.last_error_code.isNotNull()).count()
output = {{"total": total_with_errors, "by_code": [[r.last_error_code, r["count"]] for r in error_counts]}}
print(json.dumps(output))
'''
    result = client.run_delta_operation(code)

    if result:
        try:
            data = json.loads(result)
            total_errors = data.get("total", 0)
            by_code = data.get("by_code", [])
            if total_errors > 0:
                print(f"\n=== Error Summary ({total_errors} jobs with errors) ===\n")
                for error_code, count in sorted(by_code, key=lambda x: -x[1]):
                    print(f"  {error_code}: {count}")
        except json.JSONDecodeError:
            pass

    # Get recent errors
    code = f'''
import json
df = spark.read.format("delta").load("{JOBS_TABLE_PATH}")
errors = df.filter(df.last_error_code.isNotNull()).orderBy(df.last_error_at.desc()).limit(5).collect()
output = [[r.job_id, r.attempts, r.last_error_code, str(r.last_error_at)] for r in errors]
print(json.dumps(output))
'''
    result = client.run_delta_operation(code)

    if result:
        try:
            errors = json.loads(result)
            if errors:
                print("\n=== Recent Errors ===\n")
                for row in errors:
                    job_id, attempts, error_code, error_at = row
                    print(f"  {job_id}: {error_code} (attempt {attempts}) at {error_at}")
        except json.JSONDecodeError:
            pass

    # Get active leases
    code = f'''
import json
df = spark.read.format("delta").load("{JOBS_TABLE_PATH}")
leases = df.filter(df.state == "leased").orderBy("lease_expires_at").limit(10).collect()
output = [[r.job_id, r.leased_by, str(r.lease_expires_at)] for r in leases]
print(json.dumps(output))
'''
    result = client.run_delta_operation(code)

    if result:
        try:
            leases = json.loads(result)
            if leases:
                print(f"\n=== Active Leases ({len(leases)} shown) ===\n")
                for row in leases:
                    job_id, leased_by, expires = row
                    print(f"  {job_id}: {leased_by} (expires {expires})")
        except json.JSONDecodeError:
            pass

    print()


if __name__ == "__main__":
    main()
