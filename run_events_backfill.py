#!/usr/bin/env python3
"""
Main entry point for events backfill operations.

Usage:
    python run_events_backfill.py setup     # Create and prefill the jobs table
    python run_events_backfill.py status    # Check job status
    python run_events_backfill.py worker    # Run a single worker
    python run_events_backfill.py run -n 4  # Run 4 parallel workers
    python run_events_backfill.py reset_leased  # Reset leased jobs back to pending
"""
import sys


def reset_leases():
    """Reset all leased jobs back to pending state."""
    from events_backfill.db_client import get_delta_client
    from events_backfill.config import JOBS_TABLE_PATH

    print("Resetting all leased jobs to pending...")
    client = get_delta_client()
    code = f'''
from delta.tables import DeltaTable

path = "{JOBS_TABLE_PATH}"
delta_table = DeltaTable.forPath(spark, path)

# Count leased jobs first
df = spark.read.format("delta").load(path)
leased_count = df.filter(df.state == "leased").count()

delta_table.update(
    condition="state = 'leased'",
    set={{
        "state": "'pending'",
        "leased_by": "NULL",
        "lease_expires_at": "NULL",
        "next_attempt_at": "current_timestamp()",
        "updated_at": "current_timestamp()"
    }}
)
print(f"Reset {{leased_count}} leased jobs to pending")
'''
    result = client.run_delta_operation(code)
    if result:
        print(result)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]
    sys.argv = sys.argv[1:]  # Remove script name from argv

    if command == "setup":
        from events_backfill.setup_table import main as setup_main
        setup_main()
    elif command == "status":
        from events_backfill.status import main as status_main
        status_main()
    elif command == "worker":
        from events_backfill.worker import main as worker_main
        worker_main()
    elif command == "run":
        from events_backfill.runner import main as runner_main
        runner_main()
    elif command == "reset_leased":
        reset_leases()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
