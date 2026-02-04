"""
Runner script to spawn multiple events backfill workers in parallel.
"""
import argparse
import multiprocessing
import signal
import sys
from typing import Optional

from events_backfill.worker import EventsBackfillWorker


def run_worker(worker_id: str, max_jobs: Optional[int], idle_sleep: int):
    """Worker process entry point."""
    try:
        worker = EventsBackfillWorker(worker_id=worker_id)
        worker.run(max_jobs=max_jobs, idle_sleep=idle_sleep)
    except KeyboardInterrupt:
        print(f"[{worker_id}] Interrupted, exiting...")
    except Exception as e:
        print(f"[{worker_id}] Fatal error: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Run multiple events backfill workers")
    parser.add_argument(
        "-n", "--num-workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)"
    )
    parser.add_argument(
        "--max-jobs-per-worker",
        type=int,
        help="Max jobs per worker (default: unlimited)"
    )
    parser.add_argument(
        "--idle-sleep",
        type=int,
        default=30,
        help="Sleep seconds when no jobs available (default: 30)"
    )
    args = parser.parse_args()

    print(f"Starting {args.num_workers} workers...")

    # Handle Ctrl+C gracefully
    original_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)

    processes = []
    for i in range(args.num_workers):
        worker_id = f"worker-{i:02d}"
        p = multiprocessing.Process(
            target=run_worker,
            args=(worker_id, args.max_jobs_per_worker, args.idle_sleep),
            name=worker_id,
        )
        p.start()
        processes.append(p)

    signal.signal(signal.SIGINT, original_sigint)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nInterrupted, stopping workers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=5)
        sys.exit(1)

    print("All workers finished.")


if __name__ == "__main__":
    main()
