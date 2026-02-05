#!/usr/bin/env python3
"""
CLI for events refresh operations.

Commands:
    setup   - Create state table and seed with initial offset
    status  - Show current state
    run     - Run one refresh cycle
"""
import argparse
import logging
import sys
from pathlib import Path

# Add polypoly_func to path so we can import from there
sys.path.insert(0, str(Path(__file__).parent / "polypoly_func"))


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    # Set events_refresh loggers to appropriate level
    logging.getLogger("events_refresh").setLevel(level)
    # Quiet down noisy libraries
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def cmd_setup(args):
    """Create state table and seed with initial offset."""
    from events_refresh_new.delta_state import create_table_with_seed, table_exists

    if table_exists() and not args.force:
        print("State table already exists. Use --force to recreate.")
        sys.exit(1)

    create_table_with_seed()
    print("State table created and seeded.")


def cmd_status(args):
    """Show current state."""
    from events_refresh_new.delta_state import get_status

    status = get_status()

    if not status["exists"]:
        print("State table does not exist. Run 'setup' first.")
        sys.exit(1)

    print(f"State Table Status")
    print(f"------------------")
    print(f"Total offsets tracked: {status['total_offsets']}")
    print(f"  Complete (full page): {status['complete']}")
    print(f"  Partial (< limit):    {status['partial']}")
    print(f"  Pending (not run):    {status['pending']}")
    print(f"  With errors:          {status['with_errors']}")
    print(f"")
    print(f"Total records fetched:  {status['total_records_fetched']}")
    print(f"Next offset to fetch:   {status['next_offset']}")


def cmd_run(args):
    """Run one refresh cycle."""
    from events_refresh_new.runner import run_refresh

    result = run_refresh(verbose=args.verbose)

    print(f"\nRefresh complete:")
    print(f"  Records fetched:    {result['total_records']}")
    print(f"  Offsets processed:  {result['offsets_processed']}")


def main():
    parser = argparse.ArgumentParser(
        description="Events refresh CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # setup command
    setup_parser = subparsers.add_parser("setup", help="Create state table and seed with initial offset")
    setup_parser.add_argument("--force", action="store_true", help="Recreate table if exists")

    # status command
    subparsers.add_parser("status", help="Show current state")

    # run command
    subparsers.add_parser("run", help="Run one refresh cycle")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    setup_logging(args.verbose)

    if args.command == "setup":
        cmd_setup(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "run":
        cmd_run(args)


if __name__ == "__main__":
    main()
