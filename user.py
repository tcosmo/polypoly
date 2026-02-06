"""
Extract Polymarket user activity to CSV.

Usage:
    python user.py --user 0x1234...abcd
    python user.py --user 0x1234...abcd --output my_activity.csv --limit 2000
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone

import requests

ACTIVITY_API = "https://data-api.polymarket.com/activity"

# Clean output columns matching the Polymarket Activity tab
OUTPUT_FIELDS = ["type", "market", "outcome", "price", "shares", "amount", "date"]

PAGE_SIZE = 500  # API max per request


def fetch_activity(user: str, total: int = 1000) -> list[dict]:
    """Fetch user activity with pagination.

    Args:
        user: Polymarket user address (0x...).
        total: Total number of activities to fetch (up to 10000).

    Returns:
        List of activity dicts.
    """
    all_activities: list[dict] = []
    offset = 0

    while offset < total:
        limit = min(PAGE_SIZE, total - offset)
        params = {"user": user, "limit": limit, "offset": offset}

        print(f"  Fetching offset={offset}, limit={limit}...", end="\r")
        resp = requests.get(ACTIVITY_API, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()

        if not isinstance(batch, list) or len(batch) == 0:
            break

        all_activities.extend(batch)

        if len(batch) < limit:
            break

        offset += limit

    print(f"  Fetched {len(all_activities)} activities total.")
    return all_activities


def format_row(raw: dict) -> dict:
    """Transform a raw API activity into a clean output row."""
    ts = raw.get("timestamp")
    if ts:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        date_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        date_str = ""

    price = raw.get("price")
    price_str = f"{float(price):.2f}" if price else ""

    size = raw.get("size")
    size_str = f"{float(size):.1f}" if size else ""

    usdc = raw.get("usdcSize")
    amount_str = f"{float(usdc):.2f}" if usdc else ""

    return {
        "type": raw.get("type", ""),
        "market": raw.get("title", ""),
        "outcome": raw.get("outcome", ""),
        "price": price_str,
        "shares": size_str,
        "amount": amount_str,
        "date": date_str,
    }


def write_csv(activities: list[dict], output: str) -> None:
    """Write activities to a CSV file."""
    rows = [format_row(a) for a in activities]
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Extract Polymarket user activity to CSV"
    )
    parser.add_argument(
        "--user", required=True, help="Polymarket user address (0x...)"
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output CSV filename (default: <user_address>.csv)",
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=1000,
        help="Total activities to fetch, up to 10000 (default: 1000)",
    )

    args = parser.parse_args()

    if args.limit < 1 or args.limit > 10000:
        parser.error("--limit must be between 1 and 10000")

    output = args.output or f"{args.user}.csv"

    print(f"Fetching activity for {args.user}")
    activities = fetch_activity(args.user, total=args.limit)

    if not activities:
        print("No activity found for this user.")
        sys.exit(1)

    write_csv(activities, output)
    print(f"Saved {len(activities)} activities to {output}")


if __name__ == "__main__":
    main()
