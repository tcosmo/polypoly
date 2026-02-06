"""
Extract Polymarket user activity to CSV.

Usage:
    python user.py --user 0x1234...abcd                          # fetch all activity
    python user.py --user 0x1234...abcd --output my_activity.csv # custom output file
    python user.py --user 0x1234...abcd --limit 2000             # cap at 2000 rows
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
OFFSET_CEILING = 3000  # reset before hitting API offset cap via time-windowing


def fetch_activity(user: str, total: int | None = None) -> list[dict]:
    """Fetch user activity with pagination.

    Uses timestamp windowing to work around the API offset cap so any
    amount of history can be retrieved.

    Args:
        user: Polymarket user address (0x...).
        total: Maximum activities to fetch.  ``None`` means fetch all.

    Returns:
        List of activity dicts.
    """
    all_activities: list[dict] = []
    offset = 0
    end_ts: int | None = None  # upper-bound timestamp for current window

    while total is None or len(all_activities) < total:
        want = (total - len(all_activities)) if total else PAGE_SIZE
        limit = min(PAGE_SIZE, want)

        params: dict = {"user": user, "limit": limit, "offset": offset}
        if end_ts is not None:
            params["end"] = end_ts

        label = f"  Fetching offset={offset} limit={limit}"
        if end_ts is not None:
            label += f" end={end_ts}"
        print(label, end="\r", flush=True)

        try:
            resp = requests.get(ACTIVITY_API, params=params, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            # If offset is too large the API returns 400 — start a new
            # time window from the oldest timestamp we have so far.
            if resp.status_code == 400 and offset > 0 and all_activities:
                min_ts = min(
                    int(a["timestamp"])
                    for a in all_activities
                    if a.get("timestamp")
                )
                end_ts = min_ts - 1
                offset = 0
                continue
            raise

        batch = resp.json()
        if not isinstance(batch, list) or len(batch) == 0:
            break

        all_activities.extend(batch)

        if len(batch) < limit:
            break

        offset += limit

        # Proactively open a new time window before reaching the offset cap.
        if offset >= OFFSET_CEILING and batch:
            min_ts = min(int(a["timestamp"]) for a in batch if a.get("timestamp"))
            end_ts = min_ts - 1
            offset = 0

    print(f"  Fetched {len(all_activities)} activities total.          ")
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
        default=None,
        help="Max activities to fetch (default: all)",
    )

    args = parser.parse_args()

    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be a positive integer")

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
