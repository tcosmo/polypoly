"""
Extract Polymarket price data to CSV.

Usage:
    # Single event
    python polypoly/extract.py <event-slug> --output data/prices.csv
    python polypoly/extract.py <event-slug> --list
    python polypoly/extract.py <event-slug> --market "bitcoin" -g 1h -o data/btc.csv

    # Bulk: all active events between two dates
    python polypoly/extract.py --bulk --from 2025-01-01 --to 2025-06-30 -o data/bulk/
    python polypoly/extract.py --bulk --from 2025-01-01 --to 2025-06-30 --dry-run
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

# Import from root backtest.py (polypoly/backtest.py)
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)
# Remove script dir so we don't shadow root backtest with backtesting/backtest.py
_script_dir = str(Path(__file__).resolve().parent)
if _script_dir in sys.path:
    sys.path.remove(_script_dir)
from backtest import (
    Timeline,
    PolymarketClient,
    EventMeta,
    GRANULARITY_SECONDS,
    GAMMA_API,
)


def extract_to_csv(
    slug: str,
    output: str,
    granularity: str = "1h",
    market_query: str = "",
    outcome_index: int = 0,
) -> None:
    """Fetch price data for a Polymarket event and save as CSV."""
    print(f"Fetching event: {slug}")
    tl = Timeline.from_slug(slug, granularity=granularity)

    market = tl._resolve_market(market_query)
    print(f"Market: {market.question}")
    print(f"Granularity: {granularity}")

    df = tl.to_dataframe(query=market_query, outcome_index=outcome_index)

    if df.empty:
        print("No price data available for this market.")
        sys.exit(1)

    # Keep only OHLC columns for the CSV
    df_out = df[["open", "high", "low", "close"]].copy()
    df_out.index = df_out.index.strftime("%Y-%m-%d %H:%M")
    df_out.index.name = "datetime"

    Path(output).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(output)
    print(f"Saved {len(df_out)} bars to {output}")
    print(f"  Range: {df_out.index[0]} -> {df_out.index[-1]}")


def list_markets(slug: str) -> None:
    """List all markets in an event."""
    tl = Timeline.from_slug(slug)
    e = tl.event
    print(f"\nEvent: {e.title}")
    print(f"  Slug: {e.slug}")
    print(f"  Markets ({len(e.markets)}):")
    print(f"  {'-'*60}")
    for i, m in enumerate(e.markets):
        prices = " / ".join(
            f"{o}={p:.2f}" for o, p in zip(m.outcomes, m.outcome_prices)
        )
        status = "CLOSED" if m.closed else "OPEN"
        print(f"  [{i}] {m.question}")
        print(f"      {prices}  [{status}]  vol={m.volume:,.0f}")
    print()


def _slugify(text: str) -> str:
    """Turn a string into a filesystem-safe slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text[:80]


def fetch_all_events(
    client: PolymarketClient,
    date_from: str | None = None,
    date_to: str | None = None,
    active: bool | None = None,
    page_size: int = 50,
) -> list[EventMeta]:
    """Paginate through Gamma API events with date filters.

    Args:
        date_from: minimum start date (YYYY-MM-DD or ISO)
        date_to: maximum end date (YYYY-MM-DD or ISO)
        active: filter by active status (None = all)
        page_size: events per page
    """
    all_events: list[EventMeta] = []
    offset = 0

    while True:
        params: dict = {"limit": page_size, "offset": offset}
        if date_from:
            params["start_date_min"] = (
                f"{date_from}T00:00:00Z" if "T" not in date_from else date_from
            )
        if date_to:
            params["end_date_max"] = (
                f"{date_to}T23:59:59Z" if "T" not in date_to else date_to
            )
        if active is not None:
            params["active"] = str(active).lower()

        data = client._get(f"{GAMMA_API}/events", params, cache=False)
        if not isinstance(data, list) or len(data) == 0:
            break

        for raw in data:
            all_events.append(EventMeta.from_api(raw))

        print(f"  Fetched {len(all_events)} events...", end="\r")

        if len(data) < page_size:
            break
        offset += page_size

    print(f"  Fetched {len(all_events)} events total.")
    return all_events


def bulk_extract(
    output_dir: str,
    granularity: str = "1h",
    date_from: str | None = None,
    date_to: str | None = None,
    active: bool | None = None,
    dry_run: bool = False,
) -> None:
    """Extract OHLC data for all events/markets matching the filters."""
    client = PolymarketClient()
    out_path = Path(output_dir)

    print(f"Bulk extract: {date_from or '...'} -> {date_to or '...'}")
    print(f"Granularity: {granularity}")
    print(f"Output dir: {out_path}")
    print()

    events = fetch_all_events(
        client, date_from=date_from, date_to=date_to, active=active
    )

    if not events:
        print("No events found with these filters.")
        return

    # Collect all (event, market) pairs
    pairs = []
    for event in events:
        for market in event.markets:
            pairs.append((event, market))

    print(f"\n{len(events)} events, {len(pairs)} markets total.")

    if dry_run:
        print("\n--- DRY RUN ---")
        for event, market in pairs:
            status = "CLOSED" if market.closed else "OPEN"
            fname = f"{_slugify(event.slug)}__{_slugify(market.slug)}.csv"
            print(f"  [{status}] {market.question}")
            print(f"    -> {out_path / fname}")
        print(f"\nWould extract {len(pairs)} CSV files.")
        return

    out_path.mkdir(parents=True, exist_ok=True)

    index = {}  # fname -> metadata
    extracted = 0
    skipped = 0

    for i, (event, market) in enumerate(pairs):
        fname = f"{_slugify(event.slug)}__{_slugify(market.slug)}.csv"
        csv_path = out_path / fname

        print(f"[{i+1}/{len(pairs)}] {market.question}")

        try:
            tl = Timeline(event, granularity=granularity, client=client)
            df = tl.to_dataframe(query=market.question)

            if df.empty:
                print(f"  -> skip (no data)")
                skipped += 1
                continue

            df_out = df[["open", "high", "low", "close"]].copy()
            df_out.index = df_out.index.strftime("%Y-%m-%d %H:%M")
            df_out.index.name = "datetime"
            df_out.to_csv(csv_path)

            index[fname] = {
                "event_slug": event.slug,
                "event_title": event.title,
                "market_slug": market.slug,
                "question": market.question,
                "end_date": market.end_date or event.end_date,
                "closed": market.closed,
                "outcomes": market.outcomes,
                "outcome_prices": market.outcome_prices,
                "volume": market.volume,
            }

            print(f"  -> {len(df_out)} bars -> {fname}")
            extracted += 1

        except Exception as e:
            print(f"  -> ERROR: {e}")
            skipped += 1

    # Save index
    index_path = out_path / "_index.json"
    index_path.write_text(json.dumps(index, indent=2))
    print(f"\nDone: {extracted} extracted, {skipped} skipped.")
    print(f"Index: {index_path}")


def main():
    parser = argparse.ArgumentParser(description="Extract Polymarket data to CSV")

    # Positional: event slug (optional if --bulk)
    parser.add_argument(
        "slug",
        nargs="?",
        default=None,
        help="Event slug (e.g. 'will-bitcoin-hit-100k'). Not needed with --bulk.",
    )

    # Single event options
    parser.add_argument("--output", "-o", default=None, help="Output CSV path or dir (for --bulk)")
    parser.add_argument(
        "--granularity",
        "-g",
        default="1h",
        choices=list(GRANULARITY_SECONDS.keys()),
        help="Bar granularity (default: 1h)",
    )
    parser.add_argument(
        "--market",
        "-m",
        default="",
        help="Market query (substring match on question/slug). Empty = first market.",
    )
    parser.add_argument(
        "--outcome",
        type=int,
        default=0,
        help="Outcome index (default: 0 = Yes/first)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all markets in the event and exit",
    )

    # Bulk extraction options
    parser.add_argument(
        "--bulk",
        action="store_true",
        help="Bulk extract all events/markets matching filters",
    )
    parser.add_argument("--from", dest="date_from", default=None, help="Start date filter (YYYY-MM-DD)")
    parser.add_argument("--to", dest="date_to", default=None, help="End date filter (YYYY-MM-DD)")
    parser.add_argument(
        "--active",
        default=None,
        choices=["true", "false"],
        help="Filter by active status (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be extracted without downloading",
    )

    args = parser.parse_args()

    # --- Bulk mode ---
    if args.bulk:
        output_dir = args.output or "data/bulk/"
        active = None
        if args.active == "true":
            active = True
        elif args.active == "false":
            active = False
        bulk_extract(
            output_dir=output_dir,
            granularity=args.granularity,
            date_from=args.date_from,
            date_to=args.date_to,
            active=active,
            dry_run=args.dry_run,
        )
        return

    # --- Single event mode ---
    if not args.slug:
        parser.error("slug is required (or use --bulk)")

    if args.list:
        list_markets(args.slug)
        return

    output = args.output or f"data/{args.slug}.csv"
    extract_to_csv(
        slug=args.slug,
        output=output,
        granularity=args.granularity,
        market_query=args.market,
        outcome_index=args.outcome,
    )


if __name__ == "__main__":
    main()
