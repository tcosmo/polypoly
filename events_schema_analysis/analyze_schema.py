#!/usr/bin/env python3
"""
Analyze JSONL files to determine a consistent schema for Events, Markets, Series, and Tags.

Usage:
    # Local files
    python analyze_schema.py --local ./data_explore/*.jsonl

    # Azure ADLS (backfill only)
    python analyze_schema.py --adls --mode backfill --ingest-date 2026-02-05

    # Azure ADLS (refresh_new only)
    python analyze_schema.py --adls --mode refresh_new --ingest-date 2026-02-05

    # Azure ADLS (BOTH backfill and refresh_new)
    python analyze_schema.py --adls --mode all --ingest-date 2026-02-05

    # Azure ADLS (multiple ingest dates)
    python analyze_schema.py --adls --mode all --ingest-date 2026-02-05 --ingest-date 2026-02-04

    # Smoke test (only 5 files per folder)
    python analyze_schema.py --adls --mode all --ingest-date 2026-02-05 --max-files 5
"""

import json
import argparse
import time
from pathlib import Path
from collections import defaultdict
from typing import Any
import os

# Optional Azure imports
try:
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import DefaultAzureCredential
    from dotenv import load_dotenv
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False

# Script directory for relative paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent


def get_type_str(value: Any) -> str:
    """Get a string representation of the type."""
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "bool"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, str):
        return "str"
    elif isinstance(value, list):
        return "array"
    elif isinstance(value, dict):
        return "object"
    else:
        return type(value).__name__


def analyze_object(obj: dict, stats: dict, prefix: str = ""):
    """Recursively analyze an object and update stats."""
    for key, value in obj.items():
        full_key = f"{prefix}.{key}" if prefix else key
        type_str = get_type_str(value)

        stats[full_key]["count"] += 1
        stats[full_key]["types"][type_str] += 1

        # Store sample values (up to 3 unique)
        if type_str not in ("array", "object", "null") and len(stats[full_key]["samples"]) < 3:
            sample = str(value)[:100]  # truncate long strings
            if sample not in stats[full_key]["samples"]:
                stats[full_key]["samples"].add(sample)


def create_stats_dict():
    """Create a nested defaultdict for stats."""
    return defaultdict(lambda: {
        "count": 0,
        "types": defaultdict(int),
        "samples": set()
    })


def print_schema_report(name: str, stats: dict, total_count: int):
    """Print a schema report for an entity type."""
    print(f"\n{'='*80}")
    print(f" {name.upper()} SCHEMA ({total_count} records analyzed)")
    print(f"{'='*80}")

    # Sort by count descending
    sorted_fields = sorted(stats.items(), key=lambda x: -x[1]["count"])

    # Group by presence percentage
    always = []      # 100%
    common = []      # 90-99%
    frequent = []    # 50-89%
    rare = []        # <50%

    for field, data in sorted_fields:
        pct = (data["count"] / total_count) * 100 if total_count > 0 else 0
        types = dict(data["types"])
        samples = list(data["samples"])[:2]

        entry = (field, pct, types, samples)

        if pct >= 99.9:
            always.append(entry)
        elif pct >= 90:
            common.append(entry)
        elif pct >= 50:
            frequent.append(entry)
        else:
            rare.append(entry)

    def print_group(title: str, entries: list):
        if not entries:
            return
        print(f"\n{title}")
        print("-" * 60)
        for field, pct, types, samples in entries:
            type_str = ", ".join(f"{t}:{c}" for t, c in sorted(types.items(), key=lambda x: -x[1]))
            sample_str = f" e.g. {samples[0][:50]}" if samples else ""
            print(f"  {field:<45} {pct:5.1f}%  [{type_str}]{sample_str}")

    print_group("ALWAYS PRESENT (100%)", always)
    print_group("COMMON (90-99%)", common)
    print_group("FREQUENT (50-89%)", frequent)
    print_group("RARE (<50%)", rare)


def generate_sql_schema(name: str, stats: dict, total_count: int, threshold: float = 90.0) -> str:
    """Generate SQL CREATE TABLE statement for fields above threshold."""
    lines = [f"CREATE TABLE {name.lower()} ("]

    sorted_fields = sorted(stats.items(), key=lambda x: -x[1]["count"])

    field_defs = []
    for field, data in sorted_fields:
        pct = (data["count"] / total_count) * 100 if total_count > 0 else 0
        if pct < threshold:
            continue

        # Skip nested fields for main table
        if "." in field:
            continue

        types = data["types"]
        nullable = pct < 99.9 or "null" in types

        # Determine SQL type
        non_null_types = {t for t in types if t != "null"}

        if non_null_types == {"str"}:
            sql_type = "TEXT"
        elif non_null_types == {"int"}:
            sql_type = "BIGINT"
        elif non_null_types == {"float"}:
            sql_type = "DOUBLE"
        elif non_null_types == {"bool"}:
            sql_type = "BOOLEAN"
        elif non_null_types == {"int", "float"} or non_null_types == {"float"}:
            sql_type = "DOUBLE"
        elif non_null_types == {"array"}:
            sql_type = "TEXT"  # JSON array stored as text
        elif non_null_types == {"object"}:
            sql_type = "TEXT"  # JSON object stored as text
        elif "str" in non_null_types:
            sql_type = "TEXT"  # Mixed types, use text
        else:
            sql_type = "TEXT"

        null_str = "" if nullable else " NOT NULL"
        field_defs.append(f"    {field} {sql_type}{null_str}")

    lines.append(",\n".join(field_defs))
    lines.append(");")

    return "\n".join(lines)


def parse_content(content: str):
    """Generator that yields events from various JSON/JSONL formats."""
    content = content.strip()
    if not content:
        return

    # Handle different formats:
    # 1. JSON array: [{...}, {...}]
    # 2. JSONL with actual newlines
    # 3. Objects separated by literal \n (two chars: backslash + n)

    if content.startswith("["):
        # JSON array
        for event in json.loads(content):
            yield event
    elif r"}\n{" in content:
        # Objects separated by literal \n (backslash + n, not newline)
        parts = content.split(r"}\n{")
        for i, part in enumerate(parts):
            if i > 0:
                part = "{" + part
            if i < len(parts) - 1:
                part = part + "}"
            part = part.strip()
            if part:
                try:
                    yield json.loads(part)
                except json.JSONDecodeError:
                    pass
    elif "\n" in content:
        # Regular JSONL with actual newlines
        for line in content.split("\n"):
            line = line.strip()
            if line:
                yield json.loads(line)
    else:
        # Single JSON object
        yield json.loads(content)


def stream_local_files(file_paths: list[str]):
    """Generator that streams events from local JSONL files."""
    for path in file_paths:
        print(f"Reading {path}...")
        with open(path, "r") as f:
            content = f.read()
            yield from parse_content(content)


def read_local_files(file_paths: list[str]) -> list[dict]:
    """Read events from local JSONL files (legacy, loads all into memory)."""
    return list(stream_local_files(file_paths))


def stream_adls_files(mode: str, ingest_dates: list[str], max_files: int = 0):
    """Generator that streams events from Azure ADLS (memory-efficient).

    Args:
        mode: 'backfill', 'refresh_new', or 'all' (both)
        ingest_dates: List of ingest dates to scan (e.g., ['2026-02-05', '2026-02-04'])
        max_files: Max files to process per folder (0=unlimited)
    """
    if not AZURE_AVAILABLE:
        raise RuntimeError("Azure SDK not available. Install with: pip install azure-storage-file-datalake azure-identity python-dotenv")

    # Load .env from project root (parent of this script's directory)
    load_dotenv(PROJECT_ROOT / ".env")

    account_name = os.environ["ADLS_ACCOUNT_NAME"]

    # Use DefaultAzureCredential (works with az login, managed identity, etc.)
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential
    )

    container = "polypoly"
    fs_client = service_client.get_file_system_client(container)

    # Determine which modes to scan
    modes_to_scan = ["backfill", "refresh_new"] if mode == "all" else [mode]

    for m in modes_to_scan:
        for ingest_date in ingest_dates:
            prefix = f"bronze/polymarket/events/mode={m}/ingest_date={ingest_date}/"
            print(f"\nListing files in: {prefix}")

            try:
                paths = list(fs_client.get_paths(path=prefix))
                print(f"  Found {len(paths)} files")
            except Exception as e:
                print(f"  Error listing path: {e}")
                continue

            files_processed = 0
            for i, path in enumerate(paths):
                if path.name.endswith(".jsonl"):
                    if max_files > 0 and files_processed >= max_files:
                        print(f"  Reached max_files limit ({max_files}), skipping remaining...")
                        break
                    if files_processed % 50 == 0:
                        print(f"  Processing file {files_processed+1}/{len(paths)}...")
                    file_client = fs_client.get_file_client(path.name)
                    download = file_client.download_file()
                    content = download.readall().decode("utf-8")
                    yield from parse_content(content)
                    files_processed += 1


def read_adls_files(mode: str, ingest_dates: list[str]) -> list[dict]:
    """Read events from Azure ADLS (legacy, loads all into memory)."""
    return list(stream_adls_files(mode, ingest_dates))


def main():
    parser = argparse.ArgumentParser(description="Analyze JSONL schema")
    parser.add_argument("--local", nargs="*", help="Local JSONL files to analyze")
    parser.add_argument("--adls", action="store_true", help="Read from Azure ADLS")
    parser.add_argument("--mode", choices=["backfill", "refresh_new", "all"], default="all",
                        help="Mode to scan: backfill, refresh_new, or all (both)")
    parser.add_argument("--ingest-date", action="append", dest="ingest_dates",
                        help="Ingest date(s) to scan (can be repeated)")
    parser.add_argument("--sql", action="store_true", help="Generate SQL schema")
    parser.add_argument("--threshold", type=float, default=90.0, help="Min %% for SQL fields")
    parser.add_argument("--output", help="Output file for results")
    parser.add_argument("--max-files", type=int, default=0, help="Max files per folder (0=unlimited, for smoke testing)")

    args = parser.parse_args()

    # Default ingest date if not specified
    if not args.ingest_dates:
        args.ingest_dates = ["2026-02-05"]

    # Get event stream (memory-efficient generator)
    if args.local:
        from glob import glob
        files = []
        for pattern in args.local:
            files.extend(glob(pattern))
        event_stream = stream_local_files(files)
    elif args.adls:
        event_stream = stream_adls_files(args.mode, args.ingest_dates, args.max_files)
    else:
        # Default: read from data_explore
        from glob import glob
        files = glob("data_explore/*.jsonl")
        if not files:
            print("No files found. Use --local or --adls")
            return
        event_stream = stream_local_files(files)

    # Initialize stats for all entity types
    event_stats = create_stats_dict()
    market_stats = create_stats_dict()
    series_stats = create_stats_dict()
    tag_stats = create_stats_dict()
    category_stats = create_stats_dict()
    collection_stats = create_stats_dict()
    chat_stats = create_stats_dict()
    event_creator_stats = create_stats_dict()
    template_stats = create_stats_dict()
    image_optimized_stats = create_stats_dict()

    event_count = 0
    market_count = 0
    series_count = 0
    tag_count = 0
    category_count = 0
    collection_count = 0
    chat_count = 0
    event_creator_count = 0
    template_count = 0
    image_optimized_count = 0

    # Track unique entities
    unique_market_slugs = set()
    unique_series_slugs = set()
    unique_tag_slugs = set()
    unique_creator_names = set()

    # Time tracking for ETA
    start_time = time.time()
    last_report_time = start_time

    # Stream and analyze each event (memory-efficient)
    print("\nAnalyzing events (streaming)...")
    for event in event_stream:
        event_count += 1

        # Progress with ETA every 5000 events
        if event_count % 5000 == 0:
            elapsed = time.time() - start_time
            rate = event_count / elapsed if elapsed > 0 else 0
            # Estimate total based on ~188K events for full backfill
            estimated_total = 188000
            remaining = estimated_total - event_count
            eta_seconds = remaining / rate if rate > 0 else 0
            eta_min = int(eta_seconds // 60)
            eta_sec = int(eta_seconds % 60)
            print(f"  Processed {event_count:,} events... ({rate:.1f}/s, ETA: {eta_min}m {eta_sec}s)")

        # Analyze event fields (excluding nested arrays)
        event_flat = {k: v for k, v in event.items() if k not in ("markets", "series", "tags", "categories", "collections", "chats", "eventCreators", "templates")}
        analyze_object(event_flat, event_stats)

        # Track presence of nested arrays on events
        for nested_field in ("markets", "series", "tags", "categories", "collections", "chats", "eventCreators", "templates"):
            if nested_field in event:
                val = event[nested_field]
                event_stats[nested_field]["count"] += 1
                if val is None:
                    event_stats[nested_field]["types"]["null"] += 1
                elif isinstance(val, list):
                    event_stats[nested_field]["types"]["array"] += 1
                    if len(event_stats[nested_field]["samples"]) < 3:
                        event_stats[nested_field]["samples"].add(f"len={len(val)}")

        # Analyze markets
        for market in event.get("markets", []):
            market_flat = {k: v for k, v in market.items() if k not in ("events", "categories", "tags", "imageOptimized", "iconOptimized")}
            analyze_object(market_flat, market_stats)
            market_count += 1
            if "slug" in market:
                unique_market_slugs.add(market["slug"])

            # Track presence of nested arrays/objects (not their contents, just if they exist)
            for nested_field in ("events", "categories", "tags", "imageOptimized", "iconOptimized"):
                if nested_field in market:
                    val = market[nested_field]
                    market_stats[nested_field]["count"] += 1
                    if val is None:
                        market_stats[nested_field]["types"]["null"] += 1
                    elif isinstance(val, list):
                        market_stats[nested_field]["types"]["array"] += 1
                        if len(market_stats[nested_field]["samples"]) < 3:
                            market_stats[nested_field]["samples"].add(f"len={len(val)}")
                    elif isinstance(val, dict):
                        market_stats[nested_field]["types"]["object"] += 1

            # Analyze market's imageOptimized/iconOptimized
            for img_field in ("imageOptimized", "iconOptimized"):
                if img_field in market and market[img_field]:
                    analyze_object(market[img_field], image_optimized_stats)
                    image_optimized_count += 1

            # Analyze market's categories
            for cat in market.get("categories", []):
                analyze_object(cat, category_stats)
                category_count += 1

            # Analyze market's tags
            for tag in market.get("tags", []):
                analyze_object(tag, tag_stats)
                tag_count += 1
                if "slug" in tag:
                    unique_tag_slugs.add(tag["slug"])

        # Analyze series
        for s in event.get("series", []):
            series_flat = {k: v for k, v in s.items() if k not in ("events", "collections", "categories", "tags", "chats")}
            analyze_object(series_flat, series_stats)
            series_count += 1
            if "slug" in s:
                unique_series_slugs.add(s["slug"])

            # Track presence of nested arrays on series
            for nested_field in ("events", "collections", "categories", "tags", "chats"):
                if nested_field in s:
                    val = s[nested_field]
                    series_stats[nested_field]["count"] += 1
                    if val is None:
                        series_stats[nested_field]["types"]["null"] += 1
                    elif isinstance(val, list):
                        series_stats[nested_field]["types"]["array"] += 1
                        if len(series_stats[nested_field]["samples"]) < 3:
                            series_stats[nested_field]["samples"].add(f"len={len(val)}")

            # Analyze series' collections
            for coll in s.get("collections", []):
                coll_flat = {k: v for k, v in coll.items() if k not in ("events", "series", "categories", "tags")}
                analyze_object(coll_flat, collection_stats)
                collection_count += 1

                # Track presence of nested arrays on collections
                for nested_field in ("events", "series", "categories", "tags"):
                    if nested_field in coll:
                        val = coll[nested_field]
                        collection_stats[nested_field]["count"] += 1
                        if val is None:
                            collection_stats[nested_field]["types"]["null"] += 1
                        elif isinstance(val, list):
                            collection_stats[nested_field]["types"]["array"] += 1
                            if len(collection_stats[nested_field]["samples"]) < 3:
                                collection_stats[nested_field]["samples"].add(f"len={len(val)}")

            # Analyze series' categories
            for cat in s.get("categories", []):
                analyze_object(cat, category_stats)
                category_count += 1

            # Analyze series' tags
            for tag in s.get("tags", []):
                analyze_object(tag, tag_stats)
                tag_count += 1
                if "slug" in tag:
                    unique_tag_slugs.add(tag["slug"])

            # Analyze series' chats
            for chat in s.get("chats", []):
                analyze_object(chat, chat_stats)
                chat_count += 1

        # Analyze event-level tags
        for tag in event.get("tags", []):
            analyze_object(tag, tag_stats)
            tag_count += 1
            if "slug" in tag:
                unique_tag_slugs.add(tag["slug"])

        # Analyze event-level categories
        for cat in event.get("categories", []):
            analyze_object(cat, category_stats)
            category_count += 1

        # Analyze event-level collections
        for coll in event.get("collections", []):
            coll_flat = {k: v for k, v in coll.items() if k not in ("events", "series", "categories", "tags")}
            analyze_object(coll_flat, collection_stats)
            collection_count += 1

            # Track presence of nested arrays on collections
            for nested_field in ("events", "series", "categories", "tags"):
                if nested_field in coll:
                    val = coll[nested_field]
                    collection_stats[nested_field]["count"] += 1
                    if val is None:
                        collection_stats[nested_field]["types"]["null"] += 1
                    elif isinstance(val, list):
                        collection_stats[nested_field]["types"]["array"] += 1
                        if len(collection_stats[nested_field]["samples"]) < 3:
                            collection_stats[nested_field]["samples"].add(f"len={len(val)}")

        # Analyze event-level chats
        for chat in event.get("chats", []):
            analyze_object(chat, chat_stats)
            chat_count += 1

        # Analyze eventCreators
        for creator in event.get("eventCreators", []):
            analyze_object(creator, event_creator_stats)
            event_creator_count += 1
            if "creatorName" in creator:
                unique_creator_names.add(creator["creatorName"])

        # Analyze templates
        for tmpl in event.get("templates", []):
            analyze_object(tmpl, template_stats)
            template_count += 1

    elapsed_total = time.time() - start_time
    print(f"\nProcessed {event_count:,} events in {elapsed_total:.1f}s ({event_count/elapsed_total:.1f} events/s)")
    print(f"\nEntity counts:")
    print(f"  Events:         {event_count:,}")
    print(f"  Markets:        {market_count:,} ({len(unique_market_slugs):,} unique)")
    print(f"  Series:         {series_count:,} ({len(unique_series_slugs):,} unique)")
    print(f"  Tags:           {tag_count:,} ({len(unique_tag_slugs):,} unique)")
    print(f"  Categories:     {category_count:,}")
    print(f"  Collections:    {collection_count:,}")
    print(f"  Chats:          {chat_count:,}")
    print(f"  EventCreators:  {event_creator_count:,} ({len(unique_creator_names):,} unique)")
    print(f"  Templates:      {template_count:,}")
    print(f"  ImageOptimized: {image_optimized_count:,}")

    # Print reports
    output_lines = []

    def log(msg: str):
        output_lines.append(msg)
        print(msg)

    def print_schema_report_to_log(name: str, stats: dict, total_count: int):
        log(f"\n{'='*80}")
        log(f" {name.upper()} SCHEMA ({total_count} records analyzed)")
        log(f"{'='*80}")

        sorted_fields = sorted(stats.items(), key=lambda x: -x[1]["count"])

        always, common, frequent, rare = [], [], [], []

        for field, data in sorted_fields:
            pct = (data["count"] / total_count) * 100 if total_count > 0 else 0
            types = dict(data["types"])
            samples = list(data["samples"])[:2]
            entry = (field, pct, types, samples)

            if pct >= 99.9:
                always.append(entry)
            elif pct >= 90:
                common.append(entry)
            elif pct >= 50:
                frequent.append(entry)
            else:
                rare.append(entry)

        def print_group(title: str, entries: list):
            if not entries:
                return
            log(f"\n{title}")
            log("-" * 60)
            for field, pct, types, samples in entries:
                type_str = ", ".join(f"{t}:{c}" for t, c in sorted(types.items(), key=lambda x: -x[1]))
                sample_str = f" e.g. {samples[0][:50]}" if samples else ""
                log(f"  {field:<45} {pct:5.1f}%  [{type_str}]{sample_str}")

        print_group("ALWAYS PRESENT (100%)", always)
        print_group("COMMON (90-99%)", common)
        print_group("FREQUENT (50-89%)", frequent)
        print_group("RARE (<50%)", rare)

    print_schema_report_to_log("Event", event_stats, event_count)
    print_schema_report_to_log("Market", market_stats, market_count)
    print_schema_report_to_log("Series", series_stats, series_count)
    print_schema_report_to_log("Tag", tag_stats, tag_count)
    print_schema_report_to_log("Category", category_stats, category_count)
    print_schema_report_to_log("Collection", collection_stats, collection_count)
    print_schema_report_to_log("Chat", chat_stats, chat_count)
    print_schema_report_to_log("EventCreator", event_creator_stats, event_creator_count)
    print_schema_report_to_log("Template", template_stats, template_count)
    print_schema_report_to_log("ImageOptimized", image_optimized_stats, image_optimized_count)

    if args.sql:
        log("\n" + "="*80)
        log(" SQL SCHEMAS (fields present in >= {:.0f}% of records)".format(args.threshold))
        log("="*80)

        log("\n-- Events")
        log(generate_sql_schema("events", event_stats, event_count, args.threshold))

        log("\n-- Markets")
        log(generate_sql_schema("markets", market_stats, market_count, args.threshold))

        log("\n-- Series")
        log(generate_sql_schema("series", series_stats, series_count, args.threshold))

        log("\n-- Tags")
        log(generate_sql_schema("tags", tag_stats, tag_count, args.threshold))

        if category_count > 0:
            log("\n-- Categories")
            log(generate_sql_schema("categories", category_stats, category_count, args.threshold))

        if collection_count > 0:
            log("\n-- Collections")
            log(generate_sql_schema("collections", collection_stats, collection_count, args.threshold))

        if chat_count > 0:
            log("\n-- Chats")
            log(generate_sql_schema("chats", chat_stats, chat_count, args.threshold))

        if event_creator_count > 0:
            log("\n-- EventCreators")
            log(generate_sql_schema("event_creators", event_creator_stats, event_creator_count, args.threshold))

        if template_count > 0:
            log("\n-- Templates")
            log(generate_sql_schema("templates", template_stats, template_count, args.threshold))

        if image_optimized_count > 0:
            log("\n-- ImageOptimized")
            log(generate_sql_schema("image_optimized", image_optimized_stats, image_optimized_count, args.threshold))

    if args.output:
        with open(args.output, "w") as f:
            f.write("\n".join(output_lines))
        print(f"\nResults written to {args.output}")


if __name__ == "__main__":
    main()
