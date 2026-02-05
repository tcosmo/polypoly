"""
Events refresh runner - main logic for fetching new events.

Job Logic (runs every 5 minutes):
1. Compute next_offset from state table
2. Loop:
   a. Fetch events at next_offset with limit=20
   b. If API error → upsert error, break
   c. If count == 0 → caught up, break
   d. Write JSONL to bronze
   e. Upsert state row
   f. If count == 20 → next_offset += 20, continue loop
   g. If count < 20 → caught up, break
"""
import logging

from events_refresh_new.api import PolymarketAPIClient
from events_refresh_new.bronze_writer import BronzeWriter
from events_refresh_new.config import LIMIT
from events_refresh_new.delta_state import (
    get_next_offset,
    get_previous_count,
    upsert_error,
    upsert_state,
)

logger = logging.getLogger(__name__)


def run_refresh(verbose: bool = False) -> dict:
    """
    Run one refresh cycle.

    Returns:
        dict with total_records and offsets_processed
    """
    if verbose:
        logging.getLogger("events_refresh").setLevel(logging.DEBUG)

    logger.info("=== EVENTS REFRESH START ===")

    api_client = PolymarketAPIClient()
    bronze_writer = BronzeWriter()

    total_records = 0
    offsets_processed = 0
    next_offset = get_next_offset()

    logger.debug(f"Starting at offset={next_offset}")

    while True:
        logger.debug(f"Fetching offset={next_offset} limit={LIMIT}")

        # Fetch events
        events, error = api_client.fetch_events(next_offset, LIMIT)

        if error:
            logger.error(f"API error at offset={next_offset}: {error}")
            upsert_error(next_offset, error)
            break

        count = len(events)
        logger.debug(f"API returned {count} records")

        # If empty, we're caught up
        if count == 0:
            logger.info(f"Caught up at offset={next_offset} (empty response)")
            break

        # Check if count changed from previous run (avoid writing duplicate files)
        previous_count = get_previous_count(next_offset)
        if previous_count is not None and count == previous_count:
            logger.info(f"Caught up at offset={next_offset} (count unchanged: {count})")
            break

        # Write to bronze (only if count changed)
        bronze_path = bronze_writer.write_events(events, next_offset, LIMIT)
        logger.debug(f"Wrote bronze file: {bronze_path}")

        # Update state
        upsert_state(next_offset, count, bronze_path)
        logger.debug(f"Upserted state for offset={next_offset}")

        total_records += count
        offsets_processed += 1

        # If full page, continue to next offset
        if count == LIMIT:
            next_offset += LIMIT
            continue

        # Partial page - we're at the end
        logger.info(f"Caught up at offset={next_offset} (partial page: {count} records)")
        break

    result = {
        "total_records": total_records,
        "offsets_processed": offsets_processed,
    }

    logger.info(f"=== EVENTS REFRESH COMPLETE === records_fetched={total_records} offsets_processed={offsets_processed}")

    return result
