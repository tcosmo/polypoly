"""
Polymarket API client for fetching events.
"""
import logging
import time
from typing import Optional

import requests

from events_refresh_new.config import POLYMARKET_API_BASE, REQUESTS_PER_SECOND

logger = logging.getLogger(__name__)


class PolymarketAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.min_request_interval = 1.0 / REQUESTS_PER_SECOND
        self.last_request_time = 0.0

    def fetch_events(self, offset: int, limit: int) -> tuple[list[dict], Optional[str]]:
        """
        Fetch events from Polymarket API with rate limiting.

        Returns:
            (events, error): events list and optional error string
        """
        # Rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            wait_time = self.min_request_interval - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.3f}s")
            time.sleep(wait_time)

        url = f"{POLYMARKET_API_BASE}/events"
        params = {"ascending": "true", "limit": limit, "offset": offset}

        logger.debug(f"Fetching offset={offset} limit={limit}")
        start = time.time()
        self.last_request_time = time.time()

        try:
            response = self.session.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            return [], f"REQUEST_ERROR:{str(e)[:100]}"

        fetch_time = time.time() - start
        logger.debug(f"API response: status={response.status_code} time={fetch_time:.2f}s size={len(response.content)}B")

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "60")
            logger.warning(f"Rate limited (429), retry after {retry_after}s")
            return [], f"HTTP_429:retry_after={retry_after}"

        if response.status_code != 200:
            logger.error(f"HTTP error: {response.status_code}")
            return [], f"HTTP_{response.status_code}"

        try:
            data = response.json()
            events = data if isinstance(data, list) else []
            logger.debug(f"Parsed {len(events)} events")
            return events, None
        except ValueError as e:
            logger.error(f"JSON decode failed: {e}")
            return [], f"JSON_ERROR:{str(e)[:100]}"
