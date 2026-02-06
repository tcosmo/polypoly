"""
Polymarket backtesting environment.

Fetch event metadata (Gamma API or local JSONL), historical price data (CLOB API),
aggregate into configurable-granularity bars, and replay for backtesting.

Usage:
    python backtest.py <event-slug> [granularity]

    from backtest import Timeline, search, load
    tl = Timeline.from_slug("some-event-slug", granularity="1h")
    tl.summary()
    bars = tl.bars("market question substring")
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

GRANULARITY_SECONDS = {
    "1s": 1, "5s": 5, "10s": 10, "30s": 30,
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "4h": 14400, "1d": 86400,
}

CACHE_DIR = Path.home() / ".cache" / "polymarket_backtest"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_ts(val: Any) -> Optional[int]:
    """Accept unix seconds (int/float/str), ISO datetime, or YYYY-MM-DD."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip()
    if not s:
        return None
    # Pure numeric
    try:
        return int(float(s))
    except ValueError:
        pass
    # ISO / date string
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------
@dataclass
class Bar:
    """Single time bucket aggregation (OHLC + optional trade data)."""
    t_start: int
    t_end: int
    price_open: float
    price_close: float
    price_high: float
    price_low: float
    tick_count: int
    price_vwap: Optional[float] = None
    trade_count: Optional[int] = None
    volume: Optional[float] = None
    volume_buy: Optional[float] = None
    volume_sell: Optional[float] = None
    price_change: float = field(init=False)
    price_change_pct: float = field(init=False)

    def __post_init__(self):
        self.price_change = self.price_close - self.price_open
        if self.price_open != 0:
            self.price_change_pct = self.price_change / self.price_open
        else:
            self.price_change_pct = 0.0

    @property
    def has_trades(self) -> bool:
        return self.volume is not None

    def __repr__(self):
        ts = datetime.fromtimestamp(self.t_start, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        return (f"Bar({ts} O={self.price_open:.4f} H={self.price_high:.4f} "
                f"L={self.price_low:.4f} C={self.price_close:.4f} ticks={self.tick_count})")


@dataclass
class MarketMeta:
    """Parsed market metadata."""
    market_id: str
    question: str
    condition_id: str
    slug: str
    outcomes: list[str]
    token_ids: list[str]
    volume: float
    end_date: Optional[str]
    closed: bool
    outcome_prices: list[float]

    @classmethod
    def from_api(cls, raw: dict) -> MarketMeta:
        outcomes = raw.get("outcomes", "[]")
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)

        token_ids = raw.get("clobTokenIds", "[]")
        if isinstance(token_ids, str):
            token_ids = json.loads(token_ids)

        prices = raw.get("outcomePrices", "[]")
        if isinstance(prices, str):
            prices = json.loads(prices)
        prices = [float(p) for p in prices]

        vol_raw = raw.get("volumeNum") or raw.get("volume") or 0
        return cls(
            market_id=str(raw.get("id", "")),
            question=raw.get("question", ""),
            condition_id=raw.get("conditionId", ""),
            slug=raw.get("slug", ""),
            outcomes=outcomes,
            token_ids=token_ids,
            volume=float(vol_raw),
            end_date=raw.get("endDate"),
            closed=bool(raw.get("closed", False)),
            outcome_prices=prices,
        )

    def token_id_for(self, outcome_name: str) -> Optional[str]:
        """Return the token ID for a named outcome (case-insensitive)."""
        lower = outcome_name.lower()
        for i, name in enumerate(self.outcomes):
            if name.lower() == lower and i < len(self.token_ids):
                return self.token_ids[i]
        return None


@dataclass
class EventMeta:
    """Parsed event metadata."""
    event_id: str
    title: str
    slug: str
    description: str
    neg_risk: bool
    closed: bool
    volume: float
    markets: list[MarketMeta]
    tags: list[str]
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @classmethod
    def from_api(cls, raw: dict) -> EventMeta:
        markets = [MarketMeta.from_api(m) for m in raw.get("markets", [])]
        tags_raw = raw.get("tags", [])
        if isinstance(tags_raw, list):
            tags = [t.get("label", "") if isinstance(t, dict) else str(t) for t in tags_raw]
        else:
            tags = []

        vol_raw = raw.get("volume") or 0
        try:
            vol = float(vol_raw)
        except (ValueError, TypeError):
            vol = 0.0

        return cls(
            event_id=str(raw.get("id", "")),
            title=raw.get("title", ""),
            slug=raw.get("slug", ""),
            description=raw.get("description", ""),
            neg_risk=bool(raw.get("negRisk", False)),
            closed=bool(raw.get("closed", False)),
            volume=vol,
            markets=markets,
            tags=tags,
            start_date=raw.get("startDate"),
            end_date=raw.get("endDate"),
        )

    def find_market(self, query: str) -> Optional[MarketMeta]:
        """Find market by substring match on question, slug, or market_id."""
        q = query.lower()
        for m in self.markets:
            if (q in m.question.lower() or q in m.slug.lower()
                    or q == m.market_id.lower()):
                return m
        # Fallback: return first market if query is empty or matches event
        if not query or q in self.title.lower() or q in self.slug.lower():
            return self.markets[0] if self.markets else None
        return None


# ---------------------------------------------------------------------------
# Polymarket Client
# ---------------------------------------------------------------------------
class PolymarketClient:
    """Thin HTTP client with rate limiting and disk cache."""

    def __init__(self, requests_per_second: float = 10.0, use_cache: bool = True):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        })
        self.min_interval = 1.0 / requests_per_second
        self._last_request = 0.0
        self.use_cache = use_cache
        if use_cache:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request = time.time()

    def _cache_key(self, url: str, params: dict) -> str:
        raw = json.dumps({"url": url, "params": params}, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

    def _cache_get(self, key: str) -> Optional[Any]:
        if not self.use_cache:
            return None
        path = CACHE_DIR / f"{key}.json"
        if path.exists():
            return json.loads(path.read_text())
        return None

    def _cache_put(self, key: str, data: Any):
        if not self.use_cache:
            return
        path = CACHE_DIR / f"{key}.json"
        path.write_text(json.dumps(data))

    def _get(self, url: str, params: dict | None = None, cache: bool = True) -> Any:
        params = params or {}
        if cache:
            key = self._cache_key(url, params)
            cached = self._cache_get(key)
            if cached is not None:
                return cached

        self._rate_limit()
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if cache:
            self._cache_put(key, data)
        return data

    def get_event(self, slug: str | None = None, event_id: str | None = None) -> EventMeta:
        """Fetch single event from Gamma API."""
        params: dict[str, Any] = {}
        if event_id:
            params["id"] = event_id
        elif slug:
            params["slug"] = slug
        else:
            raise ValueError("Provide either slug or event_id")

        data = self._get(f"{GAMMA_API}/events", params)
        if isinstance(data, list):
            if not data:
                raise ValueError(f"No event found for {params}")
            return EventMeta.from_api(data[0])
        return EventMeta.from_api(data)

    def search_events(self, query: str = "", active: bool | None = None,
                      limit: int = 20) -> list[EventMeta]:
        """Search events via Gamma API.

        Uses tag_slug for keyword search, then filters results client-side
        by title/slug substring match. Fetches extra results to compensate
        for client-side filtering.
        """
        params: dict[str, Any] = {"limit": min(limit * 5, 100)}
        if query:
            params["tag_slug"] = query.lower().replace(" ", "-")
        if active is not None:
            params["active"] = str(active).lower()
        data = self._get(f"{GAMMA_API}/events", params, cache=False)
        if not isinstance(data, list):
            return []
        events = [EventMeta.from_api(e) for e in data]
        # Client-side filter if query provided
        if query:
            q = query.lower()
            filtered = [e for e in events
                        if q in e.title.lower() or q in e.slug.lower()
                        or any(q in t.lower() for t in e.tags)]
            return filtered[:limit] if filtered else events[:limit]
        return events[:limit]

    def get_price_history(self, token_id: str, start_ts: int | None = None,
                          end_ts: int | None = None, interval: str | None = None,
                          fidelity: int | None = None) -> list[dict]:
        """Fetch price history from CLOB API.

        Returns list of {"t": unix, "p": price_str}.

        CLOB API rules (empirically verified):
          - fidelity is in minutes (1=1min, 60=1h)
          - startTs alone: no duration limit, fidelity respected -> RECOMMENDED
          - startTs + endTs: max 15 days apart, HTTP 400 beyond
          - interval: degrades fidelity (fidelity=1 gives 12h instead of 1min)
          - endTs alone: HTTP 400
        """
        import warnings

        if interval is not None:
            warnings.warn(
                "get_price_history: `interval` degrades fidelity resolution "
                "(e.g. fidelity=1 gives 12h instead of 1min). "
                "Use `start_ts` alone instead.",
                stacklevel=2,
            )

        if end_ts is not None and start_ts is None:
            raise ValueError(
                "get_price_history: `end_ts` alone returns HTTP 400. "
                "Pass `start_ts` (with or without `end_ts`)."
            )

        if start_ts is not None and end_ts is not None:
            span_days = (end_ts - start_ts) / 86400
            if span_days > 15:
                raise ValueError(
                    f"get_price_history: startTs + endTs span is {span_days:.0f} days "
                    f"(max 15). The API returns HTTP 400 beyond 15 days. "
                    f"Use `start_ts` alone (without `end_ts`) for unlimited range."
                )

        params: dict[str, Any] = {"market": token_id}
        if start_ts is not None:
            params["startTs"] = start_ts
        if end_ts is not None:
            params["endTs"] = end_ts
        if interval:
            params["interval"] = interval
        if fidelity is not None:
            params["fidelity"] = fidelity

        data = self._get(f"{CLOB_API}/prices-history", params)
        history = data.get("history", []) if isinstance(data, dict) else []
        return history

    def get_order_book(self, token_id: str) -> dict:
        """Fetch live order book (not cached)."""
        return self._get(f"{CLOB_API}/book", {"token_id": token_id}, cache=False)

    @staticmethod
    def load_jsonl(path: str | Path) -> list[dict]:
        """Load events from JSONL file using raw_decode (separator-agnostic)."""
        content = Path(path).read_text(encoding="utf-8")
        decoder = json.JSONDecoder()
        results = []
        idx = 0
        length = len(content)
        while idx < length:
            # Skip whitespace
            while idx < length and content[idx] in " \t\r\n":
                idx += 1
            if idx >= length:
                break
            try:
                obj, end = decoder.raw_decode(content, idx)
                results.append(obj)
                idx = end
            except json.JSONDecodeError:
                # Try skipping literal \n separator
                if content[idx:idx + 2] == "\\n":
                    idx += 2
                    continue
                idx += 1
        return results


# ---------------------------------------------------------------------------
# Aggregation Engine
# ---------------------------------------------------------------------------
def _fidelity_for_granularity(granularity: str) -> int:
    """Map granularity to CLOB API fidelity param (minutes, minimum 1).

    fidelity=1 works fine with startTs/endTs or startTs alone.
    It only bugs out when combined with interval=max (gives 12h instead of 1min).
    Since our fallback uses startTs without interval, fidelity=1 is safe.
    """
    secs = GRANULARITY_SECONDS.get(granularity, 3600)
    return max(1, secs // 60)


def aggregate_price_bars(history: list[dict], granularity: str,
                         start_ts: int | None = None,
                         end_ts: int | None = None) -> list[Bar]:
    """Aggregate {"t","p"} price ticks into OHLC bars at given granularity."""
    if not history:
        return []

    bucket_secs = GRANULARITY_SECONDS.get(granularity)
    if bucket_secs is None:
        raise ValueError(f"Unknown granularity: {granularity}. Choose from: {list(GRANULARITY_SECONDS)}")

    # Parse and filter
    points: list[tuple[int, float]] = []
    for pt in history:
        t = int(pt["t"])
        p = float(pt["p"])
        if start_ts and t < start_ts:
            continue
        if end_ts and t > end_ts:
            continue
        points.append((t, p))

    if not points:
        return []

    points.sort(key=lambda x: x[0])

    # Group into aligned buckets
    buckets: dict[int, list[float]] = {}
    for t, p in points:
        bucket_start = (t // bucket_secs) * bucket_secs
        buckets.setdefault(bucket_start, []).append(p)

    bars = []
    for bstart in sorted(buckets):
        prices = buckets[bstart]
        bars.append(Bar(
            t_start=bstart,
            t_end=bstart + bucket_secs,
            price_open=prices[0],
            price_close=prices[-1],
            price_high=max(prices),
            price_low=min(prices),
            tick_count=len(prices),
        ))
    return bars


def aggregate_trade_bars(trades: list[dict], granularity: str,
                         start_ts: int | None = None,
                         end_ts: int | None = None) -> list[Bar]:
    """Aggregate trade records into OHLC + volume bars (for future trade data).

    Trade fields: t/timestamp, p/price, s/size, side (BUY/SELL).
    """
    if not trades:
        return []

    bucket_secs = GRANULARITY_SECONDS.get(granularity)
    if bucket_secs is None:
        raise ValueError(f"Unknown granularity: {granularity}")

    # Normalize field names
    parsed: list[tuple[int, float, float, str]] = []
    for tr in trades:
        t = int(tr.get("t") or tr.get("timestamp") or 0)
        p = float(tr.get("p") or tr.get("price") or 0)
        s = float(tr.get("s") or tr.get("size") or 0)
        side = str(tr.get("side", "")).upper()
        if start_ts and t < start_ts:
            continue
        if end_ts and t > end_ts:
            continue
        parsed.append((t, p, s, side))

    if not parsed:
        return []

    parsed.sort(key=lambda x: x[0])

    # Group into buckets
    buckets: dict[int, list[tuple[float, float, str]]] = {}
    for t, p, s, side in parsed:
        bstart = (t // bucket_secs) * bucket_secs
        buckets.setdefault(bstart, []).append((p, s, side))

    bars = []
    for bstart in sorted(buckets):
        ticks = buckets[bstart]
        prices = [t[0] for t in ticks]
        sizes = [t[1] for t in ticks]
        vol_buy = sum(s for _, s, side in ticks if side == "BUY")
        vol_sell = sum(s for _, s, side in ticks if side == "SELL")
        total_vol = sum(sizes)

        # VWAP
        if total_vol > 0:
            vwap = sum(p * s for p, s, _ in ticks) / total_vol
        else:
            vwap = prices[-1]

        bars.append(Bar(
            t_start=bstart,
            t_end=bstart + bucket_secs,
            price_open=prices[0],
            price_close=prices[-1],
            price_high=max(prices),
            price_low=min(prices),
            tick_count=len(ticks),
            price_vwap=vwap,
            trade_count=len(ticks),
            volume=total_vol,
            volume_buy=vol_buy,
            volume_sell=vol_sell,
        ))
    return bars


def merge_bars(price_bars: list[Bar], trade_bars: list[Bar]) -> list[Bar]:
    """Merge price-only bars with trade bars, preferring trade data where available."""
    trade_map = {b.t_start: b for b in trade_bars}
    merged = []
    for pb in price_bars:
        tb = trade_map.pop(pb.t_start, None)
        if tb is not None:
            merged.append(tb)
        else:
            merged.append(pb)
    # Add any trade bars not in price bars
    for tb in sorted(trade_map.values(), key=lambda b: b.t_start):
        merged.append(tb)
    merged.sort(key=lambda b: b.t_start)
    return merged


# ---------------------------------------------------------------------------
# Timeline — Main Interface
# ---------------------------------------------------------------------------
class Timeline:
    """Main backtesting interface: wraps an event and provides bar access."""

    def __init__(self, event: EventMeta, granularity: str = "1h",
                 client: PolymarketClient | None = None):
        if granularity not in GRANULARITY_SECONDS:
            raise ValueError(f"Unknown granularity: {granularity}. Choose from: {list(GRANULARITY_SECONDS)}")
        self.event = event
        self.granularity = granularity
        self.client = client or PolymarketClient()
        self._raw_history_cache: dict[str, list[dict]] = {}

    # --- Constructors -------------------------------------------------------
    @classmethod
    def from_slug(cls, slug: str, granularity: str = "1h",
                  client: PolymarketClient | None = None) -> Timeline:
        c = client or PolymarketClient()
        event = c.get_event(slug=slug)
        return cls(event, granularity, c)

    @classmethod
    def from_event_id(cls, event_id: str, granularity: str = "5m",
                      client: PolymarketClient | None = None) -> Timeline:
        c = client or PolymarketClient()
        event = c.get_event(event_id=event_id)
        return cls(event, granularity, c)

    @classmethod
    def from_jsonl(cls, path: str | Path, event_index: int = 0,
                   granularity: str = "1h",
                   client: PolymarketClient | None = None) -> Timeline:
        events = PolymarketClient.load_jsonl(path)
        if event_index >= len(events):
            raise IndexError(f"event_index={event_index} but file has {len(events)} events")
        event = EventMeta.from_api(events[event_index])
        return cls(event, granularity, client or PolymarketClient())

    # --- Price History Fetching (lazy, cached) ------------------------------
    def _fetch_history(self, token_id: str, market: MarketMeta) -> list[dict]:
        if token_id in self._raw_history_cache:
            return self._raw_history_cache[token_id]

        fidelity = _fidelity_for_granularity(self.granularity)

        # Use startTs alone — works for both active and closed markets,
        # no 15-day limit, and preserves fidelity correctly.
        # Never use interval=max (degrades fidelity=1 from 1min to 12h).
        start_ts = _parse_ts(self.event.start_date)
        ev_start = _parse_ts(market.end_date)
        if ev_start and (start_ts is None or ev_start < start_ts):
            start_ts = ev_start

        if start_ts:
            start_ts = start_ts - 30 * 86400

        history = self.client.get_price_history(
            token_id, start_ts=start_ts, fidelity=fidelity,
        )

        self._raw_history_cache[token_id] = history
        return history

    def _resolve_market(self, query: str) -> MarketMeta:
        market = self.event.find_market(query)
        if market is None:
            avail = [m.question for m in self.event.markets]
            raise ValueError(f"No market matching '{query}'. Available: {avail}")
        return market

    # --- Core Methods -------------------------------------------------------
    def bars(self, query: str = "", outcome_index: int = 0,
             start: Any = None, end: Any = None) -> list[Bar]:
        """Get aggregated price bars for a market.

        Args:
            query: substring match on market question/slug/id (empty = first market)
            outcome_index: which outcome's token to fetch (default 0 = Yes/first)
            start: filter start (unix ts, ISO string, or YYYY-MM-DD)
            end: filter end
        """
        market = self._resolve_market(query)
        if outcome_index >= len(market.token_ids):
            raise IndexError(f"outcome_index={outcome_index} but market has {len(market.token_ids)} outcomes")

        token_id = market.token_ids[outcome_index]
        history = self._fetch_history(token_id, market)
        return aggregate_price_bars(history, self.granularity,
                                    _parse_ts(start), _parse_ts(end))

    def replay(self, query: str = "", outcome_index: int = 0,
               start: Any = None, end: Any = None,
               step: int | None = None) -> Generator[Bar, None, None]:
        """Walk-forward replay: yield one bar at a time chronologically.

        Args:
            step: number of bars to step through (None = all)
        """
        all_bars = self.bars(query, outcome_index, start, end)
        count = 0
        for bar in all_bars:
            yield bar
            count += 1
            if step is not None and count >= step:
                break

    def at(self, query: str = "", timestamp: Any = None,
           lookback: str = "1d", outcome_index: int = 0) -> dict:
        """Point-in-time snapshot: price and recent bars up to timestamp.

        Args:
            timestamp: the point in time to look at
            lookback: how far back to include bars (e.g. "1d", "4h")
        """
        ts = _parse_ts(timestamp)
        if ts is None:
            ts = int(time.time())

        lb_secs = GRANULARITY_SECONDS.get(lookback, 86400)
        start_ts = ts - lb_secs

        market = self._resolve_market(query)
        recent_bars = self.bars(query, outcome_index, start=start_ts, end=ts)

        price = recent_bars[-1].price_close if recent_bars else None
        return {
            "price": price,
            "bars": recent_bars,
            "market": market,
            "timestamp": ts,
        }

    def to_dataframe(self, query: str = "", outcome_index: int = 0,
                     start: Any = None, end: Any = None):
        """Export bars as a pandas DataFrame with datetime index."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required for to_dataframe(). Install with: pip install pandas")

        bar_list = self.bars(query, outcome_index, start, end)
        if not bar_list:
            return pd.DataFrame()

        records = []
        for b in bar_list:
            records.append({
                "datetime": pd.Timestamp(b.t_start, unit="s", tz="UTC"),
                "open": b.price_open,
                "high": b.price_high,
                "low": b.price_low,
                "close": b.price_close,
                "tick_count": b.tick_count,
                "change": b.price_change,
                "change_pct": b.price_change_pct,
                "volume": b.volume,
                "vwap": b.price_vwap,
            })

        df = pd.DataFrame(records)
        df.set_index("datetime", inplace=True)
        return df

    def bars_with_trades(self, query: str = "", trades: list[dict] | None = None,
                         outcome_index: int = 0,
                         start: Any = None, end: Any = None) -> list[Bar]:
        """Merge price bars with trade data (future use)."""
        price_b = self.bars(query, outcome_index, start, end)
        if not trades:
            return price_b
        trade_b = aggregate_trade_bars(trades, self.granularity,
                                       _parse_ts(start), _parse_ts(end))
        return merge_bars(price_b, trade_b)

    def summary(self):
        """Print event overview and market prices."""
        e = self.event
        print(f"\n{'='*70}")
        print(f"Event: {e.title}")
        print(f"  ID: {e.event_id}  |  Slug: {e.slug}")
        print(f"  Closed: {e.closed}  |  Neg Risk: {e.neg_risk}  |  Volume: {e.volume:,.0f}")
        if e.start_date:
            print(f"  Start: {e.start_date}")
        if e.end_date:
            print(f"  End:   {e.end_date}")
        if e.tags:
            print(f"  Tags: {', '.join(e.tags)}")
        print(f"\n  Markets ({len(e.markets)}):")
        print(f"  {'-'*66}")
        for i, m in enumerate(e.markets):
            prices_str = " / ".join(f"{o}={p:.2f}" for o, p in zip(m.outcomes, m.outcome_prices))
            status = "CLOSED" if m.closed else "OPEN"
            print(f"  [{i}] {m.question}")
            print(f"      {prices_str}  [{status}]  vol={m.volume:,.0f}")
            print(f"      tokens: {len(m.token_ids)} outcomes, condition={m.condition_id[:16]}...")
        print(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# Convenience Functions
# ---------------------------------------------------------------------------
def load(slug: str, granularity: str = "1h") -> Timeline:
    """One-liner to create a Timeline from an event slug."""
    return Timeline.from_slug(slug, granularity=granularity)


def search(query: str, limit: int = 20) -> list[EventMeta]:
    """One-liner to search events."""
    client = PolymarketClient()
    return client.search_events(query=query, limit=limit)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python backtest.py <event-slug-or-id> [granularity]")
        print("       python backtest.py --jsonl <path.jsonl> [event_index] [granularity]")
        print(f"\nGranularities: {', '.join(GRANULARITY_SECONDS)}")
        sys.exit(1)

    if sys.argv[1] == "--jsonl":
        path = sys.argv[2] if len(sys.argv) > 2 else None
        if not path:
            print("Error: --jsonl requires a file path")
            sys.exit(1)
        idx = int(sys.argv[3]) if len(sys.argv) > 3 else 0
        gran = sys.argv[4] if len(sys.argv) > 4 else "1h"
        tl = Timeline.from_jsonl(path, event_index=idx, granularity=gran)
    else:
        slug_or_id = sys.argv[1]
        gran = sys.argv[2] if len(sys.argv) > 2 else "1h"

        # Detect if it's a numeric ID
        if slug_or_id.isdigit():
            tl = Timeline.from_event_id(slug_or_id, granularity=gran)
        else:
            tl = Timeline.from_slug(slug_or_id, granularity=gran)

    tl.summary()

    # Show bars for first market
    if tl.event.markets:
        m = tl.event.markets[0]
        print(f"Fetching {gran} bars for: {m.question}")
        print(f"  Token ID: {m.token_ids[0] if m.token_ids else 'N/A'}")
        print()

        bar_list = tl.bars(m.question)
        if bar_list:
            print(f"  {len(bar_list)} bars from "
                  f"{datetime.fromtimestamp(bar_list[0].t_start, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} to "
                  f"{datetime.fromtimestamp(bar_list[-1].t_end, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")
            print()
            # Show first and last 5 bars
            show = bar_list[:5] + (["..."] if len(bar_list) > 10 else []) + bar_list[-5:]
            for b in show:
                if isinstance(b, str):
                    print(f"  {b}")
                else:
                    print(f"  {b}")
        else:
            print("  No price history available for this market.")
    print()


if __name__ == "__main__":
    main()
