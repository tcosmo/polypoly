"""
Trading strategies for Polymarket backtesting.

Each strategy implements:
    init()  - called once at start (per market in multi mode)
    next(row, history, portfolio, meta) - called each bar, returns action dict

meta contains market metadata:
    end_date, question, closed, outcomes, volume, event_title, ...
    (available in multi-market mode, None in single CSV mode)

Add your own strategies to this file or create a new file following the same pattern.
"""
from __future__ import annotations

from datetime import datetime, timezone


class Strategy:
    """Base class for all strategies."""

    def init(self):
        """Called once at the beginning. Initialize your variables here."""
        pass

    def next(self, row: dict, history, portfolio: dict, meta: dict | None = None) -> dict:
        """Called at each time step.

        Args:
            row: dict with {datetime, open, high, low, close}
            history: DataFrame of all rows seen so far (including current)
            portfolio: {cash, shares, avg_entry, equity}
            meta: market metadata {end_date, question, closed, ...} or None

        Returns:
            {"action": "buy"/"sell"/"hold", "quantity": float}
        """
        return {"action": "hold"}


class BuyAndHold(Strategy):
    """Buy once when price is below a threshold, then hold forever.

    Args:
        buy_below: buy when close price is below this value (default 0.5)
        fraction: fraction of cash to spend on buy (default 1.0 = all-in)
    """

    def __init__(self, buy_below: float = 0.5, fraction: float = 1.0):
        self.buy_below = buy_below
        self.fraction = fraction
        self.bought = False

    def init(self):
        self.bought = False

    def next(self, row: dict, history, portfolio: dict, meta: dict | None = None) -> dict:
        if not self.bought and row["close"] < self.buy_below:
            self.bought = True
            qty = (portfolio["cash"] * self.fraction) / row["close"]
            return {"action": "buy", "quantity": qty}
        return {"action": "hold"}


class SimpleThreshold(Strategy):
    """Buy when price drops below low threshold, sell when it rises above high threshold.

    Args:
        buy_below: buy signal when close < this value
        sell_above: sell signal when close > this value
        fraction: fraction of cash/shares to trade (default 1.0)
    """

    def __init__(
        self, buy_below: float = 0.4, sell_above: float = 0.6, fraction: float = 1.0
    ):
        self.buy_below = buy_below
        self.sell_above = sell_above
        self.fraction = fraction

    def next(self, row: dict, history, portfolio: dict, meta: dict | None = None) -> dict:
        price = row["close"]

        if price < self.buy_below and portfolio["cash"] > 0:
            qty = (portfolio["cash"] * self.fraction) / price
            return {"action": "buy", "quantity": qty}

        if price > self.sell_above and portfolio["shares"] > 0:
            qty = portfolio["shares"] * self.fraction
            return {"action": "sell", "quantity": qty}

        return {"action": "hold"}


def _parse_dt(s: str, api_style: bool = False) -> datetime | None:
    """Parse a datetime string into a timezone-aware datetime, or None."""
    if api_style:
        fmts = ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d")
    else:
        fmts = ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y-%m-%d")
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


class NearExpiry(Strategy):
    """Buy 1 share when price is in [buy_above, buy_below) and market ends within max_days.

    Designed for multi-market screening: buy markets that are likely to resolve
    to 1.0 near expiry. Profit = 1.0 - buy_price per share if the market resolves Yes.

    Args:
        buy_above: minimum price to buy (default 0.9)
        buy_below: maximum price to buy (default 0.99)
        max_days: max days until market end_date (default 2.0)
        quantity: shares to buy per signal (default 1.0)
    """

    def __init__(
        self,
        buy_above: float = 0.9,
        buy_below: float = 0.99,
        max_days: float = 2.0,
        quantity: float = 1.0,
    ):
        self.buy_above = buy_above
        self.buy_below = buy_below
        self.max_days = max_days
        self.quantity = quantity
        self.bought = False

    def init(self):
        self.bought = False

    def next(self, row: dict, history, portfolio: dict, meta: dict | None = None) -> dict:
        if self.bought or meta is None:
            return {"action": "hold"}

        end_dt = _parse_dt(meta.get("end_date", ""), api_style=True)
        row_dt = _parse_dt(row["datetime"])
        if not end_dt or not row_dt:
            return {"action": "hold"}

        days_left = (end_dt - row_dt).total_seconds() / 86400
        price = row["close"]

        if days_left <= self.max_days and self.buy_above <= price < self.buy_below:
            self.bought = True
            return {"action": "buy", "quantity": self.quantity}

        return {"action": "hold"}


class NearExpiryTakeProfit(Strategy):
    """Buy in [0.9, 0.94), sell at 0.95. Take profit early instead of waiting for resolution.

    Args:
        buy_above: minimum price to buy (default 0.9)
        buy_below: maximum price to buy (default 0.94)
        take_profit: sell when price >= this (default 0.95)
        max_days: max days until market end_date (default 2.0)
        quantity: shares to buy (default 1.0)
    """

    def __init__(
        self,
        buy_above: float = 0.9,
        buy_below: float = 0.94,
        take_profit: float = 0.95,
        max_days: float = 2.0,
        quantity: float = 1.0,
    ):
        self.buy_above = buy_above
        self.buy_below = buy_below
        self.take_profit = take_profit
        self.max_days = max_days
        self.quantity = quantity
        self.bought = False

    def init(self):
        self.bought = False

    def next(self, row: dict, history, portfolio: dict, meta: dict | None = None) -> dict:
        price = row["close"]

        # If we hold shares, check take-profit
        if self.bought and portfolio["shares"] > 0:
            if price >= self.take_profit:
                return {"action": "sell", "quantity": portfolio["shares"]}
            return {"action": "hold"}

        if self.bought or meta is None:
            return {"action": "hold"}

        end_dt = _parse_dt(meta.get("end_date", ""), api_style=True)
        row_dt = _parse_dt(row["datetime"])
        if not end_dt or not row_dt:
            return {"action": "hold"}

        days_left = (end_dt - row_dt).total_seconds() / 86400

        if days_left <= self.max_days and self.buy_above <= price < self.buy_below:
            self.bought = True
            return {"action": "buy", "quantity": self.quantity}

        return {"action": "hold"}


class NearExpiryStopLoss(Strategy):
    """Buy at [0.9, 0.99), sell if price drops to stop_loss. Cut losses early.

    Args:
        buy_above: minimum price to buy (default 0.9)
        buy_below: maximum price to buy (default 0.99)
        stop_loss: sell when price <= this (default 0.5)
        max_days: max days until market end_date (default 2.0)
        quantity: shares to buy (default 1.0)
    """

    def __init__(
        self,
        buy_above: float = 0.9,
        buy_below: float = 0.99,
        stop_loss: float = 0.5,
        max_days: float = 2.0,
        quantity: float = 1.0,
    ):
        self.buy_above = buy_above
        self.buy_below = buy_below
        self.stop_loss = stop_loss
        self.max_days = max_days
        self.quantity = quantity
        self.bought = False

    def init(self):
        self.bought = False

    def next(self, row: dict, history, portfolio: dict, meta: dict | None = None) -> dict:
        price = row["close"]

        # If we hold shares, check stop-loss
        if self.bought and portfolio["shares"] > 0:
            if price <= self.stop_loss:
                return {"action": "sell", "quantity": portfolio["shares"]}
            return {"action": "hold"}

        if self.bought or meta is None:
            return {"action": "hold"}

        end_dt = _parse_dt(meta.get("end_date", ""), api_style=True)
        row_dt = _parse_dt(row["datetime"])
        if not end_dt or not row_dt:
            return {"action": "hold"}

        days_left = (end_dt - row_dt).total_seconds() / 86400

        if days_left <= self.max_days and self.buy_above <= price < self.buy_below:
            self.bought = True
            return {"action": "buy", "quantity": self.quantity}

        return {"action": "hold"}


# Registry of built-in strategies (name -> instance)
STRATEGIES = {
    "buyhold": BuyAndHold(),
    "threshold": SimpleThreshold(),
    "nearexpiry": NearExpiry(),
    "takeprofit": NearExpiryTakeProfit(),
    "stoploss": NearExpiryStopLoss(),
}
