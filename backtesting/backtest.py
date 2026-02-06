"""
Backtest engine for Polymarket strategies.

Single market:
    python polypoly/backtest.py data/prices.csv --strategy buyhold

Multi market (bulk directory with _index.json):
    python polypoly/backtest.py data/bulk/ --strategy nearexpiry
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import pandas as pd

from strategies import Strategy, STRATEGIES


def run_backtest(
    csv_path: str,
    strategy: Strategy,
    initial_cash: float = 1000.0,
    meta: dict | None = None,
) -> dict:
    """Run a backtest on CSV data with the given strategy.

    Args:
        csv_path: path to OHLC CSV (columns: datetime, open, high, low, close)
        strategy: a Strategy instance
        initial_cash: starting cash
        meta: market metadata dict (end_date, question, etc.) or None

    Returns:
        dict with metrics and equity curve
    """
    df = pd.read_csv(csv_path, parse_dates=["datetime"], index_col="datetime")

    portfolio = {
        "cash": initial_cash,
        "shares": 0.0,
        "avg_entry": 0.0,
        "equity": initial_cash,
    }

    trades = []  # list of {type, price, quantity, pnl}
    equity_curve = []

    strategy.init()

    for i in range(len(df)):
        row = df.iloc[i]
        row_dict = {
            "datetime": str(row.name),
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
        }

        history = df.iloc[: i + 1]
        price = row["close"]

        # Update equity before strategy decision
        portfolio["equity"] = portfolio["cash"] + portfolio["shares"] * price

        signal = strategy.next(row_dict, history, portfolio, meta)
        action = signal.get("action", "hold")
        quantity = signal.get("quantity", 0.0)

        if action == "buy" and quantity > 0:
            cost = quantity * price
            if cost > portfolio["cash"]:
                quantity = portfolio["cash"] / price
                cost = quantity * price

            if quantity > 0:
                total_shares = portfolio["shares"] + quantity
                if total_shares > 0:
                    portfolio["avg_entry"] = (
                        portfolio["avg_entry"] * portfolio["shares"] + cost
                    ) / total_shares
                portfolio["shares"] = total_shares
                portfolio["cash"] -= cost

                trades.append(
                    {"type": "buy", "price": price, "quantity": quantity, "pnl": None}
                )

        elif action == "sell" and quantity > 0:
            if quantity > portfolio["shares"]:
                quantity = portfolio["shares"]

            if quantity > 0:
                revenue = quantity * price
                pnl = (price - portfolio["avg_entry"]) * quantity
                portfolio["shares"] -= quantity
                portfolio["cash"] += revenue

                if portfolio["shares"] == 0:
                    portfolio["avg_entry"] = 0.0

                trades.append(
                    {"type": "sell", "price": price, "quantity": quantity, "pnl": pnl}
                )

        # Update equity after action
        portfolio["equity"] = portfolio["cash"] + portfolio["shares"] * price
        equity_curve.append(
            {"datetime": str(row.name), "equity": portfolio["equity"]}
        )

    # --- Compute metrics ---
    final_equity = portfolio["equity"]
    pnl_total = final_equity - initial_cash
    total_return_pct = (pnl_total / initial_cash) * 100

    # Trade-level metrics
    sell_trades = [t for t in trades if t["type"] == "sell" and t["pnl"] is not None]
    nb_trades = len(sell_trades)

    if nb_trades > 0:
        returns = [t["pnl"] for t in sell_trades]
        avg_return = sum(returns) / nb_trades
        std_returns = (
            math.sqrt(sum((r - avg_return) ** 2 for r in returns) / nb_trades)
            if nb_trades > 1
            else 0.0
        )
        win_trades = sum(1 for r in returns if r > 0)
        win_rate = win_trades / nb_trades
    else:
        avg_return = 0.0
        std_returns = 0.0
        win_trades = 0
        win_rate = 0.0

    # Max drawdown from equity curve
    equity_values = [e["equity"] for e in equity_curve]
    max_drawdown = _max_drawdown(equity_values)

    # Sharpe ratio (using equity curve returns)
    sharpe = _sharpe_ratio(equity_values)

    equity_df = pd.DataFrame(equity_curve)
    if not equity_df.empty:
        equity_df["datetime"] = pd.to_datetime(equity_df["datetime"])
        equity_df.set_index("datetime", inplace=True)

    return {
        "initial_cash": initial_cash,
        "final_equity": final_equity,
        "pnl": pnl_total,
        "total_return_pct": total_return_pct,
        "avg_return_per_trade": avg_return,
        "std_returns": std_returns,
        "max_drawdown_pct": max_drawdown * 100,
        "sharpe_ratio": sharpe,
        "win_rate": win_rate,
        "win_trades": win_trades,
        "nb_trades": nb_trades,
        "trades": trades,
        "equity_curve": equity_df,
        "portfolio": portfolio,
    }


def run_multi(
    csv_dir: str,
    strategy: Strategy,
    initial_cash: float = 1000.0,
) -> dict:
    """Run a strategy across all markets in a bulk extract directory.

    Each market gets its own independent portfolio (no capital sharing).
    Results are aggregated: average P&L per market, win rate across markets, etc.

    Args:
        csv_dir: directory containing CSVs and _index.json from bulk extract
        strategy: a Strategy instance
        initial_cash: starting cash per market

    Returns:
        dict with per-market results and aggregate metrics
    """
    dir_path = Path(csv_dir)
    index_path = dir_path / "_index.json"

    if not index_path.exists():
        raise FileNotFoundError(
            f"No _index.json in {csv_dir}. Run extract.py --bulk first."
        )

    index = json.loads(index_path.read_text())
    market_results = []
    skipped = 0

    for fname, meta in index.items():
        csv_path = dir_path / fname
        if not csv_path.exists():
            skipped += 1
            continue

        result = run_backtest(str(csv_path), strategy, initial_cash, meta=meta)

        buy_trades = [t for t in result["trades"] if t["type"] == "buy"]
        traded = len(buy_trades) > 0

        market_results.append({
            "file": fname,
            "question": meta.get("question", ""),
            "end_date": meta.get("end_date", ""),
            "traded": traded,
            "pnl": result["pnl"],
            "total_return_pct": result["total_return_pct"],
            "nb_buys": len(buy_trades),
            "buy_price": buy_trades[0]["price"] if buy_trades else None,
            "final_price": result["equity_curve"].iloc[-1]["equity"] / initial_cash
                if not result["equity_curve"].empty else None,
            "portfolio": result["portfolio"],
        })

    # --- Aggregate ---
    traded_markets = [m for m in market_results if m["traded"]]
    all_pnls = [m["pnl"] for m in traded_markets]

    nb_markets = len(market_results)
    nb_traded = len(traded_markets)
    nb_skipped = skipped

    if all_pnls:
        avg_pnl = sum(all_pnls) / len(all_pnls)
        std_pnl = (
            math.sqrt(sum((p - avg_pnl) ** 2 for p in all_pnls) / len(all_pnls))
            if len(all_pnls) > 1
            else 0.0
        )
        wins = sum(1 for p in all_pnls if p > 0)
        losses = sum(1 for p in all_pnls if p <= 0)
        win_rate = wins / len(all_pnls)
        total_pnl = sum(all_pnls)
        best = max(all_pnls)
        worst = min(all_pnls)
    else:
        avg_pnl = std_pnl = total_pnl = best = worst = 0.0
        wins = losses = 0
        win_rate = 0.0

    return {
        "nb_markets": nb_markets,
        "nb_traded": nb_traded,
        "nb_skipped": nb_skipped,
        "total_pnl": total_pnl,
        "avg_pnl": avg_pnl,
        "std_pnl": std_pnl,
        "win_rate": win_rate,
        "wins": wins,
        "losses": losses,
        "best_pnl": best,
        "worst_pnl": worst,
        "markets": market_results,
    }


def _max_drawdown(equity_values: list[float]) -> float:
    """Compute max drawdown as a fraction (0 to 1)."""
    if not equity_values:
        return 0.0
    peak = equity_values[0]
    max_dd = 0.0
    for eq in equity_values:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _sharpe_ratio(equity_values: list[float], risk_free: float = 0.0) -> float:
    """Compute annualized Sharpe ratio from equity curve."""
    if len(equity_values) < 2:
        return 0.0
    returns = []
    for i in range(1, len(equity_values)):
        prev = equity_values[i - 1]
        if prev > 0:
            returns.append((equity_values[i] - prev) / prev)
    if not returns:
        return 0.0
    mean_r = sum(returns) / len(returns)
    if len(returns) > 1:
        std_r = math.sqrt(
            sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
        )
    else:
        std_r = 0.0
    if std_r == 0:
        return 0.0
    return (mean_r - risk_free) / std_r


def print_results(results: dict, title: str = "Backtest") -> None:
    """Print formatted single-market backtest results."""
    r = results
    sign = "+" if r["pnl"] >= 0 else ""

    print()
    print(f"{'='*45}")
    print(f" BACKTEST : {title}")
    print(f"{'='*45}")
    print(f" Capital initial :  ${r['initial_cash']:,.2f}")
    print(f" Capital final :    ${r['final_equity']:,.2f}")
    print(f" P&L :              {sign}${r['pnl']:,.2f}")
    print(f" Return :           {sign}{r['total_return_pct']:.1f}%")
    print(f" Avg return/trade : {sign}${r['avg_return_per_trade']:,.2f}")
    print(f" Std returns :      ${r['std_returns']:,.2f}")
    print(f" Sharpe ratio :     {r['sharpe_ratio']:.2f}")
    print(f" Max drawdown :     -{r['max_drawdown_pct']:.1f}%")
    wt = r["win_trades"]
    nt = r["nb_trades"]
    print(f" Win rate :         {r['win_rate']*100:.1f}% ({wt}/{nt} trades)")
    print(f" Nb trades :        {nt}")
    print(f"{'='*45}")
    print()


def print_multi_results(results: dict, title: str = "Multi Backtest") -> None:
    """Print formatted multi-market backtest results."""
    r = results

    print()
    print(f"{'='*55}")
    print(f" {title}")
    print(f"{'='*55}")
    print(f" Markets scanned :  {r['nb_markets']}")
    print(f" Markets traded :   {r['nb_traded']}")
    print(f" Markets skipped :  {r['nb_skipped']}")
    print(f"{'-'*55}")

    if r["nb_traded"] > 0:
        sign = "+" if r["avg_pnl"] >= 0 else ""
        sign_t = "+" if r["total_pnl"] >= 0 else ""
        print(f" Avg P&L / market : {sign}${r['avg_pnl']:,.2f}")
        print(f" Std P&L :          ${r['std_pnl']:,.2f}")
        print(f" Total P&L :        {sign_t}${r['total_pnl']:,.2f}")
        print(f" Best :             +${r['best_pnl']:,.2f}")
        print(f" Worst :            ${r['worst_pnl']:,.2f}")
        print(f" Win rate :         {r['win_rate']*100:.1f}% ({r['wins']}/{r['nb_traded']})")
    else:
        print(f" No trades triggered.")

    print(f"{'='*55}")

    # Show traded markets detail
    traded = [m for m in r["markets"] if m["traded"]]
    if traded:
        print(f"\n Traded markets:")
        for m in sorted(traded, key=lambda x: x["pnl"], reverse=True):
            sign = "+" if m["pnl"] >= 0 else ""
            bp = f"@{m['buy_price']:.3f}" if m["buy_price"] else ""
            print(f"   {sign}${m['pnl']:>8,.2f}  {bp:>7s}  {m['question'][:55]}")

    print()


def main():
    parser = argparse.ArgumentParser(description="Backtest a strategy on Polymarket data")
    parser.add_argument(
        "path",
        help="CSV file (single market) or directory with _index.json (multi market)",
    )
    parser.add_argument(
        "--strategy",
        "-s",
        default="buyhold",
        help=f"Strategy name (built-in: {', '.join(STRATEGIES.keys())})",
    )
    parser.add_argument(
        "--cash",
        type=float,
        default=1000.0,
        help="Initial cash per market (default: 1000)",
    )
    parser.add_argument(
        "--title",
        "-t",
        default=None,
        help="Title for the report",
    )

    args = parser.parse_args()

    if args.strategy not in STRATEGIES:
        print(f"Unknown strategy: {args.strategy}")
        print(f"Available: {', '.join(STRATEGIES.keys())}")
        sys.exit(1)

    strategy = STRATEGIES[args.strategy]
    path = Path(args.path)

    if path.is_dir():
        title = args.title or path.name
        results = run_multi(str(path), strategy, initial_cash=args.cash)
        print_multi_results(results, title=title)
    else:
        title = args.title or path.stem
        results = run_backtest(str(path), strategy, initial_cash=args.cash)
        print_results(results, title=title)


if __name__ == "__main__":
    main()
