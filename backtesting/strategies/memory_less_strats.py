"""
Robust (q-agnostic) betting on a memoryless event.

We assume:
- Each day, the event happens with true probability q (unknown), independent of the past.
- "Ends within d days" means T <= d, where T is the day of occurrence (geometric time).

Website provides:
- c1: price/probability for contract A1 paying 1 if T <= d1
- c2: price/probability for contract A2 paying 1 if T <= d2
with d2 > d1.

We consider TWO possible two-leg portfolios (directions):

(Mode 1) "front-YES / back-NO":
    - Put fraction x on YES A1  (T <= d1), cost c1, payoff prob p1(q)
    - Put fraction (1-x) on NO  A2  (T >  d2), cost (1-c2), payoff prob 1-p2(q)
    Edge:
        f1(x;q) = x * p1/c1 + (1-x) * (1-p2)/(1-c2) - 1

(Mode 2) "front-NO / back-YES" (the opposite):
    - Put fraction x on NO  A1  (T >  d1), cost (1-c1), payoff prob 1-p1(q)
    - Put fraction (1-x) on YES A2  (T <= d2), cost c2, payoff prob p2(q)
    Edge:
        f2(x;q) = x * (1-p1)/(1-c1) + (1-x) * p2/c2 - 1

Robust choice:
- For each mode, choose x robustly: maximize min_{q} f_mode(x;q) (maximin).
- Then pick the mode with the higher best worst-case edge.
- If the best worst-case edge <= 0, recommend "no_bet".

Implementation notes:
- The robust optimizer is MEMORY-SAFE: it streams over q in chunks and keeps only
  the running min over q for each x (no giant (q,x) matrix for robust solve).
- q-grid options:
  (A) global logit grid over (0,1)
  (B) market-centered bounded logit grid over an expanded range around [qhat_min,qhat_max]
      with expand_factor (e.g. 1.2).
- We also compute a grid-based feasibility interval for "guaranteed nonnegative edge for all q on the grid".
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Dict, Any, Tuple, Literal, Optional

import numpy as np
import matplotlib.pyplot as plt


# -------------------------
# Data structures
# -------------------------

Mode = Literal["yesA1_noA2", "noA1_yesA2"]


@dataclass(frozen=True)
class RobustBetResult:
    mode: Mode                   # which direction portfolio we chose
    x: float                     # fraction on the "front leg" (A1 side)
    worst_case_edge: float       # min_q f_mode(x;q) (approx on grid)
    worst_case_q: float          # q achieving the minimum (approx on grid)
    action: str                  # human-readable action
    details: Dict[str, Any]      # extra diagnostics


# -------------------------
# Core math
# -------------------------

def validate_inputs(c1: float, d1: int, c2: float, d2: int) -> None:
    if not (0.0 < c1 < 1.0 and 0.0 < c2 < 1.0):
        raise ValueError("c1 and c2 must be in (0,1).")
    if not (isinstance(d1, int) and isinstance(d2, int) and d1 > 0 and d2 > 0):
        raise ValueError("d1 and d2 must be positive integers.")
    if not (d2 > d1):
        raise ValueError("We assume d2 > d1.")


def p_leq_d(q: np.ndarray, d: int) -> np.ndarray:
    """p(d;q) = P(T <= d) for a memoryless daily event with probability q."""
    return 1.0 - np.power(1.0 - q, d)


def edge_mode(
    mode: Mode,
    x: float,
    q: np.ndarray,
    c1: float, d1: int, c2: float, d2: int
) -> np.ndarray:
    """
    f1 (mode yesA1_noA2): x*p1/c1 + (1-x)*(1-p2)/(1-c2) - 1
    f2 (mode noA1_yesA2): x*(1-p1)/(1-c1) + (1-x)*p2/c2 - 1
    """
    p1 = p_leq_d(q, d1)
    p2 = p_leq_d(q, d2)

    if mode == "yesA1_noA2":
        return x * (p1 / c1) + (1.0 - x) * ((1.0 - p2) / (1.0 - c2)) - 1.0

    if mode == "noA1_yesA2":
        return x * ((1.0 - p1) / (1.0 - c1)) + (1.0 - x) * (p2 / c2) - 1.0

    raise ValueError(f"Unknown mode: {mode}")


def edge_surface_grid(
    mode: Mode,
    x_grid: np.ndarray,
    q_grid: np.ndarray,
    c1: float, d1: int, c2: float, d2: int
) -> np.ndarray:
    """
    Returns F with shape (len(q_grid), len(x_grid)) and F[i,j] = f_mode(x_j; q_i).
    Vectorized for plotting / small grids.
    """
    q = q_grid[:, None]      # (Q,1)
    x = x_grid[None, :]      # (1,X)

    p1 = p_leq_d(q, d1)      # (Q,1)
    p2 = p_leq_d(q, d2)      # (Q,1)

    if mode == "yesA1_noA2":
        return x * (p1 / c1) + (1.0 - x) * ((1.0 - p2) / (1.0 - c2)) - 1.0

    if mode == "noA1_yesA2":
        return x * ((1.0 - p1) / (1.0 - c1)) + (1.0 - x) * (p2 / c2) - 1.0

    raise ValueError(f"Unknown mode: {mode}")


def implied_daily_q(c: float, d: int) -> float:
    """If c = 1 - (1-q)^d then q = 1 - (1-c)^(1/d)."""
    return float(1.0 - np.power(1.0 - c, 1.0 / d))


# -------------------------
# q grids
# -------------------------

def make_logit_q_grid(n: int, eps: float = 1e-12, s: float = 6.0) -> np.ndarray:
    """
    q = sigmoid(t), t in [-s, s], then clipped into (eps, 1-eps).
    Larger s => more mass near 0 and 1.
    """
    t = np.linspace(-s, s, n, dtype=np.float64)
    q = 1.0 / (1.0 + np.exp(-t))
    return np.clip(q, eps, 1.0 - eps)


def market_q_bounds(
    c1: float, d1: int, c2: float, d2: int,
    *,
    expand_factor: float = 1.2,
    q_epsilon: float = 1e-12,
    min_width: float = 1e-6,
) -> Tuple[float, float, Dict[str, float]]:
    """
    Build an 'englobing interval' around market-implied daily probs qhat1, qhat2,
    then widen it by expand_factor (>1) around its midpoint.

    Base interval: [a,b] = [min(qhat1,qhat2), max(...)]
    Widened: center m=(a+b)/2, half-width w=(b-a)/2,
             bounds = [m - k*w, m + k*w], with k=expand_factor.

    If qhat1 == qhat2, we enforce a minimum width around m.
    """
    if expand_factor < 1.0:
        raise ValueError("expand_factor should be >= 1.0")

    qhat1 = implied_daily_q(c1, d1)
    qhat2 = implied_daily_q(c2, d2)

    a = min(qhat1, qhat2)
    b = max(qhat1, qhat2)

    m = 0.5 * (a + b)
    w = 0.5 * (b - a)
    w = max(w, 0.5 * min_width)

    k = expand_factor
    qmin = m - k * w
    qmax = m + k * w

    qmin = float(np.clip(qmin, q_epsilon, 1.0 - q_epsilon))
    qmax = float(np.clip(qmax, q_epsilon, 1.0 - q_epsilon))

    if qmin > qmax:
        qmin, qmax = qmax, qmin

    diag = {
        "qhat1": float(qhat1),
        "qhat2": float(qhat2),
        "base_min": float(a),
        "base_max": float(b),
        "center": float(m),
        "half_width": float(w),
        "expand_factor": float(expand_factor),
        "qmin": float(qmin),
        "qmax": float(qmax),
    }
    return qmin, qmax, diag


def make_logit_q_grid_bounded(
    n: int,
    qmin: float,
    qmax: float,
    *,
    eps: float = 1e-12,
    s: float = 6.0
) -> np.ndarray:
    """
    Logit-like grid but bounded to [qmin, qmax]:
        u = sigmoid(t), t in [-s, s]
        q = qmin + (qmax-qmin)*u
    This concentrates points near qmin and qmax.
    """
    if not (0.0 < qmin < qmax < 1.0):
        raise ValueError("qmin and qmax must satisfy 0 < qmin < qmax < 1.")

    t = np.linspace(-s, s, n, dtype=np.float64)
    u = 1.0 / (1.0 + np.exp(-t))
    q = qmin + (qmax - qmin) * u
    return np.clip(q, eps, 1.0 - eps)


# -------------------------
# Grid-feasibility interval for "guaranteed nonnegative" (on q grid)
# -------------------------

def feasible_x_interval_on_q_grid(
    mode: Mode,
    q_grid: np.ndarray,
    c1: float, d1: int, c2: float, d2: int,
    tol: float = 0.0,
) -> Tuple[float, float]:
    """
    For each q, f(x;q) >= -tol is a half-interval constraint on x.
    We intersect constraints over q_grid and return [L, U].
    If L > U => infeasible (no x in [0,1] works on this grid).
    """
    q = np.asarray(q_grid, dtype=np.float64)

    p1 = p_leq_d(q, d1)
    p2 = p_leq_d(q, d2)

    if mode == "yesA1_noA2":
        A = p1 / c1
        B = (1.0 - p2) / (1.0 - c2)
    elif mode == "noA1_yesA2":
        A = (1.0 - p1) / (1.0 - c1)
        B = p2 / c2
    else:
        raise ValueError(f"Unknown mode: {mode}")

    # f(x;q) = x*A + (1-x)*B - 1 = x*(A-B) + (B-1)
    D = A - B
    rhs = 1.0 - B - tol  # need x*D >= rhs

    L, U = 0.0, 1.0
    for Di, ri in zip(D, rhs):
        if np.isclose(Di, 0.0):
            if ri > 0.0:
                return 1.0, 0.0  # infeasible
            continue
        if Di > 0.0:
            L = max(L, ri / Di)
        else:
            U = min(U, ri / Di)

    return float(L), float(U)


# -------------------------
# Robust optimizer (STREAMING maximin, memory-safe)
# -------------------------

def robust_maximin_x_on_grids_streaming(
    mode: Mode,
    c1: float, d1: int, c2: float, d2: int,
    *,
    q_grid: np.ndarray,
    x_grid_size: int = 2_001,
    chunk_q: int = 4_000,
) -> Tuple[float, float, float, np.ndarray]:
    """
    Discrete robust optimization for a given mode (memory-safe):
      choose x in x_grid maximizing min_{q in q_grid} f_mode(x;q).

    Returns:
      (x_star, worst_edge, q_worst, x_grid)
    """
    validate_inputs(c1, d1, c2, d2)

    q_grid = np.asarray(q_grid, dtype=np.float64)
    x_grid = np.linspace(0.0, 1.0, x_grid_size, dtype=np.float64)

    min_over_q = np.full(x_grid.shape, np.inf, dtype=np.float64)

    for start in range(0, len(q_grid), chunk_q):
        q_chunk = q_grid[start:start + chunk_q]
        F_chunk = edge_surface_grid(mode, x_grid, q_chunk, c1, d1, c2, d2)  # (Qc, X)
        min_over_q = np.minimum(min_over_q, F_chunk.min(axis=0))

    j_star = int(np.argmax(min_over_q))
    x_star = float(x_grid[j_star])
    worst_edge = float(min_over_q[j_star])

    fx = edge_mode(mode, x_star, q_grid, c1, d1, c2, d2)
    i_worst = int(np.argmin(fx))
    q_worst = float(q_grid[i_worst])

    return x_star, worst_edge, q_worst, x_grid


def endpoint_necessary_x_windows(c1: float, c2: float) -> Dict[str, Tuple[float, float]]:
    """
    Necessary (endpoint) feasibility windows for uniform nonnegative edge:
      Mode 1 requires: c1 <= x <= c2
      Mode 2 requires: c1 <= x <= 1 - c2
    """
    return {
        "yesA1_noA2": (c1, c2),
        "noA1_yesA2": (c1, 1.0 - c2),
    }


def build_q_grid(
    c1: float, d1: int, c2: float, d2: int,
    *,
    q_grid_size: int,
    q_epsilon: float,
    logit_s: float,
    use_market_range: bool,
    market_expand_factor: float,
) -> Tuple[np.ndarray, Optional[Dict[str, float]]]:
    """
    Returns (q_grid, market_diag).
    If use_market_range is True: build bounded logit grid on an expanded market-implied range.
    Else: global logit grid on (0,1).
    """
    if use_market_range:
        qmin, qmax, market_diag = market_q_bounds(
            c1, d1, c2, d2,
            expand_factor=market_expand_factor,
            q_epsilon=q_epsilon,
        )
        q_grid = make_logit_q_grid_bounded(q_grid_size, qmin, qmax, eps=q_epsilon, s=logit_s)
        return q_grid, market_diag
    else:
        q_grid = make_logit_q_grid(q_grid_size, eps=q_epsilon, s=logit_s)
        return q_grid, None


def robust_optimal_bet_q_agnostic(
    c1: float, d1: int, c2: float, d2: int,
    *,
    q_grid_size: int = 80_000,
    x_grid_size: int = 2_001,
    q_epsilon: float = 1e-12,
    chunk_q: int = 4_000,
    logit_s: float = 6.0,
    # NEW (market-centered robustness):
    use_market_range: bool = True,
    market_expand_factor: float = 1.2,
) -> RobustBetResult:
    """
    Evaluate BOTH modes, pick the one with better maximin value.
    If best worst-case edge <= 0 => no_bet.

    If use_market_range=True, the robust q-grid is restricted to an expanded range around
    the market-implied [min(qhat1,qhat2), max(qhat1,qhat2)] (expanded by market_expand_factor).
    """
    validate_inputs(c1, d1, c2, d2)

    # Build ONE common q grid for both modes (fair comparison)
    q_grid, market_diag = build_q_grid(
        c1, d1, c2, d2,
        q_grid_size=q_grid_size,
        q_epsilon=q_epsilon,
        logit_s=logit_s,
        use_market_range=use_market_range,
        market_expand_factor=market_expand_factor,
    )

    results: Dict[Mode, Tuple[float, float, float]] = {}
    for mode in ("yesA1_noA2", "noA1_yesA2"):
        x_star, worst_edge, q_worst, _ = robust_maximin_x_on_grids_streaming(
            mode,
            c1, d1, c2, d2,
            q_grid=q_grid,
            x_grid_size=x_grid_size,
            chunk_q=chunk_q,
        )
        results[mode] = (x_star, worst_edge, q_worst)

    best_mode: Mode = max(results.keys(), key=lambda m: results[m][1])  # type: ignore[assignment]
    x_star, worst_edge, q_worst = results[best_mode]

    def snap(x: float, tol: float = 1e-4) -> float:
        if x < tol:
            return 0.0
        if x > 1.0 - tol:
            return 1.0
        return x

    x_s = snap(x_star)

    if worst_edge <= 0.0:
        action = "no_bet"
    else:
        if best_mode == "yesA1_noA2":
            if x_s == 1.0:
                action = "buy YES A1 only"
            elif x_s == 0.0:
                action = "buy NO A2 only"
            else:
                action = f"split: x={x_s:.4f} on YES A1, (1-x) on NO A2"
        else:  # "noA1_yesA2"
            if x_s == 1.0:
                action = "buy NO A1 only"
            elif x_s == 0.0:
                action = "buy YES A2 only"
            else:
                action = f"split: x={x_s:.4f} on NO A1, (1-x) on YES A2"

    qhat1 = implied_daily_q(c1, d1)
    qhat2 = implied_daily_q(c2, d2)

    windows = endpoint_necessary_x_windows(c1, c2)

    feas = {}
    for mode in ("yesA1_noA2", "noA1_yesA2"):
        L, U = feasible_x_interval_on_q_grid(mode, q_grid, c1, d1, c2, d2, tol=0.0)
        feas[mode] = {
            "L": L,
            "U": U,
            "feasible_on_grid": (max(L, 0.0) <= min(U, 1.0)),
            "clipped_interval_[0,1]": (max(L, 0.0), min(U, 1.0)),
        }

    details = {
        "implied_q1_from_(c1,d1)": float(qhat1),
        "implied_q2_from_(c2,d2)": float(qhat2),
        "discrepancy_q2_minus_q1": float(qhat2 - qhat1),
        "mode_scores_(maximin_worst_edge)": {
            "yesA1_noA2": float(results["yesA1_noA2"][1]),
            "noA1_yesA2": float(results["noA1_yesA2"][1]),
        },
        "endpoint_necessary_x_windows_(q->0_and_q->1)": {
            "yesA1_noA2_requires_c1<=x<=c2": windows["yesA1_noA2"],
            "noA1_yesA2_requires_c1<=x<=1-c2": windows["noA1_yesA2"],
        },
        "grid_feasible_x_intervals_(f>=0_for_all_q_in_grid)": feas,
        "portfolio_interpretation": (
            "Mode yesA1_noA2 = YES A1 + NO A2; "
            "Mode noA1_yesA2 = NO A1 + YES A2."
        ),
        "robust_q_grid": {
            "use_market_range": bool(use_market_range),
            "market_expand_factor": float(market_expand_factor),
            "market_diag": market_diag,
            "size": int(len(q_grid)),
            "min": float(q_grid.min()),
            "max": float(q_grid.max()),
            "type": "bounded_logit" if use_market_range else "global_logit",
            "s": float(logit_s),
            "eps": float(q_epsilon),
        },
    }

    return RobustBetResult(
        mode=best_mode,
        x=float(x_s),
        worst_case_edge=float(worst_edge),
        worst_case_q=float(q_worst),
        action=action,
        details=details,
    )


# -------------------------
# Plotting (small grids only)
# -------------------------

def plot_edge_surface_and_highlight(
    c1: float, d1: int, c2: float, d2: int,
    *,
    # robust selection settings:
    robust_q_grid_size: int = 80_000,
    robust_x_grid_size: int = 2_001,
    robust_chunk_q: int = 4_000,
    robust_logit_s: float = 6.0,
    use_market_range: bool = True,
    market_expand_factor: float = 1.2,
    # plotting grid sizes (keep small):
    q_grid_size: int = 600,
    x_grid_size: int = 600,
) -> Dict[str, float]:
    """
    Plots for the BEST mode (chosen by maximin on a (possibly market-bounded) q grid):
      1) Heatmap of f_mode(x;q) over (q,x), highlighting:
         - column x = x* (maximin robust)
         - row q = q_worst (worst-case q for x*)
         - contour f=0
      2) Curve x -> min_q f_mode(x;q), highlighting x*.
      3) Slice plot q -> f_mode(x*;q), to confirm x* behavior.

    Note: the plot uses a uniform q grid over [qmin,qmax] (or near [0,1]) just for visualization.
    """
    # Build robust q grid (bounded or global)
    q_grid_rob, market_diag = build_q_grid(
        c1, d1, c2, d2,
        q_grid_size=robust_q_grid_size,
        q_epsilon=1e-12,
        logit_s=robust_logit_s,
        use_market_range=use_market_range,
        market_expand_factor=market_expand_factor,
    )

    # compute best mode by maximin (streaming)
    mode_candidates: Tuple[Mode, Mode] = ("yesA1_noA2", "noA1_yesA2")
    mode_scores: Dict[Mode, Tuple[float, float, float]] = {}
    for mode in mode_candidates:
        x_star, worst_edge, q_worst, _ = robust_maximin_x_on_grids_streaming(
            mode,
            c1, d1, c2, d2,
            q_grid=q_grid_rob,
            x_grid_size=robust_x_grid_size,
            chunk_q=robust_chunk_q,
        )
        mode_scores[mode] = (x_star, worst_edge, q_worst)

    best_mode: Mode = max(mode_scores.keys(), key=lambda m: mode_scores[m][1])  # type: ignore[assignment]
    x_star, worst_edge, q_worst = mode_scores[best_mode]

    # Plot grids
    if use_market_range and market_diag is not None:
        qmin = market_diag["qmin"]
        qmax = market_diag["qmax"]
        q_grid = np.linspace(qmin, qmax, q_grid_size, dtype=np.float64)
    else:
        q_grid = np.linspace(1e-9, 1.0 - 1e-9, q_grid_size, dtype=np.float64)

    x_grid = np.linspace(0.0, 1.0, x_grid_size, dtype=np.float64)
    F = edge_surface_grid(best_mode, x_grid, q_grid, c1, d1, c2, d2)  # (Q,X)

    min_over_q = F.min(axis=0)

    j_star = int(np.argmin(np.abs(x_grid - x_star)))
    slice_fx = F[:, j_star]

    mode_label = "YES A1 + NO A2" if best_mode == "yesA1_noA2" else "NO A1 + YES A2"

    # Plot 1: heatmap
    fig1, ax1 = plt.subplots(figsize=(9, 5.5))
    im = ax1.imshow(
        F,
        aspect="auto",
        origin="lower",
        extent=[x_grid.min(), x_grid.max(), q_grid.min(), q_grid.max()],
        interpolation="nearest",
    )
    ax1.set_xlabel("x (fraction on A1-side leg)")
    ax1.set_ylabel("q (true daily probability)")
    ax1.set_title(f"Edge surface f_mode(x; q)  |  Mode: {mode_label}")

    ax1.axvline(float(x_grid[j_star]), linewidth=2.5)
    ax1.axhline(q_worst, linewidth=2.0, linestyle="--")

    ax1.contour(x_grid, q_grid, F, levels=[0.0], linewidths=2.0)

    cbar = plt.colorbar(im, ax=ax1)
    cbar.set_label("f_mode(x; q) (edge)")

    extra = ""
    if use_market_range and market_diag is not None:
        extra = (
            f"\nmarket q range = [{market_diag['qmin']:.6g}, {market_diag['qmax']:.6g}]"
            f"\n(qhat1={market_diag['qhat1']:.6g}, qhat2={market_diag['qhat2']:.6g}, k={market_diag['expand_factor']})"
        )

    ax1.text(
        0.02, 0.98,
        f"mode = {best_mode}\n"
        f"x* (robust, hi-res) = {x_star:.4f}\n"
        f"x* (plot grid)      = {float(x_grid[j_star]):.4f}\n"
        f"min_q f(x*;q)       = {worst_edge:.4g}\n"
        f"q_worst (hi-res)    ≈ {q_worst:.6f}"
        f"{extra}",
        transform=ax1.transAxes,
        va="top",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.85)
    )

    # Plot 2: min_q curve (on plot grid)
    fig2, ax2 = plt.subplots(figsize=(9, 3.8))
    ax2.plot(x_grid, min_over_q)
    ax2.axvline(float(x_grid[j_star]), linewidth=2.5)
    ax2.axhline(0.0, linewidth=1.5, linestyle="--")
    ax2.set_xlabel("x")
    ax2.set_ylabel("min_q f_mode(x; q)")
    ax2.set_title(f"Worst-case edge vs x (Mode: {mode_label})")

    ax2.text(
        0.02, 0.92,
        f"argmax_x min_q f (plot grid) = {float(x_grid[j_star]):.4f}\n"
        f"value (plot grid)            = {float(min_over_q[j_star]):.4g}",
        transform=ax2.transAxes,
        va="top",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.85)
    )

    # Plot 3: slice check
    fig3, ax3 = plt.subplots(figsize=(9, 3.2))
    ax3.plot(q_grid, slice_fx)
    ax3.axhline(0.0, linewidth=1.5, linestyle="--")
    ax3.set_xlabel("q")
    ax3.set_ylabel(f"f_mode(x*; q)  (x*≈{float(x_grid[j_star]):.4f})")
    ax3.set_title(f"Slice check across q (Mode: {mode_label})")

    ax3.text(
        0.02, 0.92,
        f"min_q f(x*;q) on plot grid = {float(slice_fx.min()):.6g}",
        transform=ax3.transAxes,
        va="top",
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.85)
    )

    plt.show()

    return {
        "mode": best_mode,
        "x_star_hi_res": float(x_star),
        "worst_edge_hi_res": float(worst_edge),
        "q_worst_hi_res": float(q_worst),
        "x_star_plot_grid": float(x_grid[j_star]),
        "slice_min_plot_grid": float(slice_fx.min()),
        "slice_max_plot_grid": float(slice_fx.max()),
        "used_market_range": float(1.0 if use_market_range else 0.0),
        "market_qmin": float(market_diag["qmin"]) if (use_market_range and market_diag is not None) else float("nan"),
        "market_qmax": float(market_diag["qmax"]) if (use_market_range and market_diag is not None) else float("nan"),
    }


# -------------------------
# Test mode: execute this file at runtime to run test examples
# Usage: python memory_less_strats test
# Backtest mode: backtest the strategy on a specific market
# Usage: python memory_less_strats backtest <csv_path_or_market_file>
# -------------------------

def backtest_memoryless_strategy(
    csv_path: str,
    initial_cash: float = 1000.0,
    lookback_days: float = 7.0,
    min_days_until_end: float = 10.0,
    min_history_days: float = 7.0,
    fraction: float = 1.0,
    q_grid_size: int = 80_000,
    x_grid_size: int = 2_001,
    use_market_range: bool = True,
    market_expand_factor: float = 1.2,
) -> None:
    """
    Backtest the memoryless robust strategy on a specific market.
    
    Args:
        csv_path: Path to CSV file (single market) or path to market file in bulk directory
        initial_cash: Starting cash (default: 1000.0)
        lookback_days: Days to look back for historical price (default: 7.0)
        min_days_until_end: Minimum days until market end to trade (default: 10.0)
        min_history_days: Minimum history required (default: 7.0)
        fraction: Fraction of cash to use (default: 1.0)
        q_grid_size: Size of q grid for optimization (default: 80_000)
        x_grid_size: Size of x grid for optimization (default: 2_001)
        use_market_range: Use market-centered q range (default: True)
        market_expand_factor: Expansion factor for market q range (default: 1.2)
    """
    import json
    from pathlib import Path
    
    # Import backtesting functions
    sys.path.insert(0, str(Path(__file__).parent.parent))
    try:
        from backtest import run_backtest
        from strategies import MemorylessRobustStrategy
    except ImportError:
        print("Error: Could not import backtest module. Make sure you're running from the correct directory.")
        print("Expected: polypoly/backtesting/strategies/memory_less_strats")
        return
    
    csv_file = Path(csv_path)
    meta = None
    
    # Check if it's a file in a bulk directory (has _index.json)
    if not csv_file.is_absolute():
        csv_file = Path(__file__).parent.parent.parent / csv_path
    
    # Try to find metadata if in bulk directory
    bulk_dir = csv_file.parent
    index_path = bulk_dir / "_index.json"
    if index_path.exists():
        index = json.loads(index_path.read_text())
        # Find market metadata by filename
        csv_name = csv_file.name
        if csv_name in index:
            meta = index[csv_name]
            print(f"Found market metadata: {meta.get('question', 'Unknown')}")
    
    if not csv_file.exists():
        print(f"Error: CSV file not found: {csv_file}")
        print(f"Please provide a valid path to a CSV file.")
        return
    
    # Create strategy instance
    strategy = MemorylessRobustStrategy(
        lookback_days=lookback_days,
        min_days_until_end=min_days_until_end,
        min_history_days=min_history_days,
        fraction=fraction,
        q_grid_size=q_grid_size,
        x_grid_size=x_grid_size,
        use_market_range=use_market_range,
        market_expand_factor=market_expand_factor,
    )
    
    print(f"\n{'='*60}")
    print(f"Backtesting Memoryless Robust Strategy")
    print(f"{'='*60}")
    print(f"CSV file: {csv_file}")
    print(f"Initial cash: ${initial_cash:.2f}")
    print(f"Lookback days: {lookback_days}")
    print(f"Min days until end: {min_days_until_end}")
    print(f"{'='*60}\n")
    
    # Run backtest
    try:
        results = run_backtest(str(csv_file), strategy, initial_cash, meta=meta)
        
        # Print results
        print(f"\n{'='*60}")
        print(f"BACKTEST RESULTS")
        print(f"{'='*60}")
        print(f"Initial cash:      ${results['initial_cash']:.2f}")
        print(f"Final equity:      ${results['final_equity']:.2f}")
        print(f"P&L:               ${results['pnl']:.2f}")
        print(f"Total return:      {results['total_return_pct']:.2f}%")
        print(f"Max drawdown:      {results['max_drawdown_pct']:.2f}%")
        print(f"Sharpe ratio:      {results['sharpe_ratio']:.4f}")
        print(f"\nTrades:")
        print(f"  Total trades:     {results['nb_trades']}")
        print(f"  Win rate:        {results['win_rate']*100:.1f}% ({results['win_trades']} wins)")
        if results['nb_trades'] > 0:
            print(f"  Avg return/trade: ${results['avg_return_per_trade']:.2f}")
            print(f"  Std returns:     ${results['std_returns']:.2f}")
        print(f"{'='*60}\n")
        
        # Show trades
        if results['trades']:
            print("Trade history:")
            for i, trade in enumerate(results['trades'][:10], 1):  # Show first 10
                pnl_str = f"${trade['pnl']:.2f}" if trade['pnl'] is not None else "N/A"
                print(f"  {i}. {trade['type'].upper():4s} {trade['quantity']:.2f} shares @ ${trade['price']:.4f}  P&L: {pnl_str}")
            if len(results['trades']) > 10:
                print(f"  ... and {len(results['trades']) - 10} more trades")
            print()
        
    except Exception as e:
        print(f"Error during backtest: {e}")
        import traceback
        traceback.print_exc()


def backtest_memoryless_two_markets(
    csv_path_1: str,
    csv_path_2: str,
    initial_cash: float = 1000.0,
    fraction: float = 1.0,
    q_grid_size: int = 80_000,
    x_grid_size: int = 2_001,
    use_market_range: bool = True,
    market_expand_factor: float = 1.2,
) -> None:
    """
    Backtest the memoryless robust strategy on TWO different but related events.
    
    Example: Event A1 = "War ends by Feb 24", Event A2 = "War ends by Mar 2"
    The strategy compares prices of these two events and finds robust arbitrage opportunities.
    
    Positions are automatically liquidated when markets resolve:
    - A1 positions liquidated when A1 resolves (at end_date_1)
    - A2 positions liquidated when A2 resolves (at end_date_2)
    
    Args:
        csv_path_1: Path to CSV file for event A1 (shorter duration)
        csv_path_2: Path to CSV file for event A2 (longer duration)
        initial_cash: Starting cash (default: 1000.0)
        fraction: Fraction of cash to use per trade (default: 1.0)
        q_grid_size: Size of q grid for optimization (default: 80_000)
        x_grid_size: Size of x grid for optimization (default: 2_001)
        use_market_range: Use market-centered q range (default: True)
        market_expand_factor: Expansion factor for market q range (default: 1.2)
    """
    import json
    import math
    from pathlib import Path
    from datetime import datetime, timezone
    
    # Import pandas
    try:
        import pandas as pd
    except ImportError:
        print("Error: pandas is required. Install with: pip install pandas")
        return
    
    # Helper function to parse datetime
    def _parse_dt(s: str, api_style: bool = False) -> datetime | None:
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
    
    # Load CSV files and metadata
    def load_market_data(csv_path: str):
        csv_file = Path(csv_path)
        if not csv_file.is_absolute():
            csv_file = Path(__file__).parent.parent.parent / csv_path
        
        # Try to find metadata
        bulk_dir = csv_file.parent
        index_path = bulk_dir / "_index.json"
        meta = None
        if index_path.exists():
            index = json.loads(index_path.read_text())
            csv_name = csv_file.name
            if csv_name in index:
                meta = index[csv_name]
        
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")
        
        df = pd.read_csv(csv_file, parse_dates=["datetime"], index_col="datetime")
        return df, meta, csv_file
    
    try:
        df1, meta1, file1 = load_market_data(csv_path_1)
        df2, meta2, file2 = load_market_data(csv_path_2)
    except Exception as e:
        print(f"Error loading market data: {e}")
        return
    
    # Get end dates
    end_date_1 = _parse_dt(meta1.get("end_date", ""), api_style=True) if meta1 else None
    end_date_2 = _parse_dt(meta2.get("end_date", ""), api_style=True) if meta2 else None
    
    if not end_date_1 or not end_date_2:
        print("Error: Both markets must have end_date in metadata.")
        print(f"Market 1 end_date: {meta1.get('end_date') if meta1 else 'None'}")
        print(f"Market 2 end_date: {meta2.get('end_date') if meta2 else 'None'}")
        return
    
    # Ensure d2 > d1 (A2 should end after A1)
    if end_date_2 <= end_date_1:
        print("Warning: Event A2 should end after A1. Swapping markets...")
        df1, df2 = df2, df1
        meta1, meta2 = meta2, meta1
        end_date_1, end_date_2 = end_date_2, end_date_1
        file1, file2 = file2, file1
    
    # Synchronize timestamps (use intersection)
    common_times = df1.index.intersection(df2.index)
    if len(common_times) == 0:
        print("Error: No common timestamps between the two markets.")
        return
    
    print(f"\n{'='*60}")
    print(f"Backtesting Memoryless Robust Strategy (Two Markets)")
    print(f"{'='*60}")
    print(f"Market A1: {file1.name}")
    print(f"  Question: {meta1.get('question', 'Unknown') if meta1 else 'Unknown'}")
    print(f"  End date: {end_date_1}")
    print(f"Market A2: {file2.name}")
    print(f"  Question: {meta2.get('question', 'Unknown') if meta2 else 'Unknown'}")
    print(f"  End date: {end_date_2}")
    print(f"Common timestamps: {len(common_times)}")
    print(f"Initial cash: ${initial_cash:.2f}")
    print(f"{'='*60}\n")
    
    # Portfolio: track positions on both markets
    portfolio = {
        "cash": initial_cash,
        "shares_A1": 0.0,  # YES shares on A1
        "shares_A1_NO": 0.0,  # NO shares on A1
        "shares_A2": 0.0,  # YES shares on A2
        "shares_A2_NO": 0.0,  # NO shares on A2
        "avg_entry_A1": 0.0,
        "avg_entry_A1_NO": 0.0,
        "avg_entry_A2": 0.0,
        "avg_entry_A2_NO": 0.0,
    }
    
    trades = []
    equity_curve = []
    last_recommendation = None
    
    # Process each common timestamp
    for t in common_times:
        row1 = df1.loc[t]
        row2 = df2.loc[t]
        
        current_dt = t if isinstance(t, datetime) else _parse_dt(str(t))
        if not current_dt:
            continue
        if current_dt.tzinfo is None:
            current_dt = current_dt.replace(tzinfo=timezone.utc)
        
        # Check if markets have resolved
        market_1_resolved = current_dt >= end_date_1
        market_2_resolved = current_dt >= end_date_2
        
        # Liquidate A1 positions if resolved
        if market_1_resolved and (portfolio["shares_A1"] > 0 or portfolio["shares_A1_NO"] > 0):
            # Determine outcome: use final price (if > 0.5, YES wins; else NO wins)
            # Or use metadata if available
            final_price_1 = float(row1["close"])
            outcome_1_yes = final_price_1 > 0.5
            
            if portfolio["shares_A1"] > 0:
                # YES shares: payoff is 1.0 if YES wins, 0.0 if NO wins
                payoff = 1.0 if outcome_1_yes else 0.0
                revenue = portfolio["shares_A1"] * payoff
                pnl_A1_yes = revenue - (portfolio["avg_entry_A1"] * portfolio["shares_A1"])
                portfolio["cash"] += revenue
                trades.append({
                    "datetime": str(t),
                    "type": "resolve",
                    "market": "A1_YES",
                    "quantity": portfolio["shares_A1"],
                    "price": payoff,
                    "pnl": pnl_A1_yes,
                })
                portfolio["shares_A1"] = 0.0
                portfolio["avg_entry_A1"] = 0.0
            
            if portfolio["shares_A1_NO"] > 0:
                # NO shares: payoff is 1.0 if NO wins, 0.0 if YES wins
                payoff = 1.0 if not outcome_1_yes else 0.0
                revenue = portfolio["shares_A1_NO"] * payoff
                pnl_A1_no = revenue - (portfolio["avg_entry_A1_NO"] * portfolio["shares_A1_NO"])
                portfolio["cash"] += revenue
                trades.append({
                    "datetime": str(t),
                    "type": "resolve",
                    "market": "A1_NO",
                    "quantity": portfolio["shares_A1_NO"],
                    "price": payoff,
                    "pnl": pnl_A1_no,
                })
                portfolio["shares_A1_NO"] = 0.0
                portfolio["avg_entry_A1_NO"] = 0.0
        
        # Liquidate A2 positions if resolved
        if market_2_resolved and (portfolio["shares_A2"] > 0 or portfolio["shares_A2_NO"] > 0):
            final_price_2 = float(row2["close"])
            outcome_2_yes = final_price_2 > 0.5
            
            if portfolio["shares_A2"] > 0:
                payoff = 1.0 if outcome_2_yes else 0.0
                revenue = portfolio["shares_A2"] * payoff
                pnl_A2_yes = revenue - (portfolio["avg_entry_A2"] * portfolio["shares_A2"])
                portfolio["cash"] += revenue
                trades.append({
                    "datetime": str(t),
                    "type": "resolve",
                    "market": "A2_YES",
                    "quantity": portfolio["shares_A2"],
                    "price": payoff,
                    "pnl": pnl_A2_yes,
                })
                portfolio["shares_A2"] = 0.0
                portfolio["avg_entry_A2"] = 0.0
            
            if portfolio["shares_A2_NO"] > 0:
                payoff = 1.0 if not outcome_2_yes else 0.0
                revenue = portfolio["shares_A2_NO"] * payoff
                pnl_A2_no = revenue - (portfolio["avg_entry_A2_NO"] * portfolio["shares_A2_NO"])
                portfolio["cash"] += revenue
                trades.append({
                    "datetime": str(t),
                    "type": "resolve",
                    "market": "A2_NO",
                    "quantity": portfolio["shares_A2_NO"],
                    "price": payoff,
                    "pnl": pnl_A2_no,
                })
                portfolio["shares_A2_NO"] = 0.0
                portfolio["avg_entry_A2_NO"] = 0.0
        
        # Skip trading if both markets are resolved
        if market_1_resolved and market_2_resolved:
            equity = portfolio["cash"]
            equity_curve.append({"datetime": str(t), "equity": equity})
            continue
        
        # Calculate days until end (for markets not yet resolved)
        days_until_end_1 = max(0, (end_date_1 - current_dt).total_seconds() / 86400) if not market_1_resolved else 0
        days_until_end_2 = max(0, (end_date_2 - current_dt).total_seconds() / 86400) if not market_2_resolved else 0
        
        d1 = int(max(1, round(days_until_end_1))) if days_until_end_1 > 0 else 1
        d2 = int(max(1, round(days_until_end_2))) if days_until_end_2 > 0 else 1
        
        # Get current prices
        c1 = float(row1["close"])
        c2 = float(row2["close"])
        
        if not (0.0 < c1 < 1.0 and 0.0 < c2 < 1.0):
            continue
        
        # Get robust recommendation
        try:
            result = robust_optimal_bet_q_agnostic(
                c1=c1, d1=d1, c2=c2, d2=d2,
                q_grid_size=q_grid_size,
                x_grid_size=x_grid_size,
                use_market_range=use_market_range,
                market_expand_factor=market_expand_factor,
            )
            
            if result.action == "no_bet" or result.worst_case_edge <= 0.0:
                # Update equity but don't trade
                # Use resolved prices for resolved markets
                price_A1 = 1.0 if market_1_resolved and portfolio["shares_A1"] > 0 else (0.0 if market_1_resolved and portfolio["shares_A1_NO"] > 0 else c1)
                price_A1_NO = 1.0 if market_1_resolved and portfolio["shares_A1_NO"] > 0 else (0.0 if market_1_resolved and portfolio["shares_A1"] > 0 else (1.0 - c1))
                price_A2 = 1.0 if market_2_resolved and portfolio["shares_A2"] > 0 else (0.0 if market_2_resolved and portfolio["shares_A2_NO"] > 0 else c2)
                price_A2_NO = 1.0 if market_2_resolved and portfolio["shares_A2_NO"] > 0 else (0.0 if market_2_resolved and portfolio["shares_A2"] > 0 else (1.0 - c2))
                
                equity = (
                    portfolio["cash"] +
                    portfolio["shares_A1"] * price_A1 +
                    portfolio["shares_A1_NO"] * price_A1_NO +
                    portfolio["shares_A2"] * price_A2 +
                    portfolio["shares_A2_NO"] * price_A2_NO
                )
                equity_curve.append({"datetime": str(t), "equity": equity})
                continue
            
            # Don't trade if market is already resolved
            if market_1_resolved or market_2_resolved:
                # Update equity but don't trade
                price_A1 = 1.0 if market_1_resolved and portfolio["shares_A1"] > 0 else (0.0 if market_1_resolved and portfolio["shares_A1_NO"] > 0 else c1)
                price_A1_NO = 1.0 if market_1_resolved and portfolio["shares_A1_NO"] > 0 else (0.0 if market_1_resolved and portfolio["shares_A1"] > 0 else (1.0 - c1))
                price_A2 = 1.0 if market_2_resolved and portfolio["shares_A2"] > 0 else (0.0 if market_2_resolved and portfolio["shares_A2_NO"] > 0 else c2)
                price_A2_NO = 1.0 if market_2_resolved and portfolio["shares_A2_NO"] > 0 else (0.0 if market_2_resolved and portfolio["shares_A2"] > 0 else (1.0 - c2))
                
                equity = (
                    portfolio["cash"] +
                    portfolio["shares_A1"] * price_A1 +
                    portfolio["shares_A1_NO"] * price_A1_NO +
                    portfolio["shares_A2"] * price_A2 +
                    portfolio["shares_A2_NO"] * price_A2_NO
                )
                equity_curve.append({"datetime": str(t), "equity": equity})
                continue
            
            # Execute trades based on recommendation
            if result.mode == "yesA1_noA2":
                # Mode 1: YES A1 + NO A2
                # x fraction on YES A1, (1-x) on NO A2
                x = result.x
                
                # Calculate quantities based on available cash
                cash_to_use = portfolio["cash"] * fraction
                cost_A1 = cash_to_use * x
                cost_A2_NO = cash_to_use * (1.0 - x)
                
                qty_A1 = cost_A1 / c1 if c1 > 0 else 0
                qty_A2_NO = cost_A2_NO / (1.0 - c2) if (1.0 - c2) > 0 else 0
                
                # Execute buys
                if qty_A1 > 0 and cost_A1 <= portfolio["cash"]:
                    portfolio["shares_A1"] += qty_A1
                    portfolio["cash"] -= cost_A1
                    if portfolio["shares_A1"] > 0:
                        portfolio["avg_entry_A1"] = (
                            portfolio["avg_entry_A1"] * (portfolio["shares_A1"] - qty_A1) + cost_A1
                        ) / portfolio["shares_A1"]
                    trades.append({
                        "datetime": str(t),
                        "type": "buy",
                        "market": "A1_YES",
                        "price": c1,
                        "quantity": qty_A1,
                        "pnl": None,
                    })
                
                if qty_A2_NO > 0 and cost_A2_NO <= portfolio["cash"]:
                    portfolio["shares_A2_NO"] += qty_A2_NO
                    portfolio["cash"] -= cost_A2_NO
                    if portfolio["shares_A2_NO"] > 0:
                        portfolio["avg_entry_A2_NO"] = (
                            portfolio["avg_entry_A2_NO"] * (portfolio["shares_A2_NO"] - qty_A2_NO) + cost_A2_NO
                        ) / portfolio["shares_A2_NO"]
                    trades.append({
                        "datetime": str(t),
                        "type": "buy",
                        "market": "A2_NO",
                        "price": 1.0 - c2,
                        "quantity": qty_A2_NO,
                        "pnl": None,
                    })
            
            else:  # mode == "noA1_yesA2"
                # Mode 2: NO A1 + YES A2
                # x fraction on NO A1, (1-x) on YES A2
                x = result.x
                
                cash_to_use = portfolio["cash"] * fraction
                cost_A1_NO = cash_to_use * x
                cost_A2 = cash_to_use * (1.0 - x)
                
                qty_A1_NO = cost_A1_NO / (1.0 - c1) if (1.0 - c1) > 0 else 0
                qty_A2 = cost_A2 / c2 if c2 > 0 else 0
                
                # Execute buys
                if qty_A1_NO > 0 and cost_A1_NO <= portfolio["cash"]:
                    portfolio["shares_A1_NO"] += qty_A1_NO
                    portfolio["cash"] -= cost_A1_NO
                    if portfolio["shares_A1_NO"] > 0:
                        portfolio["avg_entry_A1_NO"] = (
                            portfolio["avg_entry_A1_NO"] * (portfolio["shares_A1_NO"] - qty_A1_NO) + cost_A1_NO
                        ) / portfolio["shares_A1_NO"]
                    trades.append({
                        "datetime": str(t),
                        "type": "buy",
                        "market": "A1_NO",
                        "price": 1.0 - c1,
                        "quantity": qty_A1_NO,
                        "pnl": None,
                    })
                
                if qty_A2 > 0 and cost_A2 <= portfolio["cash"]:
                    portfolio["shares_A2"] += qty_A2
                    portfolio["cash"] -= cost_A2
                    if portfolio["shares_A2"] > 0:
                        portfolio["avg_entry_A2"] = (
                            portfolio["avg_entry_A2"] * (portfolio["shares_A2"] - qty_A2) + cost_A2
                        ) / portfolio["shares_A2"]
                    trades.append({
                        "datetime": str(t),
                        "type": "buy",
                        "market": "A2_YES",
                        "price": c2,
                        "quantity": qty_A2,
                        "pnl": None,
                    })
            
            last_recommendation = result
            
        except Exception as e:
            print(f"Error at {t}: {e}")
            continue
        
        # Update equity (use resolved prices for resolved markets)
        price_A1 = 1.0 if market_1_resolved and portfolio["shares_A1"] > 0 else (0.0 if market_1_resolved and portfolio["shares_A1_NO"] > 0 else c1)
        price_A1_NO = 1.0 if market_1_resolved and portfolio["shares_A1_NO"] > 0 else (0.0 if market_1_resolved and portfolio["shares_A1"] > 0 else (1.0 - c1))
        price_A2 = 1.0 if market_2_resolved and portfolio["shares_A2"] > 0 else (0.0 if market_2_resolved and portfolio["shares_A2_NO"] > 0 else c2)
        price_A2_NO = 1.0 if market_2_resolved and portfolio["shares_A2_NO"] > 0 else (0.0 if market_2_resolved and portfolio["shares_A2"] > 0 else (1.0 - c2))
        
        equity = (
            portfolio["cash"] +
            portfolio["shares_A1"] * price_A1 +
            portfolio["shares_A1_NO"] * price_A1_NO +
            portfolio["shares_A2"] * price_A2 +
            portfolio["shares_A2_NO"] * price_A2_NO
        )
        equity_curve.append({"datetime": str(t), "equity": equity})
    
    # Final equity is just cash (all positions should be liquidated by now)
    final_equity = portfolio["cash"]
    
    pnl = final_equity - initial_cash
    total_return_pct = (pnl / initial_cash) * 100
    
    # Calculate metrics
    equity_values = [e["equity"] for e in equity_curve]
    max_dd = max([(max(equity_values[:i+1]) - eq) / max(equity_values[:i+1]) if max(equity_values[:i+1]) > 0 else 0 
                  for i, eq in enumerate(equity_values)]) if equity_values else 0.0
    
    # Print results
    print(f"\n{'='*60}")
    print(f"BACKTEST RESULTS")
    print(f"{'='*60}")
    print(f"Initial cash:      ${initial_cash:.2f}")
    print(f"Final equity:      ${final_equity:.2f}")
    print(f"P&L:               ${pnl:.2f}")
    print(f"Total return:      {total_return_pct:.2f}%")
    print(f"Max drawdown:      {max_dd*100:.2f}%")
    print(f"\nPositions:")
    print(f"  A1 YES: {portfolio['shares_A1']:.2f} shares")
    print(f"  A1 NO:  {portfolio['shares_A1_NO']:.2f} shares")
    print(f"  A2 YES: {portfolio['shares_A2']:.2f} shares")
    print(f"  A2 NO:  {portfolio['shares_A2_NO']:.2f} shares")
    print(f"  Cash:   ${portfolio['cash']:.2f}")
    print(f"\nTrades: {len(trades)}")
    if last_recommendation:
        print(f"\nLast recommendation:")
        print(f"  Mode: {last_recommendation.mode}")
        print(f"  Action: {last_recommendation.action}")
        print(f"  Worst-case edge: {last_recommendation.worst_case_edge:.6f}")
    print(f"{'='*60}\n")
    
    if trades:
        print("Sample trades (first 10):")
        for i, trade in enumerate(trades[:10], 1):
            print(f"  {i}. {trade['datetime']}: {trade['type'].upper()} {trade['market']} "
                  f"{trade['quantity']:.2f} @ ${trade['price']:.4f}")
        if len(trades) > 10:
            print(f"  ... and {len(trades) - 10} more trades")
        print()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Test mode: run example calculations
        # Replace with your website quotes:
        c1, d1 = 0.8, 53
        c2, d2 = 0.17, 329

        # 1) Print robust recommendation (compares BOTH modes)
        # By default: uses a market-centered expanded q-range with factor 1.2
        res = robust_optimal_bet_q_agnostic(
            c1, d1, c2, d2,
            q_grid_size=80_000,
            x_grid_size=2_001,
            chunk_q=4_000,
            logit_s=6.0,
            use_market_range=True,
            market_expand_factor=1.2,
        )
        print(res)
        print("\nDiagnostics:")
        for k, v in res.details.items():
            print(f"  {k}: {v}")

        # 2) Plot surface + highlight robust x* for the chosen mode
        plot_edge_surface_and_highlight(
            c1, d1, c2, d2,
            robust_q_grid_size=80_000,
            robust_x_grid_size=2_001,
            robust_chunk_q=4_000,
            robust_logit_s=6.0,
            use_market_range=True,
            market_expand_factor=1.2,
            q_grid_size=600,
            x_grid_size=600,
        )
    
    elif len(sys.argv) > 1 and sys.argv[1] == "backtest":
        # ============================================================
        # CONFIGURATION DU BACKTEST - Modifiez ces variables
        # ============================================================
        
        # MODE: Backtest avec DEUX événements différents
        # Exemple: A1 = "War ends by Feb 24", A2 = "War ends by Mar 2"
        USE_TWO_MARKETS = True  # Mettez False pour utiliser l'ancien mode (un marché avec historique)
        
        if USE_TWO_MARKETS:
            # Chemins vers les DEUX fichiers CSV des événements à comparer
            # IMPORTANT: Les deux événements doivent être liés (même sujet, dates différentes)
            # A1 doit avoir une date de fin ANTÉRIEURE à A2
            CSV_PATH_A1 = "backtesting/data/iran1.csv"  # Événement avec date de fin plus proche
            CSV_PATH_A2 = "backtesting/data/iran2.csv"  # Événement avec date de fin plus lointaine
            
            # Paramètres du backtest
            INITIAL_CASH = 1000.0
            FRACTION = 1.0
            Q_GRID_SIZE = 80_000
            X_GRID_SIZE = 2_001
            USE_MARKET_RANGE = True
            MARKET_EXPAND_FACTOR = 1.2
            
            # Exécution du backtest avec deux marchés
            backtest_memoryless_two_markets(
                csv_path_1=CSV_PATH_A1,
                csv_path_2=CSV_PATH_A2,
                initial_cash=INITIAL_CASH,
                fraction=FRACTION,
                q_grid_size=Q_GRID_SIZE,
                x_grid_size=X_GRID_SIZE,
                use_market_range=USE_MARKET_RANGE,
                market_expand_factor=MARKET_EXPAND_FACTOR,
            )
        
        else:
            # ANCIEN MODE: Un seul marché avec prix historique (pour référence)
            CSV_PATH = "data/prices.csv"  # MODIFIEZ CE CHEMIN vers votre fichier CSV existant
            
            # Cash initial pour le backtest
            INITIAL_CASH = 1000.0
            
            # Paramètres de la stratégie
            LOOKBACK_DAYS = 7.0              # Nombre de jours pour le prix historique
            MIN_DAYS_UNTIL_END = 10.0         # Minimum de jours jusqu'à la fin pour trader
            MIN_HISTORY_DAYS = 7.0            # Minimum d'historique requis
            FRACTION = 1.0                    # Fraction de cash à utiliser (1.0 = all-in)
            
            # Paramètres d'optimisation (optionnel, valeurs par défaut recommandées)
            Q_GRID_SIZE = 80_000
            X_GRID_SIZE = 2_001
            USE_MARKET_RANGE = True
            MARKET_EXPAND_FACTOR = 1.2
            
            # ============================================================
            # Exécution du backtest
            # ============================================================
            backtest_memoryless_strategy(
                csv_path=CSV_PATH,
                initial_cash=INITIAL_CASH,
                lookback_days=LOOKBACK_DAYS,
                min_days_until_end=MIN_DAYS_UNTIL_END,
                min_history_days=MIN_HISTORY_DAYS,
                fraction=FRACTION,
                q_grid_size=Q_GRID_SIZE,
                x_grid_size=X_GRID_SIZE,
                use_market_range=USE_MARKET_RANGE,
                market_expand_factor=MARKET_EXPAND_FACTOR,
            )
    
    else:
        print("Usage:")
        print("  Test mode:    python memory_less_strats test")
        print("  Backtest mode: python memory_less_strats backtest")
        print("\nPour le mode backtest:")
        print("  1. Modifiez les variables de configuration dans le code")
        print("  2. Exécutez: python memory_less_strats backtest")
        print("\nDeux modes disponibles:")
        print("  - USE_TWO_MARKETS = True  : Compare deux événements différents (A1 et A2)")
        print("    Exemple: A1='War ends by Feb 24', A2='War ends by Mar 2'")
        print("    Configurez: CSV_PATH_A1 et CSV_PATH_A2")
        print("\n  - USE_TWO_MARKETS = False : Un seul marché avec prix historique")
        print("    Configurez: CSV_PATH")
