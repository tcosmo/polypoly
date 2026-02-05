"""
Tests manuels de l'API CLOB Polymarket /prices-history.

Lance ce script pour constater les comportements documentes dans polymarket_api.md.

Usage:
    python polypoly/test_api.py
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "https://clob.polymarket.com/prices-history"

# --- Tokens de test ---

# SOL/ETH Up or Down in May? (marche ferme, 24 jours, faible volume)
# Outcome "Up"
TOKEN_SOLETH = "103142045332418635900492391066357924909131656560718668180680871206046899519836"
SOLETH_START = 1746662400  # 8 mai 2025

# PA Senate Election (marche ferme, 232 jours, 2.3M volume)
# Outcome "Democrat wins"
TOKEN_PA = "58059853537451964777059792095816455340743648080733545719045977017101901211397"
PA_START = 1711929600  # 1 avril 2024


def fetch(token, **params):
    """GET /prices-history et retourne (http_code, history_list)."""
    params["market"] = token
    try:
        r = requests.get(BASE, params=params, timeout=15)
        if r.status_code != 200:
            return r.status_code, []
        h = r.json().get("history", [])
        return 200, h
    except Exception as e:
        return 0, []


def describe(history):
    """Decrit une reponse : nb points, espacement, range."""
    n = len(history)
    if n == 0:
        return "0 pts"
    if n == 1:
        t = datetime.fromtimestamp(history[0]["t"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        return f"1 pt ({t})"
    gaps = [history[i]["t"] - history[i - 1]["t"] for i in range(1, n)]
    med = sorted(gaps)[len(gaps) // 2]
    d1 = datetime.fromtimestamp(history[0]["t"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    d2 = datetime.fromtimestamp(history[-1]["t"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    jours = (history[-1]["t"] - history[0]["t"]) / 86400
    return f"{n:,} pts, median {med}s ({med/60:.0f}min), {d1} -> {d2} ({jours:.0f}j)"


def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def test(label, token, expected_ok=True, **params):
    code, h = fetch(token, **params)
    status = f"HTTP {code}"
    desc = describe(h)
    ok = "OK" if (code == 200 and len(h) > 0) == expected_ok else "UNEXPECTED"
    print(f"  {ok:10s} | {label:45s} | {status} | {desc}")
    return code, h


def main():
    print("Test de l'API CLOB Polymarket /prices-history")
    print(f"Token SOL/ETH : ...{TOKEN_SOLETH[-20:]}")
    print(f"Token PA Senate : ...{TOKEN_PA[-20:]}")

    # =====================================================================
    section("1. MODE startTs SEUL (recommande)")
    # =====================================================================
    print("  Pas de limite de duree, fidelity respectee.\n")

    test("startTs seul, fidelity=1 (SOL/ETH 24j)",
         TOKEN_SOLETH, startTs=SOLETH_START, fidelity=1)

    test("startTs seul, fidelity=1 (PA 232j)",
         TOKEN_PA, startTs=PA_START, fidelity=1)

    test("startTs seul, fidelity=5 (PA 232j)",
         TOKEN_PA, startTs=PA_START, fidelity=5)

    test("startTs seul, fidelity=60 (PA 232j)",
         TOKEN_PA, startTs=PA_START, fidelity=60)

    # =====================================================================
    section("2. MODE startTs + endTs (max 15 jours)")
    # =====================================================================
    print("  Fidelity respectee, mais HTTP 400 si > 15 jours.\n")

    for days in [1, 7, 14, 15, 16, 30]:
        end = SOLETH_START + days * 86400
        ok = days <= 15
        test(f"startTs+endTs, {days}j, fidelity=1",
             TOKEN_SOLETH, expected_ok=ok, startTs=SOLETH_START, endTs=end, fidelity=1)

    print()
    test("startTs+endTs 14j, fidelity=5 (PA)",
         TOKEN_PA, startTs=1727740800, endTs=1727740800 + 14 * 86400, fidelity=5)

    test("startTs+endTs 16j, fidelity=5 (PA)",
         TOKEN_PA, expected_ok=False, startTs=1727740800, endTs=1727740800 + 16 * 86400, fidelity=5)

    # =====================================================================
    section("3. MODE interval (A EVITER)")
    # =====================================================================
    print("  Degrade la fidelity. fidelity=1 donne 12h au lieu de 1min.\n")

    test("interval=max, fidelity=1 (SOL/ETH)",
         TOKEN_SOLETH, expected_ok=False, interval="max", fidelity=1)

    test("interval=max, fidelity=60 (SOL/ETH)",
         TOKEN_SOLETH, interval="max", fidelity=60)

    test("interval=max seul, marche ferme (PA)",
         TOKEN_PA, expected_ok=False, interval="max", fidelity=5)

    # =====================================================================
    section("4. startTs + interval=max (PIEGE)")
    # =====================================================================
    print("  interval=max prend le dessus et degrade la fidelity.\n")

    test("startTs+interval=max, fidelity=1",
         TOKEN_SOLETH, startTs=SOLETH_START, interval="max", fidelity=1)

    print()
    print("  Comparaison : meme requete SANS interval :")
    test("startTs seul, fidelity=1",
         TOKEN_SOLETH, startTs=SOLETH_START, fidelity=1)

    # =====================================================================
    section("5. MODES QUI MARCHENT PAS")
    # =====================================================================

    test("endTs seul",
         TOKEN_PA, expected_ok=False, endTs=1732233600, fidelity=5)

    test("interval=1m (bug serveur)",
         TOKEN_PA, expected_ok=False, interval="1m", fidelity=5)

    # =====================================================================
    section("6. FIDELITY = MINUTES (verification)")
    # =====================================================================
    print("  On verifie que fidelity N donne bien un espacement de N minutes.\n")

    for fid in [1, 2, 3, 5, 10, 60]:
        test(f"fidelity={fid} (attendu: {fid}min)",
             TOKEN_SOLETH, startTs=SOLETH_START, endTs=SOLETH_START + 86400, fidelity=fid)

    # =====================================================================
    section("RESUME")
    # =====================================================================
    print("""
  +----------------------------+---------------+--------------------+
  | Mode                       | Limite        | Fidelity OK ?      |
  +----------------------------+---------------+--------------------+
  | startTs seul               | AUCUNE        | OUI (toute valeur) |
  | startTs + endTs            | max 15 jours  | OUI (toute valeur) |
  | interval (seul ou mixe)    | -             | NON (degradee)     |
  | endTs seul                 | -             | HTTP 400           |
  +----------------------------+---------------+--------------------+

  Recommandation : toujours utiliser startTs seul + fidelity en minutes.
""")


if __name__ == "__main__":
    main()
