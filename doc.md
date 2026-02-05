# polypoly - Documentation

## Structure du projet

```
polymarket/
├── backtest.py              # Librairie interne (NE PAS MODIFIER)
│                            # Timeline, Bar, PolymarketClient, etc.
│
├── polypoly/
│   ├── extract.py           # Extraction de donnees -> CSV
│   ├── strategies.py        # Strategies de trading
│   ├── backtest.py          # Moteur de backtest + metriques
│   └── events_backfill/     # Pipeline Azure (NE PAS TOUCHER)
│
└── data/                    # CSVs generes
    └── bulk/                # CSVs de l'extraction bulk
```

---

## Format de donnees

### CSV (entree/sortie de extract.py, entree de backtest.py)

```csv
datetime,open,high,low,close
2024-06-15 14:00,0.45,0.48,0.43,0.465
2024-06-15 15:00,0.465,0.47,0.44,0.45
```

- **datetime** : `YYYY-MM-DD HH:MM` (UTC)
- **open/high/low/close** : prix entre 0 et 1 (probabilite du marche)

### Portfolio (dans le backtest)

```python
{
    "cash": 1000.0,       # argent disponible
    "shares": 0.0,        # nombre de parts detenues
    "avg_entry": 0.0,     # prix moyen d'achat
    "equity": 1000.0,     # cash + shares * prix courant
}
```

### Signal de strategie (sortie de `next()`)

```python
{"action": "buy", "quantity": 42.5}   # acheter 42.5 shares
{"action": "sell", "quantity": 10.0}   # vendre 10 shares
{"action": "hold"}                     # ne rien faire
```

### Resultats du backtest (sortie de `run_backtest()`)

```python
{
    "initial_cash": 1000.0,
    "final_equity": 1152.0,
    "pnl": 152.0,
    "total_return_pct": 15.2,
    "avg_return_per_trade": 38.0,
    "std_returns": 21.0,
    "max_drawdown_pct": 6.3,
    "sharpe_ratio": 1.81,
    "win_rate": 0.667,
    "win_trades": 4,
    "nb_trades": 6,
    "trades": [...],            # liste des trades executes
    "equity_curve": DataFrame,  # courbe d'equity
    "portfolio": {...},         # etat final du portfolio
}
```

---

## Process complet

### 1. Extraire des donnees

**Un seul event :**

```bash
# Lister les marches d'un event
python polypoly/extract.py "will-bitcoin-hit-100k" --list

# Extraire le premier marche en barres 1h
python polypoly/extract.py "will-bitcoin-hit-100k" -o data/btc100k.csv

# Choisir un marche specifique + granularite
python polypoly/extract.py "2024-us-election" --market "president" -g 5m -o data/election.csv

# Choisir l'outcome (0=Yes par defaut, 1=No)
python polypoly/extract.py "some-event" --outcome 1 -o data/no_side.csv
```

**Extraction bulk (tous les events entre deux dates) :**

```bash
# Voir ce qui serait extrait (dry-run)
python polypoly/extract.py --bulk --from 2025-01-01 --to 2025-06-30 --dry-run

# Seulement les events actifs
python polypoly/extract.py --bulk --from 2025-01-01 --to 2025-06-30 --active true --dry-run

# Extraire pour de vrai
python polypoly/extract.py --bulk --from 2025-06-01 --to 2025-06-30 -o data/juin2025/ -g 1h
```

Les fichiers bulk sont nommes `{event-slug}__{market-slug}.csv`.

**Options extract.py :**

| Option | Description |
|--------|-------------|
| `slug` | Slug de l'event (pas necessaire avec `--bulk`) |
| `-o, --output` | Chemin du CSV ou dossier (bulk) |
| `-g, --granularity` | Granularite : `1s 5s 10s 30s 1m 5m 15m 30m 1h 4h 1d` |
| `-m, --market` | Sous-chaine pour choisir un marche |
| `--outcome` | Index de l'outcome (0=Yes, 1=No) |
| `--list` | Lister les marches et quitter |
| `--bulk` | Mode bulk |
| `--from` | Date min (YYYY-MM-DD) |
| `--to` | Date max (YYYY-MM-DD) |
| `--active` | `true` ou `false` |
| `--dry-run` | Lister sans telecharger |

### 2. Ecrire une strategie

Editer `polypoly/strategies.py` (ou creer un nouveau fichier).

```python
class MaStrategie(Strategy):
    def __init__(self):
        self.sma_window = 10

    def init(self):
        """Appele une fois au debut."""
        self.last_signal = None

    def next(self, row, history, portfolio):
        """Appele a chaque bar.

        row = {datetime, open, high, low, close}
        history = DataFrame de toutes les lignes jusqu'ici
        portfolio = {cash, shares, avg_entry, equity}
        """
        price = row["close"]

        # Exemple : moyenne mobile simple
        if len(history) >= self.sma_window:
            sma = history["close"].tail(self.sma_window).mean()

            if price < sma * 0.95 and portfolio["cash"] > 0:
                qty = portfolio["cash"] / price
                return {"action": "buy", "quantity": qty}

            if price > sma * 1.05 and portfolio["shares"] > 0:
                qty = portfolio["shares"]
                return {"action": "sell", "quantity": qty}

        return {"action": "hold"}
```

Puis l'enregistrer dans le registre :

```python
STRATEGIES = {
    "buyhold": BuyAndHold(),
    "threshold": SimpleThreshold(),
    "ma_strat": MaStrategie(),          # <-- ajouter ici
}
```

**Ce que le moteur gere pour toi :**

- Si tu demandes d'acheter plus que ton cash, il clamp a ce que tu peux payer
- Si tu demandes de vendre plus que tes shares, il clamp a ce que tu possedes
- Le prix moyen d'entree (`avg_entry`) est mis a jour automatiquement
- L'equity est recalculee a chaque bar

### 3. Lancer un backtest

```bash
cd polypoly

# Backtest basique
python backtest.py ../data/btc100k.csv --strategy buyhold

# Avec plus de cash initial
python backtest.py ../data/btc100k.csv --strategy threshold --cash 5000

# Titre custom pour le rapport
python backtest.py ../data/btc100k.csv -s ma_strat -t "MA Strategy sur BTC 100k"
```

**Options backtest.py :**

| Option | Description |
|--------|-------------|
| `csv` | Chemin vers le fichier CSV |
| `-s, --strategy` | Nom de la strategie (doit etre dans `STRATEGIES`) |
| `--cash` | Capital initial (defaut: 1000) |
| `-t, --title` | Titre du rapport |

**Sortie :**

```
=============================================
 BACKTEST : BTC 100k
=============================================
 Capital initial :  $1,000.00
 Capital final :    $1,152.00
 P&L :              +$152.00
 Return :           +15.2%
 Avg return/trade : +$38.00
 Std returns :      $21.00
 Sharpe ratio :     1.81
 Max drawdown :     -6.3%
 Win rate :         66.7% (4/6 trades)
 Nb trades :        6
=============================================
```

### 4. Utiliser en Python (sans CLI)

```python
import sys
sys.path.insert(0, "polypoly")
from strategies import BuyAndHold, SimpleThreshold
from backtest import run_backtest, print_results

# Creer une strategie avec des params custom
strat = SimpleThreshold(buy_below=0.35, sell_above=0.70, fraction=0.5)

# Lancer le backtest
results = run_backtest("data/btc100k.csv", strat, initial_cash=2000)

# Afficher le rapport
print_results(results, title="Mon test custom")

# Acceder aux donnees
print(results["equity_curve"])        # DataFrame
print(results["trades"])              # Liste des trades
print(results["portfolio"])           # Etat final
```

---

## Metriques

| Metrique | Description |
|----------|-------------|
| **P&L** | Profit & Loss = equity finale - capital initial |
| **Total return %** | (P&L / capital initial) * 100 |
| **Avg return/trade** | Moyenne du P&L par trade (sell uniquement) |
| **Std returns** | Ecart-type du P&L par trade |
| **Max drawdown** | Plus grande chute depuis un pic (sur la courbe d'equity) |
| **Sharpe ratio** | mean(returns) / std(returns) sur la courbe d'equity |
| **Win rate** | % de trades avec P&L > 0 |
| **Nb trades** | Nombre de sells executes |

**Note :** Un "trade" est compte quand il y a un **sell**. Un buy-and-hold sans sell aura 0 trades mais un P&L non nul (profit non realise).

---

## Granularites disponibles

`1s` `5s` `10s` `30s` `1m` `5m` `15m` `30m` `1h` `4h` `1d`

Les granularites fines (`1s`, `5s`) produisent beaucoup de barres et sont plus lentes a extraire.
Pour un backtest classique, `1h` ou `1d` est un bon point de depart.
