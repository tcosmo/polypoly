# Polymarket API - Documentation technique

Deux APIs sont utilisees par le projet. Aucune authentification n'est requise pour la lecture.

---

## 1. Gamma API (metadata)

**Base URL :** `https://gamma-api.polymarket.com`

API REST pour les metadonnees des events et marches. Pas de doc officielle publique,
les endpoints et parametres sont documentes ici par observation.

### GET /events

Recherche et listing des events.

**Parametres :**

| Param | Type | Description |
|-------|------|-------------|
| `id` | string | ID d'un event specifique |
| `slug` | string | Slug d'un event specifique |
| `tag_slug` | string | Recherche par tag (ex: `bitcoin`, `us-elections`) |
| `active` | string | `"true"` / `"false"` - filtrer par statut actif |
| `limit` | int | Nombre max de resultats (defaut ~20, max observe ~100) |
| `offset` | int | Pagination : sauter les N premiers resultats |
| `start_date_min` | ISO 8601 | Date de debut minimum (ex: `2025-01-01T00:00:00Z`) |
| `end_date_max` | ISO 8601 | Date de fin maximum (ex: `2025-12-31T23:59:59Z`) |

**Reponse :** Array JSON d'objets event.

**Exemple :**
```
GET /events?slug=will-bitcoin-hit-100k
GET /events?active=true&limit=50&offset=0
GET /events?start_date_min=2025-01-01T00:00:00Z&end_date_max=2025-06-30T23:59:59Z&limit=50
```

**Structure d'un event :**
```json
{
    "id": "12345",
    "title": "Will Bitcoin hit $100k?",
    "slug": "will-bitcoin-hit-100k",
    "description": "...",
    "negRisk": false,
    "closed": false,
    "active": true,
    "volume": "1234567.89",
    "startDate": "2024-01-15T00:00:00Z",
    "endDate": "2025-12-31T00:00:00Z",
    "createdAt": "2024-01-15T12:00:00.000Z",
    "updatedAt": "2025-06-01T10:00:00.000Z",
    "closedTime": null,
    "tags": [{"label": "crypto"}, {"label": "bitcoin"}],
    "markets": [
        {
            "id": "0x...",
            "question": "Will Bitcoin exceed $100,000?",
            "conditionId": "0x...",
            "slug": "will-bitcoin-exceed-100000",
            "outcomes": "[\"Yes\",\"No\"]",
            "clobTokenIds": "[\"token_yes_id\",\"token_no_id\"]",
            "outcomePrices": "[\"0.65\",\"0.35\"]",
            "volumeNum": 987654.32,
            "endDate": "2025-12-31T00:00:00Z",
            "closed": false
        }
    ]
}
```

**Notes :**
- `outcomes`, `clobTokenIds`, `outcomePrices` sont des strings JSON (pas des arrays natifs)
- Un event peut contenir plusieurs markets (ex: "Who will win?" avec un marche par candidat)
- `volume` est parfois un string, parfois un float
- La pagination fonctionne avec `limit` + `offset`, pas de champ `total` dans la reponse

### Pagination

Pas de header ou champ indiquant le nombre total. Il faut paginer jusqu'a recevoir
moins de resultats que `limit` :

```python
offset = 0
while True:
    data = get(f"/events?limit=50&offset={offset}")
    if len(data) < 50:
        break
    offset += 50
```

---

## 2. CLOB API (prix et orderbook)

**Base URL :** `https://clob.polymarket.com`

API pour les donnees de prix historiques et l'orderbook en temps reel.

### GET /prices-history

Historique des prix d'un token.

**Parametres :**

| Param | Type | Description |
|-------|------|-------------|
| `market` | string | **Requis.** Token ID (le `clobTokenIds` d'un outcome) |
| `startTs` | int | Timestamp unix UTC de debut |
| `endTs` | int | Timestamp unix UTC de fin |
| `interval` | enum | Duree relative a maintenant. **Mutuellement exclusif avec startTs/endTs.** Valeurs: `1h`, `6h`, `1d`, `1w`, `1m`, `max` |
| `fidelity` | int | Resolution en **minutes**. Minimum utile : 2. |

**Modes d'utilisation :**

| Mode | Limite | Fidelity respectee |
|------|--------|--------------------|
| `startTs` seul | **Aucune** (teste sur 232j / 334k pts) | Oui, toute valeur |
| `startTs` + `endTs` | **Max 15 jours** (HTTP 400 au-dela) | Oui, toute valeur |
| `interval` seul | Fenetre relative a maintenant | **Non** : fidelity=1 donne 12h |
| `startTs` + `interval` | interval prend le dessus | **Non** : fidelity degradee |
| `endTs` seul | N/A | HTTP 400 |

**Recommandation : toujours utiliser `startTs` seul.** Ne jamais passer `interval`.

**Fidelity (teste empiriquement, marche SOL/ETH 24j + PA Senate 232j) :**

| fidelity | Espacement reel | Exemple (SOL/ETH 24j) |
|----------|-----------------|------------------------|
| `2` | 2 min (120s) | 17,733 points |
| `3` | 3 min (180s) | 11,824 points |
| `5` | 5 min (300s) | 7,094 points |
| `10` | 10 min (600s) | 3,547 points |
| `60` | 1h (3600s) | 591 points |
| `720` | 12h (43200s) | 49 points |
| `1` | 1 min (60s) | 1,440 pts/jour. **BUG si combine avec `interval=max`** (donne 12h) |

L'unite est bien en minutes (conforme a la doc officielle).
`fidelity=1` marche correctement avec `startTs`/`endTs` ou `startTs` seul.
**Bug** : `fidelity=1` + `interval=max` donne 12h au lieu de 1 min.
Solution : ne pas mixer `interval` avec `startTs` quand on veut du 1 min.

**Reponse :**
```json
{
    "history": [
        {"t": 1718438400, "p": "0.45"},
        {"t": 1718442000, "p": "0.47"},
        ...
    ]
}
```

**Exemples :**
```
GET /prices-history?market=TOKEN_ID&interval=max&fidelity=60
GET /prices-history?market=TOKEN_ID&startTs=1704067200&endTs=1705276800&fidelity=5
GET /prices-history?market=TOKEN_ID&startTs=1704067200&fidelity=5
```

**Notes :**
- `t` = timestamp unix (secondes)
- `p` = prix sous forme de string (doit etre converti en float)
- Les points retournes sont des ticks bruts, pas des barres OHLC.
  L'aggregation en barres est faite cote client par `backtest.py`
- `interval=1m` (1 mois) retourne HTTP 400 (bug cote serveur)

**Limite des 15 jours avec `startTs` + `endTs` (CONFIRME) :**

| Fenetre | Resultat |
|---------|----------|
| <= 15 jours | OK, retourne les points a la resolution demandee |
| 16+ jours | **HTTP 400** |

Teste sur le marche PA Senate (fidelity=5) :
```
12j -> 3457 pts    15j -> 4321 pts    16j -> HTTP 400    30j -> HTTP 400
```

**Contournements :**
1. **`startTs` seul (sans `endTs`)** : retourne tout depuis startTs sans limite.
   Teste : 15,024 points sur 52 jours, 66,869 points sur 232 jours. Filtrer cote client.
2. **`interval=max`** : retourne tout l'historique (mais 0 points sur marches fermes).
3. **Chunker par paquets de 15 jours** si on a besoin de `endTs`.

**Marches fermes :**
- `interval=max` seul (sans `startTs`) retourne souvent 0 points
- **Ce qui marche** : `startTs` seul, avec un startTs large (ex: 30j avant l'ouverture)
- `startTs` + `endTs` marche aussi tant que l'ecart <= 15 jours

### GET /book

Orderbook en temps reel (carnet d'ordres).

**Parametres :**

| Param | Type | Description |
|-------|------|-------------|
| `token_id` | string | **Requis.** Token ID |

**Reponse :**
```json
{
    "market": "TOKEN_ID",
    "asset_id": "TOKEN_ID",
    "bids": [
        {"price": "0.45", "size": "100.0"},
        {"price": "0.44", "size": "250.0"}
    ],
    "asks": [
        {"price": "0.46", "size": "150.0"},
        {"price": "0.47", "size": "200.0"}
    ]
}
```

**Notes :**
- Pas cache (donnees live)
- `price` et `size` sont des strings

---

## 3. Concepts cles

### Event vs Market

- **Event** = une question generale ("2024 US Election")
- **Market** = un sous-marche specifique ("Will Biden win?", "Will Trump win?")
- Un event peut avoir 1 seul market (binaire) ou N markets (multi-choix)

### Outcomes et Tokens

Chaque market a 2+ outcomes (generalement "Yes" et "No").
Chaque outcome a un **token ID** unique sur le CLOB.

```
Event: "2024 US Election"
 └── Market: "Will Trump win?"
      ├── Outcome "Yes" -> token_id = "abc123..."  (prix = 0.65)
      └── Outcome "No"  -> token_id = "def456..."  (prix = 0.35)
```

Les prix des outcomes d'un meme marche binaire somment a ~1.0.

### Prix

- Les prix sont entre **0.0 et 1.0** (probabilites)
- 0.65 = le marche estime 65% de chance que l'outcome se realise
- A la resolution, le prix converge vers 0.0 ou 1.0

### Slug

Identifiant lisible dans l'URL. Exemple :
- URL Polymarket : `https://polymarket.com/event/will-bitcoin-hit-100k`
- Slug : `will-bitcoin-hit-100k`

---

## 4. Aggregation des prix

L'API CLOB retourne des **ticks bruts** (timestamp + prix).
Le fichier `backtest.py` (racine) les agrege en **barres OHLC** :

```
Ticks bruts :  t=100 p=0.45, t=110 p=0.48, t=120 p=0.43, t=130 p=0.46
                    ↓ aggregation en barre 1h ↓
Barre :        open=0.45, high=0.48, low=0.43, close=0.46
```

Granularites supportees :

| Code | Duree |
|------|-------|
| `1s` | 1 seconde |
| `5s` | 5 secondes |
| `10s` | 10 secondes |
| `30s` | 30 secondes |
| `1m` | 1 minute |
| `5m` | 5 minutes |
| `15m` | 15 minutes |
| `30m` | 30 minutes |
| `1h` | 1 heure |
| `4h` | 4 heures |
| `1d` | 1 jour |

Le parametre `fidelity` de l'API CLOB est automatiquement ajuste en fonction de la granularite
choisie (fidelity = granularite en minutes, minimum 1).

---

## 5. Cache

Les requetes API sont cachees sur disque dans `~/.cache/polymarket_backtest/`.

- Les requetes vers Gamma (`/events`) avec un slug/id precis sont cachees
- Les recherches d'events (`search_events`) ne sont PAS cachees
- Les requetes vers CLOB (`/prices-history`) sont cachees
- L'orderbook (`/book`) n'est PAS cache

Pour forcer un rafraichissement, supprimer le cache :
```bash
rm -rf ~/.cache/polymarket_backtest/
```

---

## 6. Rate limiting

Le client applique un rate limit de **10 requetes/seconde** par defaut.
C'est gere automatiquement, pas besoin de s'en occuper.

L'API Polymarket n'a pas de rate limit documente, mais un exces de requetes
peut resulter en des erreurs HTTP 429.
