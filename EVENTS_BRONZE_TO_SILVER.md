# Events Bronze to Silver Transformation

## Data Model

Polymarket events data follows a hierarchical structure:

```
Series (optional)
  └── Events
        ├── Markets[]
        ├── Tags[]
        └── EventCreators[] (rare)
```

- **Series**: Similar events
- **Events**: Individual prediction events
- **Markets**: Trading markets within an event (Yes/No outcomes)
- **Tags**: Labels for categorization
- **EventCreators**: External creators (Twitter personalities, etc.)

## Silver Tables

### Relationships

| Table | Primary Key | Foreign Keys |
|-------|-------------|--------------|
| `events` | `id` | `seriesSlug` → `series.slug` |
| `markets` | `id` | `event_id` → `events.id` |
| `series` | `slug` | - |
| `tags` | `id` | - |
| `event_tags` | (`event_id`, `tag_id`) | `event_id` → `events.id`, `tag_id` → `tags.id` |
| `event_creators` | `id` | `event_id` → `events.id` |

**Note**: `event_id` does not exist in the source JSON. Markets, tags, and creators are nested arrays inside each event. During ETL, we extract them and add `event_id` from the parent event to establish the relationship.

### Schema Derivation

Schemas were derived by analyzing 189,782 events from ADLS Bronze layer:
- Fields present in **≥90%** of records → `NOT NULL`
- Fields present in **<90%** but analytically important → nullable (force-included)
- Fields present in **<50%** → generally excluded, **except** for analytically important fields (e.g., `category` at 1.5%, `competitive` at 49.5%)

See `events_schema_analysis/events_schema.txt` for full field presence analysis.

---

## SQL Schemas (Databricks Spark)

### events

```sql
CREATE TABLE silver.events (
    -- Primary key
    id STRING NOT NULL,
    slug STRING NOT NULL,

    -- Metadata (added during ETL)
    _fetched_at TIMESTAMP NOT NULL,     -- When this data was fetched from Polymarket API

    -- Core fields
    title STRING NOT NULL,
    description STRING,
    image STRING NOT NULL,
    icon STRING NOT NULL,
    ticker STRING NOT NULL,

    -- Status flags
    active BOOLEAN NOT NULL,
    closed BOOLEAN NOT NULL,
    archived BOOLEAN NOT NULL,
    restricted BOOLEAN NOT NULL,
    featured BOOLEAN NOT NULL,
    new BOOLEAN NOT NULL,

    -- Timestamps
    createdAt TIMESTAMP NOT NULL,
    updatedAt TIMESTAMP NOT NULL,
    startDate TIMESTAMP NOT NULL,
    endDate TIMESTAMP NOT NULL,
    creationDate TIMESTAMP,
    closedTime TIMESTAMP,

    -- Configuration
    cyom BOOLEAN NOT NULL,
    showAllOutcomes BOOLEAN NOT NULL,
    showMarketImages BOOLEAN NOT NULL,
    enableNegRisk BOOLEAN NOT NULL,
    negRiskAugmented BOOLEAN NOT NULL,
    pendingDeployment BOOLEAN NOT NULL,
    deploying BOOLEAN NOT NULL,
    requiresTranslation BOOLEAN NOT NULL,
    enableOrderBook BOOLEAN,
    automaticallyResolved BOOLEAN,
    negRisk BOOLEAN,

    -- Metrics
    commentCount BIGINT,
    openInterest BIGINT,

    -- Resolution
    resolutionSource STRING,

    -- Force-included fields (below 90% threshold but analytically important)
    seriesSlug STRING,              -- 88.3% - links to series table
    category STRING                 -- 1.5% - useful for filtering
);
```

### markets

```sql
CREATE TABLE silver.markets (
    -- Primary key
    id STRING NOT NULL,
    slug STRING NOT NULL,

    -- Metadata (added during ETL)
    event_id STRING NOT NULL,           -- From parent event
    _fetched_at TIMESTAMP NOT NULL,     -- When this data was fetched from Polymarket API

    -- Core fields
    question STRING NOT NULL,
    description STRING NOT NULL,
    conditionId STRING NOT NULL,
    outcomes STRING NOT NULL,           -- JSON array: ["Yes", "No"]
    outcomePrices STRING,               -- JSON array of prices

    -- Addresses
    marketMakerAddress STRING NOT NULL,
    questionID STRING,
    clobTokenIds STRING NOT NULL,       -- JSON array of token IDs

    -- Status flags
    active BOOLEAN NOT NULL,
    closed BOOLEAN NOT NULL,
    archived BOOLEAN NOT NULL,
    restricted BOOLEAN NOT NULL,
    ready BOOLEAN NOT NULL,
    funded BOOLEAN NOT NULL,
    approved BOOLEAN NOT NULL,
    featured BOOLEAN,
    new BOOLEAN NOT NULL,
    acceptingOrders BOOLEAN,

    -- Timestamps
    createdAt TIMESTAMP NOT NULL,
    updatedAt TIMESTAMP NOT NULL,
    startDate TIMESTAMP,
    endDate TIMESTAMP,
    closedTime TIMESTAMP,
    acceptingOrdersTimestamp TIMESTAMP, -- 86.9%
    umaEndDate TIMESTAMP,

    -- ISO date strings (kept for compatibility)
    startDateIso STRING,
    endDateIso STRING,

    -- Images
    image STRING,
    icon STRING,

    -- Configuration
    cyom BOOLEAN NOT NULL,
    pagerDutyNotificationEnabled BOOLEAN NOT NULL,
    clearBookOnStart BOOLEAN NOT NULL,
    manualActivation BOOLEAN NOT NULL,
    negRiskOther BOOLEAN NOT NULL,
    pendingDeployment BOOLEAN NOT NULL,
    deploying BOOLEAN NOT NULL,
    rfqEnabled BOOLEAN NOT NULL,
    holdingRewardsEnabled BOOLEAN NOT NULL,
    feesEnabled BOOLEAN NOT NULL,
    requiresTranslation BOOLEAN NOT NULL,
    enableOrderBook BOOLEAN,
    automaticallyActive BOOLEAN,
    negRisk BOOLEAN,
    hasReviewedDates BOOLEAN,

    -- Trading parameters
    rewardsMinSize DOUBLE NOT NULL,
    rewardsMaxSpread DOUBLE NOT NULL,
    spread DOUBLE NOT NULL,
    bestAsk DOUBLE NOT NULL,
    orderMinSize DOUBLE,
    orderPriceMinTickSize DOUBLE,

    -- Volume & pricing
    volume STRING,                      -- String in source (large numbers)
    volumeNum DOUBLE,
    volumeClob DOUBLE,
    lastTradePrice DOUBLE,              -- 87.0%
    oneDayPriceChange DOUBLE,           -- 73.4%
    competitive DOUBLE,                 -- 49.5%

    -- Resolution
    umaResolutionStatuses STRING NOT NULL,
    umaResolutionStatus STRING,
    resolutionSource STRING,            -- 83.1%
    resolvedBy STRING,                  -- 78.1%
    submitted_by STRING,                -- 74.8%
    automaticallyResolved BOOLEAN       -- 88.3%
);
```

### series

```sql
CREATE TABLE silver.series (
    -- Primary key
    slug STRING NOT NULL,
    id STRING NOT NULL,

    -- Metadata (added during ETL)
    _fetched_at TIMESTAMP NOT NULL,     -- When this data was fetched from Polymarket API

    -- Core fields
    ticker STRING NOT NULL,
    title STRING NOT NULL,

    -- Status flags
    active BOOLEAN NOT NULL,
    closed BOOLEAN NOT NULL,
    archived BOOLEAN NOT NULL,
    requiresTranslation BOOLEAN NOT NULL,

    -- Timestamps
    createdAt TIMESTAMP NOT NULL,
    updatedAt TIMESTAMP NOT NULL,

    -- Metrics
    commentCount BIGINT NOT NULL,

    -- Configuration
    seriesType STRING NOT NULL,         -- e.g., "single"
    recurrence STRING NOT NULL          -- e.g., "weekly"
);
```

### tags

```sql
CREATE TABLE silver.tags (
    -- Primary key
    id STRING NOT NULL,
    slug STRING NOT NULL,

    -- Core fields
    label STRING NOT NULL,

    -- Status
    requiresTranslation BOOLEAN NOT NULL,

    -- Timestamps
    createdAt TIMESTAMP,
    updatedAt TIMESTAMP NOT NULL
);
```

### event_tags (junction table)

```sql
CREATE TABLE silver.event_tags (
    event_id STRING NOT NULL,
    tag_id STRING NOT NULL
);
```

### event_creators

```sql
CREATE TABLE silver.event_creators (
    -- Primary key
    id STRING NOT NULL,

    -- Foreign key (add during ETL)
    event_id STRING NOT NULL,

    -- Core fields
    creatorName STRING NOT NULL,
    creatorHandle STRING NOT NULL,
    creatorUrl STRING NOT NULL,
    creatorImage STRING NOT NULL,

    -- Timestamps
    createdAt TIMESTAMP NOT NULL
);
```

---

## Handling Missing Data

### Strategy by Field Type

| Scenario | Strategy | Example |
|----------|----------|---------|
| `NOT NULL` field missing | **Fail/reject record** | `id`, `slug`, `title` |
| Nullable field missing | **Set to NULL** | `description`, `closedTime` |
| Boolean missing | **Default to FALSE** | `enableOrderBook` |
| Numeric missing | **Set to NULL** (not 0) | `commentCount`, `volumeNum` |
| Timestamp missing | **Set to NULL** | `closedTime`, `acceptingOrdersTimestamp` |
| JSON array missing | **Set to empty array `[]`** | `outcomes`, `outcomePrices` |

### PySpark Implementation

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

def transform_events(df):
    """Transform Bronze events to Silver schema."""
    return df.select(
        # NOT NULL fields - will fail if missing
        F.col("id").cast(StringType()).alias("id"),
        F.col("slug").cast(StringType()).alias("slug"),
        F.col("title").cast(StringType()).alias("title"),

        # Nullable fields - coalesce handles missing
        F.col("description").cast(StringType()).alias("description"),

        # Booleans - default to False if missing
        F.coalesce(F.col("active"), F.lit(False)).cast(BooleanType()).alias("active"),
        F.coalesce(F.col("enableOrderBook"), F.lit(False)).cast(BooleanType()).alias("enableOrderBook"),

        # Timestamps - parse ISO strings
        F.to_timestamp(F.col("createdAt")).alias("createdAt"),
        F.to_timestamp(F.col("closedTime")).alias("closedTime"),  # nullable

        # Numeric - cast, NULL if missing
        F.col("commentCount").cast(LongType()).alias("commentCount"),
        F.col("openInterest").cast(LongType()).alias("openInterest"),

        # Force-included fields (may be NULL)
        F.col("seriesSlug").cast(StringType()).alias("seriesSlug"),
        F.col("category").cast(StringType()).alias("category"),
    )
```

### Data Quality Checks

```python
# Check for required fields
required_fields = ["id", "slug", "title", "createdAt"]

def validate_events(df):
    """Validate required fields are present."""
    for field in required_fields:
        null_count = df.filter(F.col(field).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Found {null_count} records with NULL {field}")
    return df
```

### Deduplication

Series and Tags appear multiple times across events. Deduplicate by `slug`:

```python
def dedupe_series(df):
    """Keep latest version of each series by slug."""
    from pyspark.sql.window import Window

    window = Window.partitionBy("slug").orderBy(F.col("updatedAt").desc())
    return df.withColumn("row_num", F.row_number().over(window)) \
             .filter(F.col("row_num") == 1) \
             .drop("row_num")
```

---

## ETL Notes

### Adding `_fetched_at`

The `_fetched_at` timestamp indicates when the data was fetched from the Polymarket API. This comes from the Bronze layer file path (ingest date) or file metadata:

```python
# Option 1: From file path (e.g., ingest_date=2026-02-05)
events_df = events_df.withColumn("_fetched_at", F.to_timestamp(F.lit(ingest_date)))

# Option 2: From file modification time
events_df = events_df.withColumn("_fetched_at", F.input_file_name())  # then parse
```

Markets and series inherit `_fetched_at` from their parent event payload.

### Extracting Nested Arrays

Markets, tags, and series are nested in the Bronze events JSON. Extract with `explode`:

```python
# Extract markets from events (include _fetched_at from parent)
markets_df = events_df.select(
    F.col("id").alias("event_id"),
    F.col("_fetched_at"),
    F.explode("markets").alias("market")
).select(
    "event_id",
    "_fetched_at",
    "market.*"
)

# Extract tags
event_tags_df = events_df.select(
    F.col("id").alias("event_id"),
    F.explode("tags").alias("tag")
).select(
    "event_id",
    F.col("tag.id").alias("tag_id")
)
```

### Timestamp Parsing

Bronze timestamps are ISO 8601 strings with varying formats:
- `2024-04-25T19:57:53.317939Z`
- `2023-07-12 16:33:04+00`
- `2024-08-17` (date only)

Use Spark's `to_timestamp` which handles multiple formats:

```python
F.to_timestamp(F.col("createdAt"))
```

### Volume Fields

Some volume fields are stored as strings (to handle large numbers):
- `volume`: STRING (e.g., "89665.252158")
- `volumeNum`: DOUBLE

Keep both - use `volumeNum` for calculations, `volume` for exact values.

---

## Implementation

See `databricks/events_bronze_to_silver.py` for the complete Databricks notebook that implements this transformation.
