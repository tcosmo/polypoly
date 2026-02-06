# Databricks notebook source
# MAGIC %md
# MAGIC # Events Bronze to Silver ETL
# MAGIC
# MAGIC Transforms Polymarket events from Bronze (JSONL) to Silver (Delta tables).
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `silver.events`
# MAGIC - `silver.markets`
# MAGIC - `silver.series`
# MAGIC - `silver.tags`
# MAGIC - `silver.event_tags`
# MAGIC - `silver.event_creators`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Storage configuration
STORAGE_ACCOUNT = "polybotdlso94zgo"
CONTAINER = "polypoly"

# ADLS authentication via service principal
client_id = dbutils.secrets.get(scope="keyv-polybot", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="keyv-polybot", key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope="keyv-polybot", key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
)
spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
)

# Bronze source path (backfill data)
BRONZE_BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/polymarket/events/mode=backfill"

# Silver destination path
SILVER_BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/polymarket"

# Ingest date to process (None = all available dates)
INGEST_DATE = "2026-02-05"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    LongType, DoubleType, TimestampType, ArrayType
)
from pyspark.sql.window import Window


def parse_timestamp(col):
    """
    Parse timestamp from various formats.
    Tries common formats in order:
    1. ISO 8601 (default Spark parsing)
    2. "February 6, 2022" format
    3. "Feb 6, 2022" format
    """
    return F.coalesce(
        F.try_to_timestamp(col),                              # ISO 8601: 2024-04-25T19:57:53Z
        F.try_to_timestamp(col, F.lit("MMMM d, yyyy")),       # February 6, 2022
        F.try_to_timestamp(col, F.lit("MMM d, yyyy")),        # Feb 6, 2022
        F.try_to_timestamp(col, F.lit("MMMM dd, yyyy")),      # February 06, 2022
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Data

# COMMAND ----------

def parse_json_objects(text):
    """
    Parse concatenated JSON objects from text, handling any separator.
    Tracks brace depth and string escaping to find complete objects.
    Same approach as data_explore/index.html parser.
    """
    if not text:
        return []

    results = []
    i = 0
    n = len(text)

    while i < n:
        # Skip whitespace and non-brace chars
        while i < n and text[i] != '{':
            i += 1
        if i >= n:
            break

        # Found start of JSON object
        start = i
        depth = 0
        in_string = False
        escape = False

        while i < n:
            char = text[i]

            if escape:
                escape = False
            elif char == '\\' and in_string:
                escape = True
            elif char == '"':
                in_string = not in_string
            elif not in_string:
                if char == '{':
                    depth += 1
                elif char == '}':
                    depth -= 1
                    if depth == 0:
                        i += 1
                        results.append(text[start:i])
                        break
            i += 1

    return results


def read_bronze_events(ingest_date=None):
    """
    Read Bronze JSONL files from ADLS.

    Uses a UDF to parse concatenated JSON objects (handles any separator).

    Args:
        ingest_date: Optional specific date (e.g., "2026-02-05"). If None, reads all dates.

    Returns:
        DataFrame with raw events and _fetched_at metadata
    """
    from pyspark.sql.types import ArrayType, StringType

    # Register UDF
    parse_json_udf = F.udf(parse_json_objects, ArrayType(StringType()))

    if ingest_date:
        path = f"{BRONZE_BASE_PATH}/ingest_date={ingest_date}/*.jsonl"
    else:
        path = f"{BRONZE_BASE_PATH}/ingest_date=*/*.jsonl"

    print(f"Reading Bronze data from: {path}")

    # Read as text - each file becomes one row
    text_df = spark.read.text(path).withColumn("__input_file_path__", F.input_file_name())
    print(f"Files read: {text_df.count()} files")

    # Parse JSON objects using UDF
    exploded_df = text_df.select(
        F.explode(parse_json_udf(F.col("value"))).alias("json_line"),
        F.col("__input_file_path__")
    )

    print(f"After parsing: {exploded_df.count()} JSON records")

    # Infer schema from sample
    sample_df = spark.read.json(
        exploded_df.select("json_line").limit(1000).rdd.map(lambda r: r.json_line)
    )
    schema = sample_df.schema
    print(f"Inferred schema with {len(schema.fields)} fields")

    # Parse JSON and keep source file
    df = exploded_df.select(
        F.from_json(F.col("json_line"), schema).alias("data"),
        F.col("__input_file_path__")
    ).select(
        F.col("data.*"),
        F.col("__input_file_path__")
    )

    # Extract ingest_date from file path and convert to timestamp
    df = df.withColumn(
        "_fetched_at",
        F.to_timestamp(
            F.regexp_extract(F.col("__input_file_path__"), r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
        )
    ).drop("__input_file_path__")

    record_count = df.count()
    print(f"Read {record_count:,} events from Bronze")

    return df

# COMMAND ----------

bronze_events_df = read_bronze_events(INGEST_DATE)
display(bronze_events_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Events

# COMMAND ----------

def safe_col(df, col_name, default=None):
    """Return column if it exists, otherwise return lit(default)."""
    if col_name in df.columns:
        return F.col(col_name)
    return F.lit(default)


def transform_events(df):
    """Transform Bronze events to Silver schema."""
    # Helper for optional columns
    def opt(col_name, default=None):
        return safe_col(df, col_name, default)

    return df.select(
        # Primary key
        F.col("id").cast(StringType()).alias("id"),
        F.col("slug").cast(StringType()).alias("slug"),

        # Metadata
        F.col("_fetched_at"),

        # Core fields
        F.col("title").cast(StringType()).alias("title"),
        opt("description").cast(StringType()).alias("description"),
        F.col("image").cast(StringType()).alias("image"),
        F.col("icon").cast(StringType()).alias("icon"),
        opt("ticker").cast(StringType()).alias("ticker"),

        # Status flags (default to False if missing)
        F.coalesce(opt("active"), F.lit(False)).cast(BooleanType()).alias("active"),
        F.coalesce(opt("closed"), F.lit(False)).cast(BooleanType()).alias("closed"),
        F.coalesce(opt("archived"), F.lit(False)).cast(BooleanType()).alias("archived"),
        F.coalesce(opt("restricted"), F.lit(False)).cast(BooleanType()).alias("restricted"),
        F.coalesce(opt("featured"), F.lit(False)).cast(BooleanType()).alias("featured"),
        F.coalesce(opt("new"), F.lit(False)).cast(BooleanType()).alias("new"),

        # Timestamps (parse_timestamp handles multiple formats like "February 6, 2022")
        parse_timestamp(opt("createdAt")).alias("createdAt"),
        parse_timestamp(opt("updatedAt")).alias("updatedAt"),
        parse_timestamp(opt("startDate")).alias("startDate"),
        parse_timestamp(opt("endDate")).alias("endDate"),
        parse_timestamp(opt("creationDate")).alias("creationDate"),
        parse_timestamp(opt("closedTime")).alias("closedTime"),

        # Configuration
        F.coalesce(opt("cyom"), F.lit(False)).cast(BooleanType()).alias("cyom"),
        F.coalesce(opt("showAllOutcomes"), F.lit(False)).cast(BooleanType()).alias("showAllOutcomes"),
        F.coalesce(opt("showMarketImages"), F.lit(False)).cast(BooleanType()).alias("showMarketImages"),
        F.coalesce(opt("enableNegRisk"), F.lit(False)).cast(BooleanType()).alias("enableNegRisk"),
        F.coalesce(opt("negRiskAugmented"), F.lit(False)).cast(BooleanType()).alias("negRiskAugmented"),
        F.coalesce(opt("pendingDeployment"), F.lit(False)).cast(BooleanType()).alias("pendingDeployment"),
        F.coalesce(opt("deploying"), F.lit(False)).cast(BooleanType()).alias("deploying"),
        F.coalesce(opt("requiresTranslation"), F.lit(False)).cast(BooleanType()).alias("requiresTranslation"),
        opt("enableOrderBook").cast(BooleanType()).alias("enableOrderBook"),
        opt("automaticallyResolved").cast(BooleanType()).alias("automaticallyResolved"),
        opt("negRisk").cast(BooleanType()).alias("negRisk"),

        # Metrics
        opt("commentCount").cast(LongType()).alias("commentCount"),
        opt("openInterest").cast(LongType()).alias("openInterest"),

        # Resolution
        opt("resolutionSource").cast(StringType()).alias("resolutionSource"),

        # Force-included fields (may not exist in schema)
        opt("seriesSlug").cast(StringType()).alias("seriesSlug"),
        opt("category").cast(StringType()).alias("category"),
    )

# COMMAND ----------

silver_events_df = transform_events(bronze_events_df)
print(f"Transformed {silver_events_df.count():,} events")
display(silver_events_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Transform Markets

# COMMAND ----------

def extract_markets(df):
    """Extract markets from events and transform to Silver schema."""
    # Explode markets array and flatten the struct
    markets_flat = df.select(
        F.col("id").alias("event_id"),
        F.col("_fetched_at"),
        F.explode_outer("markets").alias("market")
    ).filter(F.col("market").isNotNull()).select(
        "event_id",
        "_fetched_at",
        F.col("market.*")  # Flatten market struct
    )

    # Helper for optional columns in flattened df
    def opt(col_name, default=None):
        return safe_col(markets_flat, col_name, default)

    # Transform to Silver schema
    return markets_flat.select(
        # Primary key
        F.col("id").cast(StringType()).alias("id"),
        opt("slug").cast(StringType()).alias("slug"),

        # Metadata
        F.col("event_id"),
        F.col("_fetched_at"),

        # Core fields
        opt("question").cast(StringType()).alias("question"),
        opt("description").cast(StringType()).alias("description"),
        opt("conditionId").cast(StringType()).alias("conditionId"),
        opt("outcomes").cast(StringType()).alias("outcomes"),
        opt("outcomePrices").cast(StringType()).alias("outcomePrices"),

        # Addresses
        opt("marketMakerAddress").cast(StringType()).alias("marketMakerAddress"),
        opt("questionID").cast(StringType()).alias("questionID"),
        opt("clobTokenIds").cast(StringType()).alias("clobTokenIds"),

        # Status flags
        F.coalesce(opt("active"), F.lit(False)).cast(BooleanType()).alias("active"),
        F.coalesce(opt("closed"), F.lit(False)).cast(BooleanType()).alias("closed"),
        F.coalesce(opt("archived"), F.lit(False)).cast(BooleanType()).alias("archived"),
        F.coalesce(opt("restricted"), F.lit(False)).cast(BooleanType()).alias("restricted"),
        F.coalesce(opt("ready"), F.lit(False)).cast(BooleanType()).alias("ready"),
        F.coalesce(opt("funded"), F.lit(False)).cast(BooleanType()).alias("funded"),
        F.coalesce(opt("approved"), F.lit(False)).cast(BooleanType()).alias("approved"),
        opt("featured").cast(BooleanType()).alias("featured"),
        F.coalesce(opt("new"), F.lit(False)).cast(BooleanType()).alias("new"),
        opt("acceptingOrders").cast(BooleanType()).alias("acceptingOrders"),

        # Timestamps (parse_timestamp handles multiple formats like "February 6, 2022")
        parse_timestamp(opt("createdAt")).alias("createdAt"),
        parse_timestamp(opt("updatedAt")).alias("updatedAt"),
        parse_timestamp(opt("startDate")).alias("startDate"),
        parse_timestamp(opt("endDate")).alias("endDate"),
        parse_timestamp(opt("closedTime")).alias("closedTime"),
        parse_timestamp(opt("acceptingOrdersTimestamp")).alias("acceptingOrdersTimestamp"),
        parse_timestamp(opt("umaEndDate")).alias("umaEndDate"),

        # ISO date strings
        opt("startDateIso").cast(StringType()).alias("startDateIso"),
        opt("endDateIso").cast(StringType()).alias("endDateIso"),

        # Images
        opt("image").cast(StringType()).alias("image"),
        opt("icon").cast(StringType()).alias("icon"),

        # Configuration
        F.coalesce(opt("cyom"), F.lit(False)).cast(BooleanType()).alias("cyom"),
        F.coalesce(opt("pagerDutyNotificationEnabled"), F.lit(False)).cast(BooleanType()).alias("pagerDutyNotificationEnabled"),
        F.coalesce(opt("clearBookOnStart"), F.lit(False)).cast(BooleanType()).alias("clearBookOnStart"),
        F.coalesce(opt("manualActivation"), F.lit(False)).cast(BooleanType()).alias("manualActivation"),
        F.coalesce(opt("negRiskOther"), F.lit(False)).cast(BooleanType()).alias("negRiskOther"),
        F.coalesce(opt("pendingDeployment"), F.lit(False)).cast(BooleanType()).alias("pendingDeployment"),
        F.coalesce(opt("deploying"), F.lit(False)).cast(BooleanType()).alias("deploying"),
        F.coalesce(opt("rfqEnabled"), F.lit(False)).cast(BooleanType()).alias("rfqEnabled"),
        F.coalesce(opt("holdingRewardsEnabled"), F.lit(False)).cast(BooleanType()).alias("holdingRewardsEnabled"),
        F.coalesce(opt("feesEnabled"), F.lit(False)).cast(BooleanType()).alias("feesEnabled"),
        F.coalesce(opt("requiresTranslation"), F.lit(False)).cast(BooleanType()).alias("requiresTranslation"),
        opt("enableOrderBook").cast(BooleanType()).alias("enableOrderBook"),
        opt("automaticallyActive").cast(BooleanType()).alias("automaticallyActive"),
        opt("negRisk").cast(BooleanType()).alias("negRisk"),
        opt("hasReviewedDates").cast(BooleanType()).alias("hasReviewedDates"),

        # Trading parameters
        opt("rewardsMinSize").cast(DoubleType()).alias("rewardsMinSize"),
        opt("rewardsMaxSpread").cast(DoubleType()).alias("rewardsMaxSpread"),
        opt("spread").cast(DoubleType()).alias("spread"),
        opt("bestAsk").cast(DoubleType()).alias("bestAsk"),
        opt("orderMinSize").cast(DoubleType()).alias("orderMinSize"),
        opt("orderPriceMinTickSize").cast(DoubleType()).alias("orderPriceMinTickSize"),

        # Volume & pricing
        opt("volume").cast(StringType()).alias("volume"),
        opt("volumeNum").cast(DoubleType()).alias("volumeNum"),
        opt("volumeClob").cast(DoubleType()).alias("volumeClob"),
        opt("lastTradePrice").cast(DoubleType()).alias("lastTradePrice"),
        opt("oneDayPriceChange").cast(DoubleType()).alias("oneDayPriceChange"),
        opt("competitive").cast(DoubleType()).alias("competitive"),

        # Resolution
        opt("umaResolutionStatuses").cast(StringType()).alias("umaResolutionStatuses"),
        opt("umaResolutionStatus").cast(StringType()).alias("umaResolutionStatus"),
        opt("resolutionSource").cast(StringType()).alias("resolutionSource"),
        opt("resolvedBy").cast(StringType()).alias("resolvedBy"),
        opt("submitted_by").cast(StringType()).alias("submitted_by"),
        opt("automaticallyResolved").cast(BooleanType()).alias("automaticallyResolved"),
    )

# COMMAND ----------

silver_markets_df = extract_markets(bronze_events_df)
print(f"Extracted {silver_markets_df.count():,} markets")
display(silver_markets_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Transform Series

# COMMAND ----------

def extract_series(df):
    """Extract series from events and deduplicate by slug."""
    # Check if series column exists
    if "series" not in df.columns:
        print("Warning: series column not in schema")
        schema = StructType([
            StructField("slug", StringType(), True),
            StructField("id", StringType(), True),
            StructField("_fetched_at", TimestampType(), True),
            StructField("ticker", StringType(), True),
            StructField("title", StringType(), True),
            StructField("active", BooleanType(), True),
            StructField("closed", BooleanType(), True),
            StructField("archived", BooleanType(), True),
            StructField("requiresTranslation", BooleanType(), True),
            StructField("createdAt", TimestampType(), True),
            StructField("updatedAt", TimestampType(), True),
            StructField("commentCount", LongType(), True),
            StructField("seriesType", StringType(), True),
            StructField("recurrence", StringType(), True),
        ])
        return spark.createDataFrame([], schema)

    # Explode series array and flatten
    series_flat = df.select(
        F.col("_fetched_at"),
        F.explode_outer("series").alias("series")
    ).filter(F.col("series").isNotNull()).select(
        "_fetched_at",
        F.col("series.*")
    )

    # Helper for optional columns
    def opt(col_name, default=None):
        return safe_col(series_flat, col_name, default)

    # Transform to Silver schema
    series_df = series_flat.select(
        # Primary key
        opt("slug").cast(StringType()).alias("slug"),
        opt("id").cast(StringType()).alias("id"),

        # Metadata
        F.col("_fetched_at"),

        # Core fields
        opt("ticker").cast(StringType()).alias("ticker"),
        opt("title").cast(StringType()).alias("title"),

        # Status flags
        F.coalesce(opt("active"), F.lit(False)).cast(BooleanType()).alias("active"),
        F.coalesce(opt("closed"), F.lit(False)).cast(BooleanType()).alias("closed"),
        F.coalesce(opt("archived"), F.lit(False)).cast(BooleanType()).alias("archived"),
        F.coalesce(opt("requiresTranslation"), F.lit(False)).cast(BooleanType()).alias("requiresTranslation"),

        # Timestamps
        parse_timestamp(opt("createdAt")).alias("createdAt"),
        parse_timestamp(opt("updatedAt")).alias("updatedAt"),

        # Metrics
        opt("commentCount").cast(LongType()).alias("commentCount"),

        # Configuration
        opt("seriesType").cast(StringType()).alias("seriesType"),
        opt("recurrence").cast(StringType()).alias("recurrence"),
    )

    # Deduplicate: keep the latest version of each series by slug
    window = Window.partitionBy("slug").orderBy(F.col("updatedAt").desc())
    deduped = series_df.withColumn("_row_num", F.row_number().over(window)) \
                       .filter(F.col("_row_num") == 1) \
                       .drop("_row_num")

    return deduped

# COMMAND ----------

silver_series_df = extract_series(bronze_events_df)
print(f"Extracted {silver_series_df.count():,} unique series")
display(silver_series_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Transform Tags

# COMMAND ----------

def extract_tags(df):
    """Extract tags from events and deduplicate by id."""
    # Check if tags column exists
    if "tags" not in df.columns:
        print("Warning: tags column not in schema")
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("slug", StringType(), True),
            StructField("label", StringType(), True),
            StructField("requiresTranslation", BooleanType(), True),
            StructField("createdAt", TimestampType(), True),
            StructField("updatedAt", TimestampType(), True),
        ])
        return spark.createDataFrame([], schema)

    # Explode tags array and flatten
    tags_flat = df.select(
        F.explode_outer("tags").alias("tag")
    ).filter(F.col("tag").isNotNull()).select(
        F.col("tag.*")
    )

    # Helper for optional columns
    def opt(col_name, default=None):
        return safe_col(tags_flat, col_name, default)

    # Transform to Silver schema
    tags_df = tags_flat.select(
        opt("id").cast(StringType()).alias("id"),
        opt("slug").cast(StringType()).alias("slug"),
        opt("label").cast(StringType()).alias("label"),
        F.coalesce(opt("requiresTranslation"), F.lit(False)).cast(BooleanType()).alias("requiresTranslation"),
        parse_timestamp(opt("createdAt")).alias("createdAt"),
        parse_timestamp(opt("updatedAt")).alias("updatedAt"),
    )

    # Deduplicate: keep the latest version of each tag by id
    window = Window.partitionBy("id").orderBy(F.col("updatedAt").desc())
    deduped = tags_df.withColumn("_row_num", F.row_number().over(window)) \
                     .filter(F.col("_row_num") == 1) \
                     .drop("_row_num")

    return deduped

# COMMAND ----------

silver_tags_df = extract_tags(bronze_events_df)
print(f"Extracted {silver_tags_df.count():,} unique tags")
display(silver_tags_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Event-Tags Junction Table

# COMMAND ----------

def extract_event_tags(df):
    """Extract event-tag relationships."""
    # Check if tags column exists
    if "tags" not in df.columns:
        print("Warning: tags column not in schema")
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("tag_id", StringType(), True),
        ])
        return spark.createDataFrame([], schema)

    return df.select(
        F.col("id").alias("event_id"),
        F.explode_outer("tags").alias("tag")
    ).filter(
        F.col("tag").isNotNull()
    ).select(
        F.col("event_id").cast(StringType()),
        F.col("tag.id").cast(StringType()).alias("tag_id")
    ).distinct()

# COMMAND ----------

silver_event_tags_df = extract_event_tags(bronze_events_df)
print(f"Extracted {silver_event_tags_df.count():,} event-tag relationships")
display(silver_event_tags_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Transform Event Creators

# COMMAND ----------

def extract_event_creators(df):
    """Extract event creators from events."""
    # Check if eventCreators column exists (it's rare - only ~127 records)
    if "eventCreators" not in df.columns:
        print("Warning: eventCreators column not in schema (rare field)")
        # Return empty dataframe with correct schema
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("event_id", StringType(), True),
            StructField("creatorName", StringType(), True),
            StructField("creatorHandle", StringType(), True),
            StructField("creatorUrl", StringType(), True),
            StructField("creatorImage", StringType(), True),
            StructField("createdAt", TimestampType(), True),
        ])
        return spark.createDataFrame([], schema)

    # Explode eventCreators array
    creators_exploded = df.select(
        F.col("id").alias("event_id"),
        F.explode_outer("eventCreators").alias("creator")
    ).filter(F.col("creator").isNotNull())

    # Transform to Silver schema
    return creators_exploded.select(
        F.col("creator.id").cast(StringType()).alias("id"),
        F.col("event_id"),
        F.col("creator.creatorName").cast(StringType()).alias("creatorName"),
        F.col("creator.creatorHandle").cast(StringType()).alias("creatorHandle"),
        F.col("creator.creatorUrl").cast(StringType()).alias("creatorUrl"),
        F.col("creator.creatorImage").cast(StringType()).alias("creatorImage"),
        parse_timestamp(F.col("creator.createdAt")).alias("createdAt"),
    )

# COMMAND ----------

silver_event_creators_df = extract_event_creators(bronze_events_df)
print(f"Extracted {silver_event_creators_df.count():,} event creators")
display(silver_event_creators_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver Tables

# COMMAND ----------

def write_silver_table(df, table_name, mode="overwrite"):
    """
    Write DataFrame to Silver Delta table.

    Args:
        df: DataFrame to write
        table_name: Name of the table (e.g., "events", "markets")
        mode: Write mode ("overwrite" or "append")
    """
    path = f"{SILVER_BASE_PATH}/{table_name}"
    print(f"Writing {df.count():,} records to {path}")

    df.write \
        .format("delta") \
        .mode(mode) \
        .option("overwriteSchema", "true") \
        .save(path)

    print(f"Successfully wrote {table_name} to Silver")

# COMMAND ----------

# Write all Silver tables
write_silver_table(silver_events_df, "events")
write_silver_table(silver_markets_df, "markets")
write_silver_table(silver_series_df, "series")
write_silver_table(silver_tags_df, "tags")
write_silver_table(silver_event_tags_df, "event_tags")
write_silver_table(silver_event_creators_df, "event_creators")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Output

# COMMAND ----------

# Verify tables were written correctly
for table in ["events", "markets", "series", "tags", "event_tags", "event_creators"]:
    path = f"{SILVER_BASE_PATH}/{table}"
    df = spark.read.format("delta").load(path)
    print(f"{table}: {df.count():,} records, {len(df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Tables in Unity Catalog (Optional)

# COMMAND ----------

# Uncomment to register tables in Unity Catalog
# CATALOG = "polypoly"
# SCHEMA = "silver"

# for table in ["events", "markets", "series", "tags", "event_tags", "event_creators"]:
#     path = f"{SILVER_BASE_PATH}/{table}"
#     spark.sql(f"""
#         CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{table}
#         USING DELTA
#         LOCATION '{path}'
#     """)
#     print(f"Registered {CATALOG}.{SCHEMA}.{table}")
