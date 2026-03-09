"""
Distributed Financial Data Mesh — PySpark Processing Layer
Runs on Google Cloud Dataproc.

Transforms raw OHLCV data into ML-ready features:
  - Technical indicators: SMA, EMA, Bollinger Bands, RSI, MACD
  - Volatility metrics: rolling std, ATR, log-returns
  - Volume analytics: OBV, volume z-score
  - Cross-sectional features: momentum, golden/death cross signals

Output: partitioned Parquet in GCS + BigQuery native tables.
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("pyspark_transform")

RAW_SCHEMA = StructType([
    StructField("date",        StringType(),  True),
    StructField("ticker",      StringType(),  True),
    StructField("open",        DoubleType(),  True),
    StructField("high",        DoubleType(),  True),
    StructField("low",         DoubleType(),  True),
    StructField("close",       DoubleType(),  True),
    StructField("volume",      LongType(),    True),
    StructField("adj_open",    DoubleType(),  True),
    StructField("adj_high",    DoubleType(),  True),
    StructField("adj_low",     DoubleType(),  True),
    StructField("adj_close",   DoubleType(),  True),
    StructField("adj_volume",  LongType(),    True),
    StructField("ingested_at", StringType(),  True),
])


def build_spark_session(app_name: str = "FinancialDataMesh") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0",
        )
        .getOrCreate()
    )


def tw(days: int):
    """Per-ticker rolling window of N days."""
    return (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-days + 1, 0)
    )


def to_ordered():
    """Per-ticker ordered window (unbounded lag)."""
    return Window.partitionBy("ticker").orderBy("date")


def add_moving_averages(df):
    logger.info("Computing moving averages...")
    for n in [5, 10, 20, 50, 200]:
        df = df.withColumn(f"sma_{n}", F.avg("adj_close").over(tw(n)))
    for n in [12, 26]:
        df = df.withColumn(f"ema_{n}", F.avg("adj_close").over(tw(n)))
    return df


def add_bollinger_bands(df):
    logger.info("Computing Bollinger Bands...")
    return (
        df
        .withColumn("bb_mid",   F.avg("adj_close").over(tw(20)))
        .withColumn("bb_std",   F.stddev("adj_close").over(tw(20)))
        .withColumn("bb_upper", F.col("bb_mid") + 2 * F.col("bb_std"))
        .withColumn("bb_lower", F.col("bb_mid") - 2 * F.col("bb_std"))
        .withColumn(
            "bb_pct_b",
            (F.col("adj_close") - F.col("bb_lower")) /
            (F.col("bb_upper") - F.col("bb_lower") + 1e-10),
        )
        .withColumn(
            "bb_bandwidth",
            (F.col("bb_upper") - F.col("bb_lower")) / (F.col("bb_mid") + 1e-10),
        )
    )


def add_rsi(df, period: int = 14):
    logger.info("Computing RSI...")
    wo = to_ordered()
    df = df.withColumn("prev_close", F.lag("adj_close", 1).over(wo))
    df = (
        df
        .withColumn("price_delta", F.col("adj_close") - F.col("prev_close"))
        .withColumn("gain", F.when(F.col("price_delta") > 0, F.col("price_delta")).otherwise(0.0))
        .withColumn("loss", F.when(F.col("price_delta") < 0, -F.col("price_delta")).otherwise(0.0))
        .withColumn("avg_gain", F.avg("gain").over(tw(period)))
        .withColumn("avg_loss", F.avg("loss").over(tw(period)))
        .withColumn(
            "rsi",
            F.when(F.col("avg_loss") == 0, 100.0)
             .otherwise(100.0 - (100.0 / (1.0 + F.col("avg_gain") / (F.col("avg_loss") + 1e-10)))),
        )
        .drop("price_delta", "gain", "loss", "avg_gain", "avg_loss", "prev_close")
    )
    return df


def add_macd(df):
    logger.info("Computing MACD...")
    return (
        df
        .withColumn("macd_line",   F.col("ema_12") - F.col("ema_26"))
        .withColumn("macd_signal", F.avg("macd_line").over(tw(9)))
        .withColumn("macd_hist",   F.col("macd_line") - F.col("macd_signal"))
    )


def add_volatility(df):
    logger.info("Computing volatility features...")
    wo = to_ordered()
    df = df.withColumn("prev_close", F.lag("adj_close", 1).over(wo))
    df = df.withColumn(
        "log_return",
        F.log(F.col("adj_close") / (F.col("prev_close") + 1e-10)),
    )
    for n in [5, 10, 20, 60]:
        df = (
            df
            .withColumn(f"vol_{n}d",             F.stddev("log_return").over(tw(n)))
            .withColumn(f"annualized_vol_{n}d",   F.col(f"vol_{n}d") * F.lit(252 ** 0.5))
        )
    df = (
        df
        .withColumn("tr", F.greatest(
            F.col("high") - F.col("low"),
            F.abs(F.col("high") - F.col("prev_close")),
            F.abs(F.col("low")  - F.col("prev_close")),
        ))
        .withColumn("atr_14", F.avg("tr").over(tw(14)))
        .drop("tr", "prev_close")
    )
    return df


def add_volume_features(df):
    logger.info("Computing volume features...")
    wo = to_ordered()
    df = df.withColumn("prev_close", F.lag("adj_close", 1).over(wo))
    df = (
        df
        .withColumn(
            "obv_delta",
            F.when(F.col("adj_close") > F.col("prev_close"),  F.col("adj_volume"))
             .when(F.col("adj_close") < F.col("prev_close"), -F.col("adj_volume"))
             .otherwise(0),
        )
        .withColumn("obv", F.sum("obv_delta").over(
            Window.partitionBy("ticker").orderBy("date")
            .rowsBetween(Window.unboundedPreceding, 0)
        ))
        .withColumn("vol_sma_20", F.avg("adj_volume").over(tw(20)))
        .withColumn("vol_std_20", F.stddev("adj_volume").over(tw(20)))
        .withColumn(
            "vol_zscore",
            (F.col("adj_volume") - F.col("vol_sma_20")) / (F.col("vol_std_20") + 1e-10),
        )
        .drop("obv_delta", "prev_close")
    )
    return df


def add_price_ratios(df):
    logger.info("Computing price ratios and signals...")
    wo = to_ordered()
    df = (
        df
        .withColumn("_52w_high", F.max("adj_close").over(tw(252)))
        .withColumn("_52w_low",  F.min("adj_close").over(tw(252)))
        .withColumn(
            "pct_from_52w_high",
            (F.col("adj_close") - F.col("_52w_high")) / (F.col("_52w_high") + 1e-10),
        )
        .withColumn(
            "pct_from_52w_low",
            (F.col("adj_close") - F.col("_52w_low")) / (F.col("_52w_low") + 1e-10),
        )
        .withColumn(
            "golden_cross",
            F.when(F.col("sma_50") > F.col("sma_200"), 1).otherwise(0),
        )
        .withColumn(
            "momentum_20d",
            (F.col("adj_close") - F.lag("adj_close", 20).over(wo))
            / (F.lag("adj_close", 20).over(wo) + 1e-10),
        )
        .withColumn(
            "momentum_60d",
            (F.col("adj_close") - F.lag("adj_close", 60).over(wo))
            / (F.lag("adj_close", 60).over(wo) + 1e-10),
        )
        .drop("_52w_high", "_52w_low")
    )
    return df


def add_metadata(df, run_date: str):
    return (
        df
        .withColumn("processed_at", F.lit(datetime.utcnow().isoformat()))
        .withColumn("pipeline_run",  F.lit(run_date))
        .withColumn("date_ts",       F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("year",          F.year("date_ts"))
        .withColumn("month",         F.month("date_ts"))
    )


def run(args):
    spark    = build_spark_session()
    run_date = args.run_date or datetime.utcnow().strftime("%Y%m%d")

    input_path  = f"gs://{args.bucket}/raw/stocks/{run_date}/*.json"
    output_path = f"gs://{args.bucket}/processed/stocks/{run_date}"

    logger.info(f"Reading raw data from: {input_path}")
    df = spark.read.schema(RAW_SCHEMA).json(input_path)
    logger.info(f"Loaded {df.count()} rows")

    df = add_moving_averages(df)
    df = add_bollinger_bands(df)
    df = add_rsi(df)
    df = add_macd(df)
    df = add_volatility(df)
    df = add_volume_features(df)
    df = add_price_ratios(df)
    df = add_metadata(df, run_date)

    logger.info(f"Writing Parquet to: {output_path}")
    (
        df.repartition("year", "month", "ticker")
        .write.mode("overwrite")
        .partitionBy("year", "month", "ticker")
        .parquet(output_path)
    )

    bq_table = f"{args.project}.{args.dataset}.stock_features"
    logger.info(f"Writing to BigQuery: {bq_table}")
    (
        df.write
        .format("bigquery")
        .option("table",              bq_table)
        .option("temporaryGcsBucket", args.bucket)
        .option("partitionField",     "date_ts")
        .option("clusteredFields",    "ticker")
        .mode("overwrite")
        .save()
    )

    logger.info("PySpark job finished successfully.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Financial Data Mesh — PySpark Transform")
    parser.add_argument("--bucket",   required=True)
    parser.add_argument("--project",  required=True)
    parser.add_argument("--dataset",  required=True)
    parser.add_argument("--run_date", default=None)
    run(parser.parse_args())
