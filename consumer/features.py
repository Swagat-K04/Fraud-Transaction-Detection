"""
features.py — Feature engineering pipeline for fraud detection.

Original repo computed: age, distance (Euclidean lat/long).
This module adds: hour_of_day, day_of_week, tx_velocity_1h, amt_zscore,
                  category encoding, and normalisation.

PySpark ML imports are lazy (inside functions) so this module can be imported
in the streaming consumer without requiring a running SparkContext.
"""

import math
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# ─── UDF helpers ─────────────────────────────────────────────────────────────

@F.udf(returnType=DoubleType())
def euclidean_distance(cust_lat, cust_long, merch_lat, merch_long):
    if any(v is None for v in [cust_lat, cust_long, merch_lat, merch_long]):
        return 0.0
    return math.sqrt((cust_lat - merch_lat) ** 2 + (cust_long - merch_long) ** 2)


@F.udf(returnType=IntegerType())
def compute_age(dob_str: str) -> int:
    if not dob_str:
        return 0
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except Exception:
        return 0


# ─── Feature engineering (Spark DataFrame) ───────────────────────────────────

CATEGORY_COLS = ["category"]
NUMERIC_FEATURES = [
    "amt", "age", "distance", "hour_of_day",
    "day_of_week", "tx_velocity_1h", "amt_zscore",
]
LABEL_COL = "is_fraud"


def engineer_features(df):
    """Add all derived feature columns to a transaction DataFrame."""
    df = df.withColumn("trans_ts", F.to_timestamp("trans_time"))
    df = df.withColumn("age", compute_age(F.col("dob")))
    df = df.withColumn("distance", euclidean_distance(
        F.col("cust_lat"), F.col("cust_long"),
        F.col("merch_lat"), F.col("merch_long")
    ))
    df = df.withColumn("hour_of_day",  F.hour("trans_ts").cast(IntegerType()))
    df = df.withColumn("day_of_week",  F.dayofweek("trans_ts").cast(IntegerType()))
    df = df.withColumn("tx_velocity_1h", F.lit(1).cast(IntegerType()))

    stats = df.groupBy("category").agg(
        F.mean("amt").alias("_cat_mean"),
        F.stddev("amt").alias("_cat_std"),
    )
    df = df.join(stats, on="category", how="left")
    df = df.withColumn(
        "amt_zscore",
        F.when(F.col("_cat_std") > 0,
               (F.col("amt") - F.col("_cat_mean")) / F.col("_cat_std"))
        .otherwise(F.lit(0.0)).cast(DoubleType())
    )
    df = df.drop("_cat_mean", "_cat_std")
    return df


def build_ml_pipeline(feature_cols=None):
    """Build PySpark ML Pipeline — imports are lazy so no SparkContext needed at import time."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler

    if feature_cols is None:
        feature_cols = NUMERIC_FEATURES

    cat_indexer = StringIndexer(inputCol="category", outputCol="category_idx", handleInvalid="keep")
    cat_encoder  = OneHotEncoder(inputCols=["category_idx"], outputCols=["category_vec"])
    assembler    = VectorAssembler(inputCols=feature_cols + ["category_vec"], outputCol="features_raw", handleInvalid="keep")
    scaler       = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    return Pipeline(stages=[cat_indexer, cat_encoder, assembler, scaler])


# ─── Single-row feature dict (used in streaming consumer) ────────────────────

def _age_from_dob(dob_str: str) -> int:
    if not dob_str:
        return 35
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except Exception:
        return 35


def compute_features_dict(tx: dict, velocity: int, cat_mean: float, cat_std: float) -> dict:
    """
    Compute features on a plain Python dict — used by the streaming consumer
    for low-latency single-row inference without PySpark overhead.
    """
    age = _age_from_dob(tx.get("dob", ""))

    clat  = float(tx.get("cust_lat",  0))
    clong = float(tx.get("cust_long", 0))
    mlat  = float(tx.get("merch_lat", 0))
    mlong = float(tx.get("merch_long", 0))
    distance = math.sqrt((clat - mlat) ** 2 + (clong - mlong) ** 2)

    ts = datetime.fromisoformat(tx.get("trans_time", datetime.utcnow().isoformat()))
    hour_of_day = ts.hour
    day_of_week = ts.weekday()

    amt = float(tx.get("amt", 0))
    amt_zscore = (amt - cat_mean) / cat_std if cat_std > 0 else 0.0

    return {
        "amt":            amt,
        "age":            age,
        "distance":       distance,
        "hour_of_day":    hour_of_day,
        "day_of_week":    day_of_week,
        "tx_velocity_1h": velocity,
        "amt_zscore":     amt_zscore,
        "category":       tx.get("category", "misc_net"),
    }