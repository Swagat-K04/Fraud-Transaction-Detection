"""
train.py — Offline training script for the XGBoost fraud model.

Mirrors the original Spark ML job logic:
  1. Load training transactions from CSV
  2. Engineer features (age, distance, velocity proxy, temporal, amount z-score)
  3. Balance classes via K-Means undersampling of non-fraud (original repo technique)
  4. Train XGBoost (replaces Random Forest) with cross-validation
  5. Evaluate with AUC-ROC, precision, recall, F1
  6. Save model + SHAP explainer
"""

import os
import csv
import math
import json
import logging
import random
import pickle
from datetime import datetime, date
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
import shap
from sklearn.cluster import MiniBatchKMeans
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    classification_report, confusion_matrix
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("train")

DATA_DIR   = Path(os.getenv("DATA_DIR", "/app/data"))
MODEL_DIR  = Path(os.getenv("MODEL_PATH", "/app/models/xgb_fraud.json")).parent
MODEL_PATH = MODEL_DIR / "xgb_fraud.json"

MODEL_DIR.mkdir(parents=True, exist_ok=True)

CATEGORY_LIST = [
    "grocery_pos", "gas_transport", "home", "shopping_net", "shopping_pos",
    "food_dining", "health_fitness", "entertainment", "travel",
    "personal_care", "kids_pets", "misc_net", "misc_pos",
]
FEATURE_COLS = [
    "amt", "age", "distance", "hour_of_day",
    "day_of_week", "tx_velocity_1h", "amt_zscore",
]


# ─── Feature engineering (pandas, mirrors features.py) ───────────────────────

def compute_age(dob_str: str) -> int:
    try:
        dob = datetime.strptime(dob_str, "%Y-%m-%d").date()
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except Exception:
        return 35

def euclidean_distance(r) -> float:
    return math.sqrt((r["cust_lat"] - r["merch_lat"])**2 + (r["cust_long"] - r["merch_long"])**2)

def one_hot_category(cat: str) -> list[float]:
    vec = [0.0] * len(CATEGORY_LIST)
    if cat in CATEGORY_LIST:
        vec[CATEGORY_LIST.index(cat)] = 1.0
    return vec


def engineer(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["age"]         = df["dob"].apply(compute_age)
    df["distance"]    = df.apply(euclidean_distance, axis=1)
    df["trans_ts"]    = pd.to_datetime(df["trans_time"])
    df["hour_of_day"] = df["trans_ts"].dt.hour
    df["day_of_week"] = df["trans_ts"].dt.dayofweek

    # Amount z-score per category
    cat_stats = df.groupby("category")["amt"].agg(["mean", "std"]).rename(
        columns={"mean": "cat_mean", "std": "cat_std"}
    )
    df = df.join(cat_stats, on="category")
    df["amt_zscore"] = (df["amt"] - df["cat_mean"]) / df["cat_std"].replace(0, 1)

    # Velocity proxy: random 1-5 for training (in production, Redis tracks real velocity)
    df["tx_velocity_1h"] = df.groupby("cc_num").cumcount().clip(0, 10)

    # One-hot encode category
    cat_encoded = pd.DataFrame(
        df["category"].apply(one_hot_category).tolist(),
        columns=[f"cat_{c}" for c in CATEGORY_LIST],
        index=df.index,
    )
    df = pd.concat([df, cat_encoded], axis=1)
    return df


def get_feature_matrix(df: pd.DataFrame):
    cols = FEATURE_COLS + [f"cat_{c}" for c in CATEGORY_LIST]
    return df[cols].fillna(0).values.astype(np.float32)


# ─── K-Means undersampling (matches original repo technique) ──────────────────

def kmeans_undersample(X_non_fraud: np.ndarray, target_n: int, k: int = 50) -> np.ndarray:
    """
    Original repo used K-Means to reduce the majority (non-fraud) class.
    We use cluster centroids as representative non-fraud samples.
    """
    log.info("K-Means undersampling: %d → %d non-fraud samples (k=%d)",
             len(X_non_fraud), target_n, k)
    km = MiniBatchKMeans(n_clusters=min(k, len(X_non_fraud)), random_state=42, n_init=3)
    km.fit(X_non_fraud)
    labels = km.labels_

    sampled_idx = []
    per_cluster = max(1, target_n // k)
    for cluster_id in range(km.n_clusters):
        idx_in_cluster = np.where(labels == cluster_id)[0]
        take = min(per_cluster, len(idx_in_cluster))
        sampled_idx.extend(np.random.choice(idx_in_cluster, take, replace=False))

    # Trim or pad to exact target
    random.shuffle(sampled_idx)
    return np.array(sampled_idx[:target_n])


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    # 1. Load data
    train_path = DATA_DIR / "transactions_training.csv"
    if not train_path.exists():
        log.error("Training data not found at %s", train_path)
        log.info("Generating synthetic training data...")
        _generate_synthetic_training_data(train_path)

    log.info("Loading training data from %s", train_path)
    df = pd.read_csv(train_path)
    log.info("Loaded %d records | fraud rate: %.2f%%",
             len(df), df["is_fraud"].mean() * 100)

    # 2. Feature engineering
    log.info("Engineering features...")
    df = engineer(df)
    X = get_feature_matrix(df)
    y = df["is_fraud"].astype(int).values

    # 3. Balance via K-Means undersampling (original repo technique)
    fraud_idx     = np.where(y == 1)[0]
    non_fraud_idx = np.where(y == 0)[0]

    sampled_non_fraud = kmeans_undersample(
        X[non_fraud_idx],
        target_n=len(fraud_idx),       # 1:1 ratio
        k=min(50, len(non_fraud_idx))
    )
    keep_idx = np.concatenate([fraud_idx, non_fraud_idx[sampled_non_fraud]])
    np.random.shuffle(keep_idx)
    X_bal, y_bal = X[keep_idx], y[keep_idx]
    log.info("Balanced dataset: %d samples (fraud=%.1f%%)",
             len(y_bal), y_bal.mean() * 100)

    # 4. Train XGBoost with cross-validation
    feature_names = FEATURE_COLS + [f"cat_{c}" for c in CATEGORY_LIST]
    dtrain = xgb.DMatrix(X_bal, label=y_bal, feature_names=feature_names)

    params = {
        "objective":        "binary:logistic",
        "eval_metric":      ["auc", "aucpr"],
        "max_depth":        6,
        "learning_rate":    0.05,
        "n_estimators":     300,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "scale_pos_weight": 1,   # already balanced
        "tree_method":      "hist",
        "seed":             42,
    }

    log.info("Training XGBoost (300 rounds)...")
    cv_result = xgb.cv(
        params, dtrain,
        num_boost_round=300,
        nfold=5,
        stratified=True,
        early_stopping_rounds=20,
        verbose_eval=False,
    )
    best_round = int(cv_result["test-auc-mean"].idxmax()) + 1
    log.info("CV best round: %d | AUC: %.4f ± %.4f",
             best_round,
             cv_result.loc[best_round-1, "test-auc-mean"],
             cv_result.loc[best_round-1, "test-auc-std"])

    booster = xgb.train(
        params, dtrain,
        num_boost_round=best_round,
        verbose_eval=False,
    )

    # 5. Evaluate on held-out set (20% of original data)
    test_path = DATA_DIR / "transactions_testing.csv"
    if test_path.exists():
        df_test = pd.read_csv(test_path)
        df_test = engineer(df_test)
        X_test  = get_feature_matrix(df_test)
        y_test  = df_test["is_fraud"].astype(int).values
        dtest   = xgb.DMatrix(X_test, feature_names=feature_names)
        y_prob  = booster.predict(dtest)
        y_pred  = (y_prob >= 0.5).astype(int)

        auc  = roc_auc_score(y_test, y_prob)
        pr   = average_precision_score(y_test, y_prob)
        log.info("Test AUC-ROC: %.4f | AUC-PR: %.4f", auc, pr)
        log.info("\n%s", classification_report(y_test, y_pred, target_names=["legit", "fraud"]))

    # 6. Save model + SHAP explainer
    booster.save_model(str(MODEL_PATH))
    log.info("Model saved → %s", MODEL_PATH)

    # Save SHAP TreeExplainer
    explainer = shap.TreeExplainer(booster)
    with open(MODEL_DIR / "shap_explainer.pkl", "wb") as f:
        pickle.dump(explainer, f)
    log.info("SHAP explainer saved")

    # Save feature importance
    importance = booster.get_score(importance_type="gain")
    sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    log.info("Top 10 features by gain:")
    for feat, score in sorted_imp[:10]:
        log.info("  %-25s %.2f", feat, score)


def _generate_synthetic_training_data(path: Path):
    """Generate synthetic training CSV when no real dataset is available."""
    from generator import generate_customers, generate_transaction
    import csv as csv_mod

    path.parent.mkdir(parents=True, exist_ok=True)
    customers = generate_customers(100)

    fieldnames = ["cc_num","dob","cust_lat","cust_long","trans_time","category",
                  "merchant","amt","merch_lat","merch_long","is_fraud"]
    with open(path, "w", newline="") as f:
        writer = csv_mod.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(12000):
            import random as _r
            cust = _r.choice(customers)
            tx = generate_transaction(cust)
            writer.writerow({
                "cc_num":     tx.cc_num,
                "dob":        tx.dob,
                "cust_lat":   tx.cust_lat,
                "cust_long":  tx.cust_long,
                "trans_time": tx.trans_time,
                "category":   tx.category,
                "merchant":   tx.merchant,
                "amt":        tx.amt,
                "merch_lat":  tx.merch_lat,
                "merch_long": tx.merch_long,
                "is_fraud":   int(tx.is_fraud),
            })
    log.info("Generated %d synthetic training records → %s", 12000, path)


if __name__ == "__main__":
    main()