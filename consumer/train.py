"""
train.py — Offline training script supporting both real and synthetic data.

Phase 2: Supports the Kaggle Credit Card Fraud Detection dataset
(mlg-ulb/creditcardfraud) which contains 284,807 real transactions
from European cardholders with confirmed fraud labels.

Dataset schema:
  Time    — seconds elapsed since first transaction
  V1-V28  — PCA-anonymised behavioral features (real patterns, confidential)
  Amount  — transaction amount in EUR
  Class   — 0 = legitimate, 1 = fraud (0.172% fraud rate)

Usage:
  # With real Kaggle data (Phase 2):
  docker compose run --rm consumer python train.py --mode kaggle --data /app/data/creditcard.csv

  # With synthetic data (Phase 1 fallback):
  docker compose run --rm consumer python train.py --mode synthetic
"""

import os
import csv
import math
import json
import logging
import argparse
import pickle
import sys
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
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("train")

DATA_DIR   = Path(os.getenv("DATA_DIR", "/app/data"))
MODEL_DIR  = Path(os.getenv("MODEL_PATH", "/app/models/xgb_fraud.json")).parent
MODEL_PATH = MODEL_DIR / "xgb_fraud.json"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

# ─── Kaggle dataset ───────────────────────────────────────────────────────────

KAGGLE_FEATURE_COLS = [f"V{i}" for i in range(1, 29)] + ["Amount", "Time"]
KAGGLE_LABEL_COL    = "Class"


def load_kaggle_data(csv_path: Path) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """
    Load the Kaggle creditcard.csv dataset.

    The dataset has 31 columns:
      Time, V1..V28, Amount, Class
    V1-V28 are PCA components of real transaction behavior — these ARE
    reliable fraud signals learned from confirmed chargebacks and investigations.
    """
    log.info("Loading Kaggle dataset from %s", csv_path)
    df = pd.read_csv(csv_path)

    required = set(KAGGLE_FEATURE_COLS + [KAGGLE_LABEL_COL])
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {missing}. "
                         f"Expected Kaggle creditcard.csv format.")

    log.info("Loaded %d records | fraud: %d (%.3f%%)",
             len(df),
             df[KAGGLE_LABEL_COL].sum(),
             df[KAGGLE_LABEL_COL].mean() * 100)

    # Amount and Time need scaling — V1-V28 are already scaled by PCA
    scaler = StandardScaler()
    df["Amount_scaled"] = scaler.fit_transform(df[["Amount"]])
    df["Time_scaled"]   = scaler.fit_transform(df[["Time"]])

    # Save scaler for runtime inference
    with open(MODEL_DIR / "kaggle_scaler.pkl", "wb") as f:
        pickle.dump(scaler, f)

    feature_cols = [f"V{i}" for i in range(1, 29)] + ["Amount_scaled", "Time_scaled"]
    X = df[feature_cols].values.astype(np.float32)
    y = df[KAGGLE_LABEL_COL].values.astype(int)

    return X, y, feature_cols


def engineer_kaggle_features(df: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    """Add derived features on top of the Kaggle PCA features."""
    # Hour of day from Time (seconds since first transaction, modulo 1 day)
    df["hour_of_day"] = (df["Time"] % 86400) // 3600

    # Amount z-score overall (no category available)
    df["amt_zscore"] = (df["Amount"] - df["Amount"].mean()) / df["Amount"].std()

    extra_cols   = ["hour_of_day", "amt_zscore"]
    feature_cols = [f"V{i}" for i in range(1, 29)] + ["Amount_scaled", "Time_scaled"] + extra_cols
    return df, feature_cols


# ─── Synthetic dataset (Phase 1 fallback) ────────────────────────────────────

SYNTHETIC_CATEGORY_LIST = [
    "grocery_pos", "gas_transport", "home", "shopping_net", "shopping_pos",
    "food_dining", "health_fitness", "entertainment", "travel",
    "personal_care", "kids_pets", "misc_net", "misc_pos",
]
SYNTHETIC_FEATURE_COLS = [
    "amt", "age", "distance", "hour_of_day",
    "day_of_week", "tx_velocity_1h", "amt_zscore",
]


def _compute_age(dob_str: str) -> int:
    try:
        dob   = datetime.strptime(dob_str, "%Y-%m-%d").date()
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    except Exception:
        return 35


def _euclidean_distance(r) -> float:
    return math.sqrt((r["cust_lat"] - r["merch_lat"])**2 +
                     (r["cust_long"] - r["merch_long"])**2)


def _one_hot_category(cat: str) -> list[float]:
    vec = [0.0] * len(SYNTHETIC_CATEGORY_LIST)
    if cat in SYNTHETIC_CATEGORY_LIST:
        vec[SYNTHETIC_CATEGORY_LIST.index(cat)] = 1.0
    return vec


def engineer_synthetic_features(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    df = df.copy()
    df["age"]         = df["dob"].apply(_compute_age)
    df["distance"]    = df.apply(_euclidean_distance, axis=1)
    df["trans_ts"]    = pd.to_datetime(df["trans_time"])
    df["hour_of_day"] = df["trans_ts"].dt.hour
    df["day_of_week"] = df["trans_ts"].dt.dayofweek

    cat_stats = df.groupby("category")["amt"].agg(["mean", "std"]).rename(
        columns={"mean": "cat_mean", "std": "cat_std"}
    )
    df = df.join(cat_stats, on="category")
    df["amt_zscore"]     = (df["amt"] - df["cat_mean"]) / df["cat_std"].replace(0, 1)
    df["tx_velocity_1h"] = df.groupby("cc_num").cumcount().clip(0, 10)

    cat_encoded = pd.DataFrame(
        df["category"].apply(_one_hot_category).tolist(),
        columns=[f"cat_{c}" for c in SYNTHETIC_CATEGORY_LIST],
        index=df.index,
    )
    df = pd.concat([df, cat_encoded], axis=1)

    feature_cols = SYNTHETIC_FEATURE_COLS + [f"cat_{c}" for c in SYNTHETIC_CATEGORY_LIST]
    return df, feature_cols


def load_synthetic_data(path: Path) -> tuple[np.ndarray, np.ndarray, list[str]]:
    if not path.exists():
        log.warning("Synthetic training data not found — generating...")
        _generate_synthetic_training_data(path)

    log.info("Loading synthetic training data from %s", path)
    df = pd.read_csv(path)
    log.info("Loaded %d records | fraud rate: %.2f%%",
             len(df), df["is_fraud"].mean() * 100)

    df, feature_cols = engineer_synthetic_features(df)
    X = df[feature_cols].fillna(0).values.astype(np.float32)
    y = df["is_fraud"].astype(int).values
    return X, y, feature_cols


# ─── K-Means undersampling (matches original repo technique) ──────────────────

def kmeans_undersample(X_majority: np.ndarray, target_n: int, k: int = 50) -> np.ndarray:
    """
    Reduce majority class using K-Means cluster centroids.
    Mirrors the original Spark ML job's balancing technique.
    """
    k_actual = min(k, len(X_majority), target_n)
    log.info("K-Means undersampling: %d → %d (k=%d)", len(X_majority), target_n, k_actual)

    km = MiniBatchKMeans(n_clusters=k_actual, random_state=42, n_init=3)
    km.fit(X_majority)

    sampled = []
    per_cluster = max(1, target_n // k_actual)
    for cid in range(k_actual):
        idx = np.where(km.labels_ == cid)[0]
        take = min(per_cluster, len(idx))
        sampled.extend(np.random.choice(idx, take, replace=False))

    import random
    random.shuffle(sampled)
    return np.array(sampled[:target_n])


# ─── Training ─────────────────────────────────────────────────────────────────

def train_and_evaluate(X: np.ndarray, y: np.ndarray,
                       feature_cols: list[str], mode: str):
    fraud_idx     = np.where(y == 1)[0]
    non_fraud_idx = np.where(y == 0)[0]

    log.info("Class distribution — fraud: %d | legitimate: %d",
             len(fraud_idx), len(non_fraud_idx))

    # Balance dataset using K-Means undersampling
    target_n      = len(fraud_idx)
    sampled_nf    = kmeans_undersample(X[non_fraud_idx], target_n)
    keep_idx      = np.concatenate([fraud_idx, non_fraud_idx[sampled_nf]])
    np.random.shuffle(keep_idx)
    X_bal, y_bal  = X[keep_idx], y[keep_idx]
    log.info("Balanced: %d samples | fraud=%.1f%%", len(y_bal), y_bal.mean() * 100)

    # XGBoost cross-validation to find optimal rounds
    dtrain = xgb.DMatrix(X_bal, label=y_bal, feature_names=feature_cols)
    params = {
        "objective":        "binary:logistic",
        "eval_metric":      ["auc", "aucpr"],
        "max_depth":        6,
        "learning_rate":    0.05,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "scale_pos_weight": 1,
        "tree_method":      "hist",
        "seed":             42,
    }

    log.info("Running 5-fold cross-validation...")
    cv = xgb.cv(
        params, dtrain,
        num_boost_round=500,
        nfold=5,
        stratified=True,
        early_stopping_rounds=30,
        verbose_eval=False,
    )
    best_round = int(cv["test-auc-mean"].idxmax()) + 1
    log.info("Best round: %d | CV AUC: %.4f ± %.4f",
             best_round,
             cv.loc[best_round - 1, "test-auc-mean"],
             cv.loc[best_round - 1, "test-auc-std"])

    # Train final model on full balanced set
    log.info("Training final XGBoost model (%d rounds)...", best_round)
    booster = xgb.train(params, dtrain, num_boost_round=best_round, verbose_eval=False)

    # Evaluate on held-out 20% of ORIGINAL (unbalanced) data for realistic metrics
    split    = int(len(X) * 0.8)
    X_test   = X[split:]
    y_test   = y[split:]
    dtest    = xgb.DMatrix(X_test, feature_names=feature_cols)
    y_prob   = booster.predict(dtest)
    y_pred   = (y_prob >= 0.5).astype(int)

    auc_roc  = roc_auc_score(y_test, y_prob)
    auc_pr   = average_precision_score(y_test, y_prob)
    log.info("Test AUC-ROC: %.4f | AUC-PR: %.4f", auc_roc, auc_pr)
    log.info("\n%s", classification_report(y_test, y_pred,
             target_names=["legitimate", "fraud"], zero_division=0))

    cm = confusion_matrix(y_test, y_pred)
    log.info("Confusion matrix:\n  TN=%-6d FP=%-6d\n  FN=%-6d TP=%-6d",
             cm[0][0], cm[0][1], cm[1][0], cm[1][1])

    # False positive rate (critical for production fraud systems)
    tn, fp, fn, tp = cm.ravel()
    fpr = fp / (fp + tn) if (fp + tn) > 0 else 0
    fnr = fn / (fn + tp) if (fn + tp) > 0 else 0
    log.info("False Positive Rate: %.4f%% (legitimate txns wrongly blocked)",  fpr * 100)
    log.info("False Negative Rate: %.4f%% (fraud txns wrongly allowed through)", fnr * 100)

    return booster


def save_model(booster, feature_cols: list[str], mode: str):
    booster.save_model(str(MODEL_PATH))
    log.info("Model saved → %s", MODEL_PATH)

    # Save SHAP explainer
    explainer = shap.TreeExplainer(booster)
    with open(MODEL_DIR / "shap_explainer.pkl", "wb") as f:
        pickle.dump(explainer, f)
    log.info("SHAP explainer saved")

    # Save metadata — tells consumer which features to use at runtime
    meta = {
        "mode":         mode,
        "feature_cols": feature_cols,
        "trained_at":   datetime.utcnow().isoformat(),
        "model_version": "xgb-v2-kaggle" if mode == "kaggle" else "xgb-v1-synthetic",
    }
    with open(MODEL_DIR / "model_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    log.info("Model metadata saved → %s", MODEL_DIR / "model_meta.json")

    # Feature importance
    importance  = booster.get_score(importance_type="gain")
    sorted_imp  = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    log.info("Top 10 features by gain:")
    for feat, score in sorted_imp[:10]:
        log.info("  %-25s %.2f", feat, score)


# ─── Synthetic data generator (fallback) ──────────────────────────────────────

def _generate_synthetic_training_data(path: Path):
    from generator import generate_customers, generate_transaction
    import random as _r

    path.parent.mkdir(parents=True, exist_ok=True)
    customers  = generate_customers(100)
    fieldnames = ["cc_num", "dob", "cust_lat", "cust_long", "trans_time",
                  "category", "merchant", "amt", "merch_lat", "merch_long", "is_fraud"]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(12000):
            cust = _r.choice(customers)
            tx   = generate_transaction(cust)
            writer.writerow({
                "cc_num": tx.cc_num, "dob": tx.dob,
                "cust_lat": tx.cust_lat, "cust_long": tx.cust_long,
                "trans_time": tx.trans_time, "category": tx.category,
                "merchant": tx.merchant, "amt": tx.amt,
                "merch_lat": tx.merch_lat, "merch_long": tx.merch_long,
                "is_fraud": int(tx.is_fraud),
            })
    log.info("Generated 12000 synthetic records → %s", path)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Train XGBoost fraud detection model")
    parser.add_argument("--mode", choices=["kaggle", "synthetic"], default="synthetic",
                        help="Data source: 'kaggle' for real data, 'synthetic' for generated data")
    parser.add_argument("--data", type=str, default=None,
                        help="Path to dataset CSV (required for --mode kaggle)")
    args = parser.parse_args()

    if args.mode == "kaggle":
        # ── Phase 2: Real Kaggle data ──────────────────────────────────────
        if args.data is None:
            log.error("--data path is required for kaggle mode")
            log.error("Usage: python train.py --mode kaggle --data /app/data/creditcard.csv")
            sys.exit(1)

        csv_path = Path(args.data)
        if not csv_path.exists():
            log.error("Dataset not found at %s", csv_path)
            log.error("Download it from: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud")
            log.error("Then place creditcard.csv in your data/ folder")
            sys.exit(1)

        log.info("=" * 60)
        log.info("PHASE 2: Training on real Kaggle fraud dataset")
        log.info("=" * 60)
        df          = pd.read_csv(csv_path)
        df, feat    = engineer_kaggle_features(df)

        # Scale Amount and Time
        from sklearn.preprocessing import StandardScaler
        scaler = StandardScaler()
        df["Amount_scaled"] = scaler.fit_transform(df[["Amount"]])
        df["Time_scaled"]   = scaler.fit_transform(df[["Time"]])
        with open(MODEL_DIR / "kaggle_scaler.pkl", "wb") as f:
            pickle.dump(scaler, f)

        feature_cols = [f"V{i}" for i in range(1, 29)] + \
                       ["Amount_scaled", "Time_scaled", "hour_of_day", "amt_zscore"]
        X = df[feature_cols].values.astype(np.float32)
        y = df["Class"].values.astype(int)

    else:
        # ── Phase 1: Synthetic fallback ────────────────────────────────────
        log.info("=" * 60)
        log.info("PHASE 1: Training on synthetic data (fallback)")
        log.info("=" * 60)
        X, y, feature_cols = load_synthetic_data(DATA_DIR / "transactions_training.csv")

    booster = train_and_evaluate(X, y, feature_cols, args.mode)
    save_model(booster, feature_cols, args.mode)
    log.info("Training complete!")


if __name__ == "__main__":
    main()