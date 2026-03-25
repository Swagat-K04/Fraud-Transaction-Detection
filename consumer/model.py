"""
model.py — XGBoost fraud detection model with SHAP explainability.

Supports both modes:
  Phase 1 (synthetic): 7 engineered features + category one-hot
  Phase 2 (kaggle):    V1-V28 PCA features + Amount + Time + derived features

The active mode is determined by model_meta.json saved during training.
"""

import os
import json
import pickle
import logging
import numpy as np
import xgboost as xgb
import shap
from pathlib import Path
from typing import Optional

log = logging.getLogger("model")

MODEL_DIR  = Path(os.getenv("MODEL_PATH", "/app/models/xgb_fraud.json")).parent
MODEL_PATH = MODEL_DIR / "xgb_fraud.json"
META_PATH  = MODEL_DIR / "model_meta.json"

# Synthetic mode feature list (Phase 1)
SYNTHETIC_CATEGORY_LIST = [
    "grocery_pos", "gas_transport", "home", "shopping_net", "shopping_pos",
    "food_dining", "health_fitness", "entertainment", "travel",
    "personal_care", "kids_pets", "misc_net", "misc_pos",
]
SYNTHETIC_FEATURE_COLS = (
    ["amt", "age", "distance", "hour_of_day", "day_of_week",
     "tx_velocity_1h", "amt_zscore"]
    + [f"cat_{c}" for c in SYNTHETIC_CATEGORY_LIST]
)

RISK_THRESHOLDS = {
    "LOW":      (0.00, 0.30),
    "MEDIUM":   (0.30, 0.55),
    "HIGH":     (0.55, 0.75),
    "CRITICAL": (0.75, 1.01),
}


def _risk_level(score: float) -> str:
    for level, (lo, hi) in RISK_THRESHOLDS.items():
        if lo <= score < hi:
            return level
    return "CRITICAL"


def _one_hot_category(cat: str) -> list[float]:
    vec = [0.0] * len(SYNTHETIC_CATEGORY_LIST)
    if cat in SYNTHETIC_CATEGORY_LIST:
        vec[SYNTHETIC_CATEGORY_LIST.index(cat)] = 1.0
    return vec


class FraudModel:
    def __init__(self):
        self._booster:       Optional[xgb.Booster] = None
        self._explainer:     Optional[shap.TreeExplainer] = None
        self._feature_names: list[str] = []
        self._mode:          str = "synthetic"

    def load(self) -> bool:
        if not MODEL_PATH.exists():
            log.warning("Model not found at %s — run train.py first", MODEL_PATH)
            return False

        self._booster = xgb.Booster()
        self._booster.load_model(str(MODEL_PATH))

        # Load metadata to know which feature set to use
        if META_PATH.exists():
            with open(META_PATH) as f:
                meta = json.load(f)
            self._mode          = meta.get("mode", "synthetic")
            self._feature_names = meta.get("feature_cols", SYNTHETIC_FEATURE_COLS)
            log.info("Model mode: %s | features: %d | version: %s",
                     self._mode, len(self._feature_names),
                     meta.get("model_version", "unknown"))
        else:
            self._mode          = "synthetic"
            self._feature_names = SYNTHETIC_FEATURE_COLS
            log.info("No metadata found — assuming synthetic mode")

        self._explainer = shap.TreeExplainer(self._booster)
        log.info("Loaded XGBoost model from %s", MODEL_PATH)
        return True

    def predict(self, features: dict, threshold: float = 0.5) -> dict:
        """
        Run prediction with a configurable decision threshold.

        threshold: float between 0.1-0.9
          Lower  → catch more fraud, more false positives
          Higher → fewer false positives, miss more fraud
          Default 0.5 is neutral; tune based on business needs
        """
        if self._booster is None:
            raise RuntimeError("Model not loaded")

        X = self._build_feature_vector(features)
        dmat = xgb.DMatrix(X, feature_names=self._feature_names)
        score = float(self._booster.predict(dmat)[0])

        # SHAP attribution
        shap_values = self._explainer.shap_values(X)[0]
        top_idx = np.argsort(np.abs(shap_values))[::-1][:3]
        top_features = [
            {
                "feature":   self._feature_names[i],
                "value":     float(X[0][i]),
                "shap":      float(shap_values[i]),
                "direction": "increases" if shap_values[i] > 0 else "decreases",
            }
            for i in top_idx
        ]

        return {
            "fraud_score":  round(score, 4),
            "is_fraud":     score >= threshold,          # ← threshold applied here
            "risk_level":   _risk_level(score),          # risk level stays score-based
            "threshold":    round(threshold, 2),
            "top_features": top_features,
        }

    def _build_feature_vector(self, features: dict) -> np.ndarray:
        if self._mode == "kaggle":
            return self._build_kaggle_vector(features)
        else:
            return self._build_synthetic_vector(features)

    def _build_synthetic_vector(self, features: dict) -> np.ndarray:
        base = [features.get(c, 0.0) for c in [
            "amt", "age", "distance", "hour_of_day",
            "day_of_week", "tx_velocity_1h", "amt_zscore"
        ]]
        base += _one_hot_category(features.get("category", "misc_net"))
        return np.array([base], dtype=np.float32)

    def _build_kaggle_vector(self, features: dict) -> np.ndarray:
        """
        At runtime the producer sends synthetic transactions (no V1-V28).
        We approximate the Kaggle features from available fields so the
        pipeline keeps running — but note: for true production use you'd
        need real V1-V28 values from your payment processor.
        """
        row = []
        for col in self._feature_names:
            if col.startswith("V"):
                # V1-V28: use amt_zscore as a proxy for anomaly signal,
                # rest default to 0 (neutral in PCA space)
                if col == "V1":
                    row.append(features.get("amt_zscore", 0.0))
                elif col == "V3":
                    row.append(-features.get("distance", 0.0))  # distance as negative V3 proxy
                else:
                    row.append(0.0)
            elif col == "Amount_scaled":
                row.append(features.get("amt_zscore", 0.0))
            elif col == "Time_scaled":
                row.append(features.get("hour_of_day", 12.0) / 24.0)
            elif col == "hour_of_day":
                row.append(features.get("hour_of_day", 12.0))
            elif col == "amt_zscore":
                row.append(features.get("amt_zscore", 0.0))
            else:
                row.append(features.get(col, 0.0))
        return np.array([row], dtype=np.float32)

    def is_loaded(self) -> bool:
        return self._booster is not None

    @property
    def mode(self) -> str:
        return self._mode


_model_instance: Optional[FraudModel] = None

def get_model() -> FraudModel:
    global _model_instance
    if _model_instance is None:
        _model_instance = FraudModel()
        _model_instance.load()
    return _model_instance