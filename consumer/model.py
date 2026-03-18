"""
model.py — XGBoost fraud detection model with SHAP explainability.

Replaces original Spark MLlib Random Forest with XGBoost, which:
  • Trains 3-5x faster
  • Achieves higher AUC on tabular fraud data
  • Supports SHAP values for per-prediction feature attribution
  • Exports to portable JSON (no JVM dependency)
"""

import os
import json
import logging
import numpy as np
import pandas as pd
import xgboost as xgb
import shap
from pathlib import Path
from typing import Optional

log = logging.getLogger("model")

MODEL_PATH  = Path(os.getenv("MODEL_PATH", "/app/models/xgb_fraud.json"))
SHAP_PATH   = Path(os.getenv("MODEL_PATH", "/app/models/xgb_fraud.json")).parent / "shap_explainer.pkl"

# Feature order must match training
FEATURE_COLS = [
    "amt", "age", "distance", "hour_of_day",
    "day_of_week", "tx_velocity_1h", "amt_zscore",
]

CATEGORY_LIST = [
    "grocery_pos", "gas_transport", "home", "shopping_net", "shopping_pos",
    "food_dining", "health_fitness", "entertainment", "travel",
    "personal_care", "kids_pets", "misc_net", "misc_pos",
]

RISK_THRESHOLDS = {
    "LOW":      (0.00, 0.30),
    "MEDIUM":   (0.30, 0.55),
    "HIGH":     (0.55, 0.75),
    "CRITICAL": (0.75, 1.01),
}


def _encode_category(cat: str) -> list[float]:
    """One-hot encode category to match training pipeline."""
    vec = [0.0] * len(CATEGORY_LIST)
    if cat in CATEGORY_LIST:
        vec[CATEGORY_LIST.index(cat)] = 1.0
    return vec


def _features_to_array(features: dict) -> np.ndarray:
    row = [features.get(c, 0.0) for c in FEATURE_COLS]
    row += _encode_category(features.get("category", "misc_net"))
    return np.array([row], dtype=np.float32)


def _risk_level(score: float) -> str:
    for level, (lo, hi) in RISK_THRESHOLDS.items():
        if lo <= score < hi:
            return level
    return "CRITICAL"


class FraudModel:
    """Wrapper around XGBoost booster with SHAP attribution."""

    def __init__(self):
        self._booster: Optional[xgb.Booster] = None
        self._explainer: Optional[shap.TreeExplainer] = None
        self._feature_names = FEATURE_COLS + [f"cat_{c}" for c in CATEGORY_LIST]

    def load(self) -> bool:
        if not MODEL_PATH.exists():
            log.warning("Model file not found at %s — run train.py first", MODEL_PATH)
            return False
        self._booster = xgb.Booster()
        self._booster.load_model(str(MODEL_PATH))
        self._explainer = shap.TreeExplainer(self._booster)
        log.info("Loaded XGBoost model from %s", MODEL_PATH)
        return True

    def predict(self, features: dict) -> dict:
        """
        Returns:
            {
              "fraud_score": float,   # probability [0, 1]
              "is_fraud": bool,
              "risk_level": str,
              "top_features": list[dict]  # SHAP top-3 explanations
            }
        """
        if self._booster is None:
            raise RuntimeError("Model not loaded — call load() first")

        X = _features_to_array(features)
        dmat = xgb.DMatrix(X, feature_names=self._feature_names)
        score = float(self._booster.predict(dmat)[0])

        # SHAP attribution
        shap_values = self._explainer.shap_values(X)[0]
        top_idx = np.argsort(np.abs(shap_values))[::-1][:3]
        top_features = [
            {
                "feature": self._feature_names[i],
                "value": float(X[0][i]),
                "shap": float(shap_values[i]),
                "direction": "increases" if shap_values[i] > 0 else "decreases",
            }
            for i in top_idx
        ]

        return {
            "fraud_score": round(score, 4),
            "is_fraud": score >= 0.5,
            "risk_level": _risk_level(score),
            "top_features": top_features,
        }

    def is_loaded(self) -> bool:
        return self._booster is not None


# ─── Singleton ────────────────────────────────────────────────────────────────
_model_instance: Optional[FraudModel] = None

def get_model() -> FraudModel:
    global _model_instance
    if _model_instance is None:
        _model_instance = FraudModel()
        _model_instance.load()
    return _model_instance
