from __future__ import annotations
import logging
import os
import gdown
import numpy as np
import pandas as pd
import xgboost as xgb

# Настройка логгера
logger = logging.getLogger(__name__)

model_th = 0.98

class XGBScorer:
    def __init__(self):
        self.model = self._load_model()

    def _load_model(self):
        # Загружаем модель с Google Drive
        MODEL_URL = "https://drive.google.com/uc?id=1fQ7hQUijNBnCQEptuREDvPOTPHXyWRsa"
        MODEL_PATH = "/app/models/model_xgb.json"
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        if not os.path.exists(MODEL_PATH):
            gdown.download(MODEL_URL, MODEL_PATH, quiet=False)
        model = xgb.XGBClassifier()
        model.load_model(MODEL_PATH)
        logger.info("XGBoost model loaded from %s", MODEL_PATH)
        return model

    def make_pred(self, X):
        proba = self.model.predict_proba(X)
        return (proba[:, 1] > model_th) * 1, proba[:, 1]


    def top5_feature_importances(self):
        try:
            booster = self.model.get_booster()
            scores = booster.get_score(importance_type='gain')
            # Заменяем на имена признаков
            names = getattr(self.model, "feature_names_in_", None)
            out = {}
            for k, v in scores.items():
                if k.startswith('f') and k[1:].isdigit() and names is not None:
                    idx = int(k[1:])
                    if 0 <= idx < len(names):
                        out[names[idx]] = float(v)
                    else:
                        out[k] = float(v)
                else:
                    out[k] = float(v)
            # топ-5 по фичей
            top = dict(sorted(out.items(), key=lambda kv: abs(kv[1]), reverse=True)[:5])
            return top
        except Exception as e:
            logger.warning("Failed to compute XGB importances: %s", e)
            return {}
