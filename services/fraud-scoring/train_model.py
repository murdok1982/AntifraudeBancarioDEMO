"""
Entrena un modelo XGBoost de detección de fraude sobre datos sintéticos.
Se ejecuta en tiempo de build del contenedor Docker.
"""
import os
import pickle
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, classification_report
import xgboost as xgb

SEED = 42
N_SAMPLES = 60_000
FRAUD_RATE = 0.08
MODEL_DIR = "model"

os.makedirs(MODEL_DIR, exist_ok=True)

np.random.seed(SEED)
n_fraud = int(N_SAMPLES * FRAUD_RATE)
n_legit = N_SAMPLES - n_fraud


def gen_legit(n):
    return pd.DataFrame({
        "amount":            np.random.lognormal(4.5, 1.2, n).clip(1, 49000),
        "hour":              np.random.choice(range(24), n, p=[
                                 0.01,0.01,0.01,0.01,0.01,0.02,0.04,0.06,
                                 0.07,0.07,0.07,0.07,0.07,0.07,0.07,0.06,
                                 0.06,0.06,0.05,0.05,0.04,0.03,0.02,0.01]),
        "day_of_week":       np.random.randint(0, 7, n),
        "is_weekend":        np.random.choice([0, 1], n, p=[0.71, 0.29]),
        "country_risk":      np.random.choice([0, 1, 2], n, p=[0.88, 0.09, 0.03]),
        "new_merchant":      np.random.choice([0, 1], n, p=[0.80, 0.20]),
        "txn_velocity_1h":   np.random.poisson(1.2, n).clip(0, 20),
        "amount_zscore":     np.random.normal(0, 0.8, n),
        "geo_distance_km":   np.random.exponential(50, n).clip(0, 300),
        "device_age_days":   np.random.exponential(180, n).clip(0, 1000),
        "merchant_mcc_risk": np.random.choice([0, 1, 2, 3], n, p=[0.60, 0.25, 0.10, 0.05]),
        "label": 0,
    })


def gen_fraud(n):
    patterns = np.random.choice(["velocity", "geo", "night", "new_merch", "country"], n)
    df = gen_legit(n)
    df["label"] = 1

    mask_vel = patterns == "velocity"
    df.loc[mask_vel, "txn_velocity_1h"] = np.random.randint(8, 25, mask_vel.sum())
    df.loc[mask_vel, "amount"] *= np.random.uniform(1.5, 4, mask_vel.sum())

    mask_geo = patterns == "geo"
    df.loc[mask_geo, "geo_distance_km"] = np.random.uniform(900, 5000, mask_geo.sum())
    df.loc[mask_geo, "amount_zscore"] = np.random.uniform(2.5, 6, mask_geo.sum())

    mask_night = patterns == "night"
    df.loc[mask_night, "hour"] = np.random.choice([1, 2, 3, 4, 5], mask_night.sum())
    df.loc[mask_night, "amount"] = np.random.uniform(500, 15000, mask_night.sum())

    mask_new = patterns == "new_merch"
    df.loc[mask_new, "new_merchant"] = 1
    df.loc[mask_new, "amount"] = np.random.uniform(1500, 30000, mask_new.sum())
    df.loc[mask_new, "merchant_mcc_risk"] = np.random.choice([2, 3], mask_new.sum())

    mask_country = patterns == "country"
    df.loc[mask_country, "country_risk"] = 2
    df.loc[mask_country, "amount_zscore"] = np.random.uniform(1.5, 5, mask_country.sum())

    return df


print("Generando datos sintéticos...")
df = pd.concat([gen_legit(n_legit), gen_fraud(n_fraud)], ignore_index=True).sample(frac=1, random_state=SEED)

FEATURES = [
    "amount", "hour", "day_of_week", "is_weekend",
    "country_risk", "new_merchant", "txn_velocity_1h",
    "amount_zscore", "geo_distance_km", "device_age_days",
    "merchant_mcc_risk",
]

X = df[FEATURES]
y = df["label"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=SEED, stratify=y)

scaler = StandardScaler()
X_train_s = scaler.fit_transform(X_train)
X_test_s  = scaler.transform(X_test)

print("Entrenando XGBoost...")
scale_pos_weight = (y_train == 0).sum() / (y_train == 1).sum()
model = xgb.XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=scale_pos_weight,
    use_label_encoder=False,
    eval_metric="auc",
    random_state=SEED,
    n_jobs=-1,
)
model.fit(X_train_s, y_train, eval_set=[(X_test_s, y_test)], verbose=False)

y_prob = model.predict_proba(X_test_s)[:, 1]
auc = roc_auc_score(y_test, y_prob)
print(f"AUC-ROC en test: {auc:.4f}")
print(classification_report(y_test, (y_prob > 0.5).astype(int), target_names=["Legit", "Fraud"]))

with open(f"{MODEL_DIR}/fraud_model.pkl", "wb") as f:
    pickle.dump(model, f)
with open(f"{MODEL_DIR}/scaler.pkl", "wb") as f:
    pickle.dump(scaler, f)
with open(f"{MODEL_DIR}/features.pkl", "wb") as f:
    pickle.dump(FEATURES, f)

print(f"Modelo guardado en {MODEL_DIR}/")
