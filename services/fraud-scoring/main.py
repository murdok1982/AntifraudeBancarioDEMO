"""
Fraud Scoring Service — API principal de detección de fraude en tiempo real.
"""
import json
import math
import os
import pickle
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from threading import Lock
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras
import redis
import shap
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Histogram, make_asgi_app
from pydantic import BaseModel, Field

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST    = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT    = int(os.getenv("REDIS_PORT", 6379))
PG_HOST       = os.getenv("POSTGRES_HOST", "localhost")
PG_DB         = os.getenv("POSTGRES_DB", "frauddb")
PG_USER       = os.getenv("POSTGRES_USER", "fraud")
PG_PASS       = os.getenv("POSTGRES_PASSWORD", "fraud123")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
MODEL_VERSION = os.getenv("MODEL_VERSION", "1.0.0")
MODEL_DIR     = "model"

# ── Prometheus metrics ────────────────────────────────────────────────────────
requests_counter = Counter(
    "fraud_scoring_requests_total",
    "Total scoring requests",
    ["decision"],
)
latency_hist = Histogram(
    "fraud_scoring_latency_seconds",
    "Scoring latency",
    buckets=[0.005, 0.010, 0.020, 0.030, 0.050, 0.075, 0.100, 0.200, 0.500],
)
score_hist = Histogram(
    "fraud_score_distribution",
    "Distribution of fraud scores",
    buckets=[i / 10 for i in range(11)],
)
rules_counter = Counter(
    "rules_triggered_total",
    "Rules triggered count",
    ["rule"],
)

# ── In-memory stats buffer ────────────────────────────────────────────────────
recent_decisions: deque = deque(maxlen=1000)
stats_lock = Lock()

# ── Model ─────────────────────────────────────────────────────────────────────
with open(f"{MODEL_DIR}/fraud_model.pkl", "rb") as f:
    MODEL = pickle.load(f)
with open(f"{MODEL_DIR}/scaler.pkl", "rb") as f:
    SCALER = pickle.load(f)
with open(f"{MODEL_DIR}/features.pkl", "rb") as f:
    FEATURES: list[str] = pickle.load(f)

# SHAP explainer
EXPLAINER = shap.TreeExplainer(MODEL)

# ── Connections (lazy with retry) ─────────────────────────────────────────────
_redis_client: redis.Redis | None = None
_kafka_producer: KafkaProducer | None = None


def get_redis() -> redis.Redis | None:
    global _redis_client
    if _redis_client is None:
        try:
            _redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT,
                                        decode_responses=True, socket_timeout=1)
            _redis_client.ping()
        except Exception as e:
            print(f"[WARN] Redis no disponible: {e}")
            _redis_client = None
    return _redis_client


def get_kafka() -> KafkaProducer | None:
    global _kafka_producer
    if _kafka_producer is None:
        try:
            _kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode(),
                acks="all",
                retries=3,
                request_timeout_ms=5000,
            )
        except Exception as e:
            print(f"[WARN] Kafka no disponible: {e}")
            _kafka_producer = None
    return _kafka_producer


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS,
        connect_timeout=5,
    )


# ── Rules Engine ─────────────────────────────────────────────────────────────
SANCTIONED_COUNTRIES = {"IR", "KP", "SY", "CU", "MM", "BY"}
HIGH_RISK_MCC = {"7995", "5933", "5912"}

ROUND_AMOUNTS = {500, 1000, 2000, 5000, 10000, 20000, 50000}


def run_rules(txn: dict, fs: dict) -> tuple[float, list[str]]:
    """
    Devuelve (score_contribution, reglas_disparadas).
    Score contribution se suma al score del modelo (capped a 1.0).
    """
    score = 0.0
    triggered = []

    dest = txn.get("country_destination", "").upper()
    if dest in SANCTIONED_COUNTRIES:
        triggered.append("P0_SANCTIONED_COUNTRY")
        return 1.0, triggered

    if txn.get("amount", 0) > 50_000:
        triggered.append("P0_AMOUNT_LIMIT")
        return 0.97, triggered

    velocity = int(fs.get("txn_velocity_1h", 0))
    if velocity > 10:
        triggered.append("P1_HIGH_VELOCITY")
        score += 0.40

    last_lat = fs.get("last_lat")
    last_lon = fs.get("last_lon")
    last_ts  = fs.get("last_txn_ts")
    if last_lat and last_lon and last_ts and txn.get("latitude") and txn.get("longitude"):
        try:
            dist = _haversine(float(last_lat), float(last_lon),
                              txn["latitude"], txn["longitude"])
            elapsed_min = (time.time() - float(last_ts)) / 60
            if dist > 800 and elapsed_min < 120:
                triggered.append("P1_IMPOSSIBLE_TRAVEL")
                score += 0.50
        except Exception:
            pass

    mcc = txn.get("merchant_mcc", "")
    if txn.get("new_merchant_flag", False) and txn.get("amount", 0) > 2000:
        triggered.append("P2_NEW_MERCHANT_HIGH_AMOUNT")
        score += 0.25
    if mcc in HIGH_RISK_MCC and txn.get("amount", 0) > 500:
        triggered.append("P2_HIGH_RISK_MCC")
        score += 0.20

    hour = datetime.now(timezone.utc).hour
    try:
        hour = datetime.fromisoformat(txn.get("timestamp") or
                                      datetime.now(timezone.utc).isoformat()).hour
    except Exception:
        pass
    if hour in {1, 2, 3, 4, 5} and txn.get("amount", 0) > 500:
        triggered.append("P2_NIGHT_TRANSACTION")
        score += 0.15

    if txn.get("amount", 0) in ROUND_AMOUNTS:
        triggered.append("P3_ROUND_AMOUNT")
        score += 0.05

    return min(score, 1.0), triggered


def _haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── Feature Store helpers ─────────────────────────────────────────────────────
def get_features_from_store(account_id: str, txn: dict) -> dict:
    r = get_redis()
    if r is None:
        return _default_features(txn)
    try:
        prefix = f"fs:{account_id}"
        pipe = r.pipeline()
        pipe.get(f"{prefix}:txn_velocity_1h")
        pipe.get(f"{prefix}:last_lat")
        pipe.get(f"{prefix}:last_lon")
        pipe.get(f"{prefix}:last_txn_ts")
        pipe.get(f"{prefix}:avg_amount_30d")
        values = pipe.execute()
        return {
            "txn_velocity_1h": int(values[0] or 0),
            "last_lat":        values[1],
            "last_lon":        values[2],
            "last_txn_ts":     values[3],
            "avg_amount_30d":  float(values[4] or txn.get("amount", 100)),
        }
    except Exception:
        return _default_features(txn)


def _default_features(txn: dict) -> dict:
    return {
        "txn_velocity_1h": 1,
        "last_lat": None, "last_lon": None, "last_txn_ts": None,
        "avg_amount_30d": txn.get("amount", 100),
    }


def update_feature_store(account_id: str, txn: dict):
    r = get_redis()
    if r is None:
        return
    try:
        prefix = f"fs:{account_id}"
        pipe = r.pipeline()
        pipe.incr(f"{prefix}:txn_velocity_1h")
        pipe.expire(f"{prefix}:txn_velocity_1h", 3600)
        if txn.get("latitude"):
            pipe.set(f"{prefix}:last_lat", txn["latitude"])
            pipe.set(f"{prefix}:last_lon", txn["longitude"])
        pipe.set(f"{prefix}:last_txn_ts", time.time())
        pipe.execute()
    except Exception as e:
        print(f"[WARN] Feature store update failed: {e}")


# ── ML Scoring ────────────────────────────────────────────────────────────────
def build_feature_vector(txn: dict, fs: dict) -> np.ndarray:
    now = datetime.now(timezone.utc)
    try:
        ts = datetime.fromisoformat(txn.get("timestamp") or now.isoformat())
    except Exception:
        ts = now

    avg_amt = float(fs.get("avg_amount_30d") or txn["amount"])
    zscore  = (txn["amount"] - avg_amt) / max(avg_amt * 0.3, 1)

    country_risk_map = {
        "ES": 0, "FR": 0, "DE": 0, "GB": 0, "IT": 0, "PT": 0,
        "US": 0, "JP": 0, "AU": 0, "CA": 0,
        "BR": 1, "MX": 1, "CO": 1, "ZA": 1, "TR": 1, "NG": 1,
        "RU": 2, "CN": 2, "IR": 2, "KP": 2, "SY": 2, "VE": 2,
    }
    country_risk = country_risk_map.get(
        txn.get("country_destination", "").upper(), 1
    )

    mcc_risk_map = {"7995": 3, "5933": 2, "6011": 1, "5411": 0, "7011": 0}
    mcc_risk = mcc_risk_map.get(txn.get("merchant_mcc", ""), 0)

    vec = {
        "amount":            txn["amount"],
        "hour":              ts.hour,
        "day_of_week":       ts.weekday(),
        "is_weekend":        int(ts.weekday() >= 5),
        "country_risk":      country_risk,
        "new_merchant":      int(txn.get("new_merchant_flag", False)),
        "txn_velocity_1h":   min(int(fs.get("txn_velocity_1h", 1)), 25),
        "amount_zscore":     min(max(zscore, -5), 10),
        "geo_distance_km":   0.0,
        "device_age_days":   float(txn.get("device_age_days", 30)),
        "merchant_mcc_risk": mcc_risk,
    }
    return np.array([[vec[f] for f in FEATURES]])


def score_transaction(feature_vec: np.ndarray) -> tuple[float, list[dict]]:
    scaled = SCALER.transform(feature_vec)
    prob   = float(MODEL.predict_proba(scaled)[0][1])

    shap_vals = EXPLAINER.shap_values(scaled)[0]
    feature_importance = sorted(
        zip(FEATURES, shap_vals.tolist()),
        key=lambda x: abs(x[1]),
        reverse=True,
    )
    top_factors = [
        {"factor": f, "contribution": round(v, 4)}
        for f, v in feature_importance[:3]
    ]
    return prob, top_factors


# ── Decision ─────────────────────────────────────────────────────────────────
def make_decision(score: float) -> tuple[str, str]:
    if score >= 0.70:
        return "BLOCK", "CRITICAL" if score >= 0.90 else "HIGH"
    if score >= 0.35:
        return "REVIEW", "MEDIUM"
    return "APPROVE", "LOW"


# ── Persistence ───────────────────────────────────────────────────────────────
def persist_decision(rec: dict):
    try:
        conn = get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO fraud_decisions
                        (transaction_id, account_id, amount, currency, channel,
                         merchant_id, country_origin, country_destination,
                         score, decision, risk_level, rules_triggered,
                         shap_values, top_risk_factors, processing_time_ms, model_version)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    rec["transaction_id"], rec["account_id"], rec["amount"],
                    rec.get("currency", "EUR"), rec.get("channel"),
                    rec.get("merchant_id"), rec.get("country_origin"),
                    rec.get("country_destination"),
                    rec["score"], rec["decision"], rec["risk_level"],
                    json.dumps(rec["rules_triggered"]),
                    json.dumps(rec.get("shap_values", {})),
                    json.dumps(rec["top_risk_factors"]),
                    rec["processing_time_ms"], MODEL_VERSION,
                ))
        conn.close()
    except Exception as e:
        print(f"[WARN] DB persist failed: {e}")


# ── Kafka publish ─────────────────────────────────────────────────────────────
def publish_event(topic: str, payload: dict):
    producer = get_kafka()
    if producer is None:
        return
    try:
        producer.send(topic, value=payload)
    except KafkaError as e:
        print(f"[WARN] Kafka publish failed: {e}")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Fraud Scoring API",
    description="Real-time fraud detection engine",
    version=MODEL_VERSION,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/metrics", make_asgi_app())


# ── Schemas ───────────────────────────────────────────────────────────────────
class TransactionRequest(BaseModel):
    transaction_id:    str   = Field(default_factory=lambda: str(uuid.uuid4()))
    account_id:        str
    amount:            float
    currency:          str   = "EUR"
    merchant_id:       str   = "UNKNOWN"
    merchant_mcc:      str   = "5999"
    channel:           str   = "WEB"
    country_origin:    str   = "ES"
    country_destination: str = "ES"
    device_id:         str | None = None
    device_age_days:   float = 30.0
    new_merchant_flag: bool  = False
    latitude:          float | None = None
    longitude:         float | None = None
    timestamp:         str | None   = None


class ScoringResponse(BaseModel):
    transaction_id:     str
    score:              float
    decision:           str
    risk_level:         str
    rules_triggered:    list[str]
    top_risk_factors:   list[dict]
    processing_time_ms: float
    model_version:      str
    timestamp:          str


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.post("/v2/transactions/score", response_model=ScoringResponse)
def score(req: TransactionRequest):
    t0 = time.perf_counter()

    txn = req.model_dump()
    fs  = get_features_from_store(req.account_id, txn)

    rule_score, rules_triggered = run_rules(txn, fs)
    for rule in rules_triggered:
        rules_counter.labels(rule=rule).inc()

    if rule_score >= 0.95:
        score_val = rule_score
        top_factors = [{"factor": rules_triggered[0], "contribution": rule_score}]
    else:
        fvec = build_feature_vector(txn, fs)
        ml_score, top_factors = score_transaction(fvec)
        score_val = min(ml_score + rule_score * 0.4, 1.0)

    decision, risk_level = make_decision(score_val)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    # metrics
    requests_counter.labels(decision=decision).inc()
    latency_hist.observe(elapsed_ms / 1000)
    score_hist.observe(score_val)

    # update feature store
    update_feature_store(req.account_id, txn)

    result = {
        "transaction_id":     req.transaction_id,
        "account_id":         req.account_id,
        "amount":             req.amount,
        "currency":           req.currency,
        "channel":            req.channel,
        "merchant_id":        req.merchant_id,
        "country_origin":     req.country_origin,
        "country_destination": req.country_destination,
        "score":              round(score_val, 4),
        "decision":           decision,
        "risk_level":         risk_level,
        "rules_triggered":    rules_triggered,
        "top_risk_factors":   top_factors,
        "processing_time_ms": round(elapsed_ms, 3),
        "model_version":      MODEL_VERSION,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
    }

    # async persistence & events
    persist_decision(result)
    publish_event("txn.scored", result)

    with stats_lock:
        recent_decisions.append({
            "transaction_id": req.transaction_id,
            "account_id":     req.account_id,
            "amount":         req.amount,
            "channel":        req.channel,
            "score":          round(score_val, 4),
            "decision":       decision,
            "risk_level":     risk_level,
            "rules_triggered": rules_triggered,
            "top_risk_factors": top_factors,
            "processing_time_ms": round(elapsed_ms, 3),
            "timestamp":      result["timestamp"],
        })

    return ScoringResponse(**{k: result[k] for k in ScoringResponse.model_fields})


@app.get("/v2/stats")
def stats():
    with stats_lock:
        decisions = list(recent_decisions)
    total = len(decisions)
    if total == 0:
        return {"total": 0, "approve": 0, "review": 0, "block": 0,
                "avg_score": 0, "avg_latency_ms": 0, "recent": []}

    approve = sum(1 for d in decisions if d["decision"] == "APPROVE")
    review  = sum(1 for d in decisions if d["decision"] == "REVIEW")
    block   = sum(1 for d in decisions if d["decision"] == "BLOCK")
    avg_sc  = round(sum(d["score"] for d in decisions) / total, 4)
    avg_lat = round(sum(d["processing_time_ms"] for d in decisions) / total, 2)

    rules_count: dict[str, int] = {}
    for d in decisions:
        for r in d.get("rules_triggered", []):
            rules_count[r] = rules_count.get(r, 0) + 1

    return {
        "total":          total,
        "approve":        approve,
        "review":         review,
        "block":          block,
        "block_rate_pct": round(block / total * 100, 2),
        "avg_score":      avg_sc,
        "avg_latency_ms": avg_lat,
        "rules_count":    rules_count,
        "recent":         list(reversed(decisions))[:20],
    }


@app.get("/v2/models/current")
def model_info():
    return {
        "model_version":  MODEL_VERSION,
        "model_type":     "XGBoost",
        "features":       FEATURES,
        "decision_thresholds": {"APPROVE": "< 0.35", "REVIEW": "0.35–0.70", "BLOCK": "> 0.70"},
    }


@app.get("/v2/health")
def health():
    status = {"model": "ok", "redis": "unknown", "postgres": "unknown", "kafka": "unknown"}

    r = get_redis()
    try:
        r.ping()
        status["redis"] = "ok"
    except Exception:
        status["redis"] = "unavailable"

    try:
        conn = get_pg_conn()
        conn.close()
        status["postgres"] = "ok"
    except Exception:
        status["postgres"] = "unavailable"

    producer = get_kafka()
    status["kafka"] = "ok" if producer else "unavailable"

    overall = "healthy" if all(v == "ok" for v in status.values()) else "degraded"
    return {"status": overall, "checks": status, "model_version": MODEL_VERSION}
