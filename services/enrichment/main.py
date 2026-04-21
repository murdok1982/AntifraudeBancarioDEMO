"""
Enrichment Service — consume txn.raw, enriquece y publica en txn.enriched.
"""
import hashlib
import json
import os
import threading
import time
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, make_asgi_app

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")

enriched_counter = Counter("enrichment_processed_total", "Messages enriched")
error_counter    = Counter("enrichment_errors_total",    "Enrichment errors")

MCC_RISK = {
    "7995": "HIGH",   # gambling
    "6051": "HIGH",   # crypto/money orders
    "5933": "HIGH",   # pawn shops
    "6012": "HIGH",   # financial institutions
    "6011": "MEDIUM", # ATM
    "7299": "MEDIUM", # misc services
    "5999": "MEDIUM", # misc retail
    "5411": "LOW",    # grocery
    "7011": "LOW",    # hotel
    "5541": "LOW",    # gas station
    "4112": "LOW",    # rail
    "5621": "LOW",    # clothing
    "5712": "LOW",    # furniture
    "5815": "LOW",    # digital goods
    "4899": "LOW",    # telecom
}

COUNTRY_RISK = {
    "ES": "LOW",  "FR": "LOW",  "DE": "LOW",  "GB": "LOW",
    "IT": "LOW",  "PT": "LOW",  "NL": "LOW",  "BE": "LOW",
    "US": "LOW",  "JP": "LOW",  "AU": "LOW",  "CA": "LOW",
    "BR": "MEDIUM", "MX": "MEDIUM", "CO": "MEDIUM",
    "NG": "MEDIUM", "TR": "MEDIUM", "TH": "MEDIUM", "ZA": "MEDIUM",
    "RU": "HIGH", "CN": "HIGH", "IR": "HIGH",
    "KP": "HIGH", "SY": "HIGH", "VE": "HIGH", "CU": "HIGH",
}


def is_pep(account_id: str) -> bool:
    return int(hashlib.md5(account_id.encode()).hexdigest(), 16) % 100 < 2


def account_age_days(account_id: str) -> int:
    seed = int(hashlib.md5(account_id.encode()).hexdigest()[:6], 16)
    return (seed % 3000) + 30


def kyc_level(account_id: str) -> str:
    seed = int(hashlib.md5((account_id + "kyc").encode()).hexdigest()[:4], 16)
    v = seed % 100
    if v < 20:
        return "BASIC"
    if v < 70:
        return "STANDARD"
    return "ENHANCED"


def enrich(txn: dict) -> dict:
    mcc  = txn.get("merchant_mcc", "5999")
    dest = txn.get("country_destination", "ES").upper()
    acc  = txn.get("account_id", "")

    txn["merchant_risk_level"]  = MCC_RISK.get(mcc, "LOW")
    txn["country_risk_level"]   = COUNTRY_RISK.get(dest, "MEDIUM")
    txn["account_age_days"]     = account_age_days(acc)
    txn["kyc_level"]            = kyc_level(acc)
    txn["is_pep"]               = is_pep(acc)
    txn["enriched_at"]          = datetime.now(timezone.utc).isoformat()
    return txn


def consumer_loop():
    while True:
        try:
            consumer = KafkaConsumer(
                "txn.raw",
                bootstrap_servers=KAFKA_BROKERS.split(","),
                group_id="enrichment-service",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode()),
                consumer_timeout_ms=1000,
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode(),
                acks=1,
            )
            print("[Enrichment] Kafka conectado. Consumiendo txn.raw...")
            count = 0
            for msg in consumer:
                try:
                    enriched = enrich(msg.value)
                    producer.send("txn.enriched", value=enriched)
                    enriched_counter.inc()
                    count += 1
                    if count % 100 == 0:
                        print(f"[Enrichment] {count} mensajes procesados")
                except Exception as e:
                    error_counter.inc()
                    print(f"[WARN] Error enriqueciendo: {e}")
        except NoBrokersAvailable:
            print("[Enrichment] Kafka no disponible, reintentando en 10s...")
            time.sleep(10)
        except Exception as e:
            print(f"[Enrichment] Error: {e}. Reintentando en 10s...")
            time.sleep(10)


app = FastAPI(title="Enrichment Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])
app.mount("/metrics", make_asgi_app())


@app.on_event("startup")
def startup():
    t = threading.Thread(target=consumer_loop, daemon=True)
    t.start()


@app.get("/health")
def health():
    return {"status": "ok", "service": "enrichment"}
