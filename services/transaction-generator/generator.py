"""
Transaction Generator — produce transacciones sintéticas realistas
y las envía al Scoring API + Kafka.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker("es_ES")

SCORING_URL   = os.getenv("SCORING_API_URL", "http://fraud-scoring:8001")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
TPS           = float(os.getenv("TPS", "2"))
FRAUD_RATE    = float(os.getenv("FRAUD_RATE", "0.10"))
SLEEP         = 1.0 / TPS

# ── Static pools ──────────────────────────────────────────────────────────────
ACCOUNTS = [f"ACC{str(i).zfill(6)}" for i in range(1, 101)]
DEVICES  = [str(uuid.uuid4())[:8] for _ in range(50)]
CHANNELS = ["POS", "WEB", "MOBILE", "ATM", "SWIFT"]
CHANNEL_WEIGHTS = [0.40, 0.30, 0.20, 0.08, 0.02]

MERCHANTS_NORMAL = [
    ("MERCADONA_001", "5411"), ("CARREFOUR_002", "5411"),
    ("REPSOL_GAS_003", "5541"), ("RENFE_004", "4112"),
    ("AMAZON_ES_005", "5999"), ("EL_CORTE_006", "5311"),
    ("ZARA_007", "5621"),       ("IKEA_008", "5712"),
    ("NETFLIX_009", "5815"),    ("TELEFONICA_010", "4899"),
    ("FNAC_011", "5734"),       ("MEDIAMARKT_012", "5734"),
    ("BOOKING_013", "7011"),    ("UBER_014", "4121"),
    ("GLOVO_015", "5812"),
]
MERCHANTS_RISKY = [
    ("CASINO_ONLINE_X", "7995"), ("CRYPTO_EXCH_Y", "6051"),
    ("PAWN_SHOP_Z",     "5933"), ("WIRE_TRANSFER_W", "6012"),
]

COUNTRIES_LEGIT  = ["ES", "FR", "DE", "GB", "IT", "PT", "US", "NL", "BE", "SE"]
COUNTRIES_RISK   = ["RU", "BR", "CO", "NG", "TR", "TH"]
COUNTRIES_BLOCK  = ["IR", "KP", "SY", "CU"]

ANSI_GREEN  = "\033[92m"
ANSI_YELLOW = "\033[93m"
ANSI_RED    = "\033[91m"
ANSI_RESET  = "\033[0m"

# ── Kafka producer ────────────────────────────────────────────────────────────
producer = None


def init_kafka(retries=10):
    global producer
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode(),
                acks=1,
                request_timeout_ms=5000,
            )
            print("[Kafka] Conectado.")
            return
        except NoBrokersAvailable:
            print(f"[Kafka] Esperando broker ({i+1}/{retries})...")
            time.sleep(5)
    print("[WARN] Kafka no disponible. Continuando sin Kafka.")


# ── Transaction builders ───────────────────────────────────────────────────────
def normal_txn(account_id: str) -> dict:
    merchant, mcc = random.choice(MERCHANTS_NORMAL)
    country = random.choice(COUNTRIES_LEGIT)
    return {
        "transaction_id":     str(uuid.uuid4()),
        "account_id":         account_id,
        "amount":             round(random.lognormvariate(3.5, 1.0), 2),
        "currency":           "EUR",
        "merchant_id":        merchant,
        "merchant_mcc":       mcc,
        "channel":            random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0],
        "country_origin":     "ES",
        "country_destination": country,
        "device_id":          random.choice(DEVICES),
        "device_age_days":    random.uniform(30, 500),
        "new_merchant_flag":  random.random() < 0.10,
        "latitude":           40.4 + random.gauss(0, 0.5),
        "longitude":          -3.7 + random.gauss(0, 0.5),
        "timestamp":          datetime.now(timezone.utc).isoformat(),
    }


def suspicious_txn(account_id: str) -> dict:
    txn = normal_txn(account_id)
    pattern = random.choice(["high_amount", "risky_merchant", "risky_country",
                              "new_device", "unusual_hour"])
    if pattern == "high_amount":
        txn["amount"] = round(random.uniform(2000, 15000), 2)
    elif pattern == "risky_merchant":
        m, mcc = random.choice(MERCHANTS_RISKY)
        txn["merchant_id"], txn["merchant_mcc"] = m, mcc
        txn["amount"] = round(random.uniform(500, 5000), 2)
    elif pattern == "risky_country":
        txn["country_destination"] = random.choice(COUNTRIES_RISK)
        txn["amount"] = round(random.uniform(1000, 8000), 2)
    elif pattern == "new_device":
        txn["device_id"] = str(uuid.uuid4())[:8]
        txn["device_age_days"] = 0.5
        txn["new_merchant_flag"] = True
    elif pattern == "unusual_hour":
        ts = datetime.now(timezone.utc).replace(hour=random.choice([2, 3, 4]))
        txn["timestamp"] = ts.isoformat()
        txn["amount"] = round(random.uniform(800, 4000), 2)
    return txn


def fraud_txn(account_id: str) -> dict:
    txn = normal_txn(account_id)
    pattern = random.choice(["blocked_country", "velocity_burst",
                              "impossible_travel", "huge_amount", "structuring"])
    if pattern == "blocked_country":
        txn["country_destination"] = random.choice(COUNTRIES_BLOCK)
        txn["amount"] = round(random.uniform(5000, 49000), 2)
    elif pattern == "velocity_burst":
        txn["amount"] = round(random.uniform(100, 800), 2)
        # Will trigger high velocity via repeated calls
    elif pattern == "impossible_travel":
        txn["latitude"]  = 48.85 + random.gauss(0, 0.1)   # Paris
        txn["longitude"] = 2.35  + random.gauss(0, 0.1)
    elif pattern == "huge_amount":
        txn["amount"] = round(random.uniform(50001, 200000), 2)
    elif pattern == "structuring":
        txn["amount"] = round(random.uniform(2800, 3300), 2)
        txn["merchant_id"] = "WIRE_TRANSFER_W"
        txn["merchant_mcc"] = "6012"
    return txn


# ── Main loop ─────────────────────────────────────────────────────────────────
def send_transaction(txn: dict):
    if producer:
        try:
            producer.send("txn.raw", value=txn)
        except Exception as e:
            print(f"[WARN] Kafka send failed: {e}")

    try:
        r = httpx.post(
            f"{SCORING_URL}/v2/transactions/score",
            json=txn,
            timeout=5.0,
        )
        if r.status_code == 200:
            resp = r.json()
            decision = resp.get("decision", "?")
            score    = resp.get("score", 0)
            rules    = ", ".join(resp.get("rules_triggered", [])[:2]) or "—"
            color    = ANSI_GREEN if decision == "APPROVE" else (
                       ANSI_RED if decision == "BLOCK" else ANSI_YELLOW)
            print(
                f"{color}[{decision}]{ANSI_RESET} "
                f"txn={txn['transaction_id'][:8]} "
                f"acc={txn['account_id']} "
                f"amt={txn['amount']:>10.2f}€ "
                f"score={score:.3f} "
                f"rules=[{rules}]"
            )
        else:
            print(f"[ERROR] HTTP {r.status_code}: {r.text[:100]}")
    except httpx.ConnectError:
        print("[WARN] Scoring API no disponible, reintentando...")
    except Exception as e:
        print(f"[ERROR] {e}")


def main():
    print("=" * 60)
    print("  FRAUD TRANSACTION GENERATOR")
    print(f"  TPS={TPS}  FRAUD_RATE={FRAUD_RATE:.0%}")
    print(f"  Target: {SCORING_URL}")
    print("=" * 60)
    print("[*] Esperando 40s para que los servicios arranquen...")
    time.sleep(40)

    init_kafka()
    print("[*] Generando transacciones...\n")

    counter = 0
    while True:
        account_id = random.choice(ACCOUNTS)
        roll = random.random()

        if roll < FRAUD_RATE:
            txn = fraud_txn(account_id)
        elif roll < FRAUD_RATE + 0.25:
            txn = suspicious_txn(account_id)
        else:
            txn = normal_txn(account_id)

        send_transaction(txn)
        counter += 1

        if counter % 50 == 0:
            print(f"\n  [{counter} transacciones enviadas]\n")

        time.sleep(SLEEP)


if __name__ == "__main__":
    main()
