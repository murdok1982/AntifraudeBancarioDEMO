"""
Case Management Service — gestiona casos de fraude generados por el scoring.
"""
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter, make_asgi_app
from pydantic import BaseModel

PG_HOST       = os.getenv("POSTGRES_HOST", "localhost")
PG_DB         = os.getenv("POSTGRES_DB", "frauddb")
PG_USER       = os.getenv("POSTGRES_USER", "fraud")
PG_PASS       = os.getenv("POSTGRES_PASSWORD", "fraud123")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")

cases_created = Counter("cases_created_total", "Cases created", ["decision"])
cases_updated = Counter("cases_updated_total", "Cases updated", ["status"])

VALID_STATUSES = {"OPEN", "INVESTIGATING", "CLOSED_FRAUD", "CLOSED_FALSE_POSITIVE"}


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS,
        connect_timeout=5,
    )


def wait_for_pg(retries=30):
    for i in range(retries):
        try:
            conn = get_conn()
            conn.close()
            print("[CaseMgmt] PostgreSQL conectado.")
            return
        except Exception:
            print(f"[CaseMgmt] Esperando PostgreSQL ({i+1}/{retries})...")
            time.sleep(2)
    raise RuntimeError("PostgreSQL no disponible")


def create_case(rec: dict):
    if rec.get("decision") not in ("REVIEW", "BLOCK"):
        return
    try:
        conn = get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cases
                        (case_id, transaction_id, account_id, amount,
                         score, decision, status, risk_level, rules_triggered)
                    VALUES (%s,%s,%s,%s,%s,%s,'OPEN',%s,%s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    str(uuid.uuid4()),
                    rec["transaction_id"],
                    rec.get("account_id", "UNKNOWN"),
                    rec.get("amount", 0),
                    rec.get("score", 0),
                    rec.get("decision"),
                    rec.get("risk_level", "MEDIUM"),
                    json.dumps(rec.get("rules_triggered", [])),
                ))
        conn.close()
        cases_created.labels(decision=rec["decision"]).inc()
    except Exception as e:
        print(f"[WARN] create_case failed: {e}")


def kafka_consumer_loop():
    while True:
        try:
            consumer = KafkaConsumer(
                "txn.scored",
                bootstrap_servers=KAFKA_BROKERS.split(","),
                group_id="case-management-service",
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode()),
                consumer_timeout_ms=1000,
            )
            print("[CaseMgmt] Consumiendo txn.scored...")
            for msg in consumer:
                create_case(msg.value)
        except NoBrokersAvailable:
            print("[CaseMgmt] Kafka no disponible, reintentando en 10s...")
            time.sleep(10)
        except Exception as e:
            print(f"[CaseMgmt] Error Kafka: {e}. Reintentando en 10s...")
            time.sleep(10)


# ── FastAPI ───────────────────────────────────────────────────────────────────
app = FastAPI(title="Case Management API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])
app.mount("/metrics", make_asgi_app())


@app.on_event("startup")
def startup():
    wait_for_pg()
    t = threading.Thread(target=kafka_consumer_loop, daemon=True)
    t.start()


class StatusUpdate(BaseModel):
    status:        str
    analyst_notes: Optional[str] = None


@app.get("/v1/cases")
def list_cases(
    status: Optional[str] = Query(None),
    risk_level: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            where, params = [], []
            if status:
                where.append("status = %s")
                params.append(status)
            if risk_level:
                where.append("risk_level = %s")
                params.append(risk_level)
            clause = "WHERE " + " AND ".join(where) if where else ""
            params += [limit, offset]
            cur.execute(f"""
                SELECT case_id, transaction_id, account_id, amount, score,
                       decision, status, risk_level, rules_triggered,
                       analyst_notes, created_at, updated_at
                FROM cases {clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, params)
            rows = [dict(r) for r in cur.fetchall()]
        return {"cases": rows, "count": len(rows)}
    finally:
        conn.close()


@app.get("/v1/cases/{case_id}")
def get_case(case_id: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM cases WHERE case_id = %s", (case_id,))
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Caso no encontrado")
        return dict(row)
    finally:
        conn.close()


@app.put("/v1/cases/{case_id}/status")
def update_case_status(case_id: str, body: StatusUpdate):
    if body.status not in VALID_STATUSES:
        raise HTTPException(400, f"Status inválido. Válidos: {VALID_STATUSES}")
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE cases
                    SET status = %s, analyst_notes = COALESCE(%s, analyst_notes),
                        updated_at = NOW()
                    WHERE case_id = %s
                    RETURNING case_id
                """, (body.status, body.analyst_notes, case_id))
                if not cur.fetchone():
                    raise HTTPException(404, "Caso no encontrado")
        cases_updated.labels(status=body.status).inc()
        return {"case_id": case_id, "status": body.status, "updated": True}
    finally:
        conn.close()


@app.get("/v1/stats")
def case_stats():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status = 'OPEN') AS open,
                    COUNT(*) FILTER (WHERE status = 'INVESTIGATING') AS investigating,
                    COUNT(*) FILTER (WHERE status = 'CLOSED_FRAUD') AS closed_fraud,
                    COUNT(*) FILTER (WHERE status = 'CLOSED_FALSE_POSITIVE') AS closed_false_positive,
                    COUNT(*) FILTER (WHERE decision = 'BLOCK') AS blocks,
                    COUNT(*) FILTER (WHERE decision = 'REVIEW') AS reviews,
                    ROUND(AVG(score)::numeric, 4) AS avg_score
                FROM cases
            """)
            row = dict(cur.fetchone())
        return row
    finally:
        conn.close()


@app.get("/health")
def health():
    try:
        conn = get_conn()
        conn.close()
        db = "ok"
    except Exception:
        db = "unavailable"
    return {"status": "ok" if db == "ok" else "degraded", "postgres": db}
