CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS fraud_decisions (
    id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) UNIQUE NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'EUR',
    channel VARCHAR(20),
    merchant_id VARCHAR(64),
    country_origin VARCHAR(3),
    country_destination VARCHAR(3),
    score DECIMAL(5,4) NOT NULL,
    decision VARCHAR(20) NOT NULL,
    risk_level VARCHAR(20) NOT NULL,
    rules_triggered JSONB DEFAULT '[]',
    shap_values JSONB DEFAULT '{}',
    top_risk_factors JSONB DEFAULT '[]',
    processing_time_ms DECIMAL(8,3),
    model_version VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fd_account  ON fraud_decisions(account_id);
CREATE INDEX IF NOT EXISTS idx_fd_decision ON fraud_decisions(decision);
CREATE INDEX IF NOT EXISTS idx_fd_created  ON fraud_decisions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fd_score    ON fraud_decisions(score DESC);

CREATE TABLE IF NOT EXISTS cases (
    case_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id VARCHAR(64) UNIQUE NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    score DECIMAL(5,4) NOT NULL,
    decision VARCHAR(20) NOT NULL,
    status VARCHAR(30) DEFAULT 'OPEN',
    risk_level VARCHAR(20),
    rules_triggered JSONB DEFAULT '[]',
    analyst_notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cases_status  ON cases(status);
CREATE INDEX IF NOT EXISTS idx_cases_created ON cases(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cases_account ON cases(account_id);

CREATE OR REPLACE VIEW fraud_stats AS
SELECT
    COUNT(*) AS total_transactions,
    COUNT(*) FILTER (WHERE decision = 'APPROVE') AS total_approved,
    COUNT(*) FILTER (WHERE decision = 'REVIEW')  AS total_review,
    COUNT(*) FILTER (WHERE decision = 'BLOCK')   AS total_blocked,
    ROUND(AVG(score)::numeric, 4)                AS avg_score,
    ROUND(AVG(processing_time_ms)::numeric, 2)   AS avg_processing_ms,
    ROUND(COUNT(*) FILTER (WHERE decision = 'BLOCK') * 100.0
          / NULLIF(COUNT(*), 0), 2)              AS block_rate_pct,
    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 hour') AS txn_last_hour
FROM fraud_decisions;
