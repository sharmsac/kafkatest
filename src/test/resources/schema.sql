-- H2-compatible schema (no partitioning, no backtick-quoted DATE)

CREATE TABLE IF NOT EXISTS TRANSACTION (
    request_id            VARCHAR(36)    NOT NULL,
    org_id                BIGINT         NOT NULL,
    org_name              VARCHAR(100)   NOT NULL,
    service_name          VARCHAR(100),
    api_name              VARCHAR(100),
    apg_request_time      BIGINT,
    apg_response_time     BIGINT,
    router_request_time   BIGINT,
    router_response_time  BIGINT,
    has_router            TINYINT        DEFAULT 0,
    api_request_time      BIGINT,
    api_response_time     BIGINT,
    total_duration_ms     INT,
    api_duration_ms       INT,
    router_duration_ms    INT,
    network_overhead_ms   INT,
    esp_x_header          CLOB,
    status_code           INT,
    is_error              TINYINT        DEFAULT 0,
    is_incomplete         TINYINT        DEFAULT 0,
    "date"                DATE           NOT NULL,
    created_at            TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (request_id, "date")
);

CREATE INDEX IF NOT EXISTS idx_org_date ON TRANSACTION (org_id, "date");
CREATE INDEX IF NOT EXISTS idx_org_date_status ON TRANSACTION (org_id, "date", status_code);

CREATE TABLE IF NOT EXISTS TRANSACTION_PAYLOAD (
    request_id       VARCHAR(36)   NOT NULL,
    request_body     CLOB,
    response_body    CLOB,
    core_payload     CLOB,
    banking_payload  CLOB,
    "date"           DATE          NOT NULL,
    created_at       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (request_id, "date")
);
