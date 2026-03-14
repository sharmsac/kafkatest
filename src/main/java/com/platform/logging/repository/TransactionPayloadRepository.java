package com.platform.logging.repository;

import com.platform.logging.model.TransactionPayload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.util.List;

@Repository
public class TransactionPayloadRepository {

    private static final String UPSERT_SQL = """
        INSERT INTO TRANSACTION_PAYLOAD
            (request_id, request_body, response_body, core_payload, banking_payload, "date")
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            request_body    = COALESCE(VALUES(request_body),    request_body),
            response_body   = COALESCE(VALUES(response_body),   response_body),
            core_payload    = COALESCE(VALUES(core_payload),    core_payload),
            banking_payload = COALESCE(VALUES(banking_payload), banking_payload)
        """;

    @Autowired private JdbcTemplate jdbc;

    public void batchInsert(List<TransactionPayload> payloads) {
        jdbc.batchUpdate(UPSERT_SQL, payloads, payloads.size(), (ps, p) -> {
            ps.setString(1, p.getRequestId());
            ps.setString(2, p.getRequestBody());
            ps.setString(3, p.getResponseBody());
            ps.setString(4, p.getCorePayload());
            ps.setString(5, p.getBankingPayload());
            ps.setDate(6,   Date.valueOf(p.getDate()));
        });
    }

    public void upsertPayload(String requestId, String field, String data) {
        String sql = switch (field) {
            case "CORE_PAYLOAD"    -> "INSERT INTO TRANSACTION_PAYLOAD (request_id, core_payload, \"date\") VALUES (?, ?, CURDATE()) ON DUPLICATE KEY UPDATE core_payload = VALUES(core_payload)";
            case "BANKING_PAYLOAD" -> "INSERT INTO TRANSACTION_PAYLOAD (request_id, banking_payload, \"date\") VALUES (?, ?, CURDATE()) ON DUPLICATE KEY UPDATE banking_payload = VALUES(banking_payload)";
            default                -> "INSERT INTO TRANSACTION_PAYLOAD (request_id, request_body, \"date\") VALUES (?, ?, CURDATE()) ON DUPLICATE KEY UPDATE request_body = VALUES(request_body)";
        };
        jdbc.update(sql, requestId, data);
    }
}
