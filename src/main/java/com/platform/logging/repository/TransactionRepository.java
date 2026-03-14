package com.platform.logging.repository;

import com.platform.logging.model.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.util.List;

@Repository
public class TransactionRepository {

    private static final String UPSERT_SQL = """
        INSERT INTO TRANSACTION (
            request_id, org_id, org_name, service_name, api_name,
            apg_request_time, apg_response_time,
            router_request_time, router_response_time,
            api_request_time, api_response_time,
            has_router, total_duration_ms, api_duration_ms,
            router_duration_ms, network_overhead_ms,
            esp_x_header, status_code, is_error, is_incomplete, "date"
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE
            apg_response_time    = COALESCE(VALUES(apg_response_time),    apg_response_time),
            router_request_time  = COALESCE(VALUES(router_request_time),  router_request_time),
            router_response_time = COALESCE(VALUES(router_response_time), router_response_time),
            api_request_time     = COALESCE(VALUES(api_request_time),     api_request_time),
            api_response_time    = COALESCE(VALUES(api_response_time),    api_response_time),
            total_duration_ms    = COALESCE(VALUES(total_duration_ms),    total_duration_ms),
            api_duration_ms      = COALESCE(VALUES(api_duration_ms),      api_duration_ms),
            router_duration_ms   = COALESCE(VALUES(router_duration_ms),   router_duration_ms),
            network_overhead_ms  = COALESCE(VALUES(network_overhead_ms),  network_overhead_ms),
            is_incomplete        = VALUES(is_incomplete)
        """;

    @Autowired private JdbcTemplate jdbc;

    public void batchInsert(List<Transaction> records) {
        jdbc.batchUpdate(UPSERT_SQL, records, records.size(), (ps, txn) -> {
            ps.setString(1,  txn.getRequestId());
            ps.setObject(2,  txn.getOrgId());
            ps.setString(3,  txn.getOrgName());
            ps.setString(4,  txn.getServiceName());
            ps.setString(5,  txn.getApiName());
            ps.setObject(6,  txn.getApgRequestTime());
            ps.setObject(7,  txn.getApgResponseTime());
            ps.setObject(8,  txn.getRouterRequestTime());
            ps.setObject(9,  txn.getRouterResponseTime());
            ps.setObject(10, txn.getApiRequestTime());
            ps.setObject(11, txn.getApiResponseTime());
            ps.setBoolean(12, txn.isHasRouter());
            ps.setObject(13, txn.getTotalDurationMs());
            ps.setObject(14, txn.getApiDurationMs());
            ps.setObject(15, txn.getRouterDurationMs());
            ps.setObject(16, txn.getNetworkOverheadMs());
            ps.setString(17, txn.getEspXHeader());
            ps.setInt(18,    txn.getStatusCode());
            ps.setBoolean(19, txn.isError());
            ps.setBoolean(20, txn.isIncomplete());
            ps.setDate(21,   Date.valueOf(txn.getDate()));
        });
    }

    public void insertSingle(Transaction txn) {
        batchInsert(List.of(txn));
    }
}
