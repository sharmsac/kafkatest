package com.platform.logging.controller;

import com.platform.logging.cache.OrgCache;
import com.platform.logging.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.jdbc.core.*;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/api/v1/transactions")
@Slf4j
public class TransactionSearchController {

    @Autowired private JdbcTemplate jdbc;
    @Autowired private OrgCache      orgCache;

    // ===============================================================
    //  /search -- dynamic SQL, resolves orgName -> orgId via OrgCache
    // ===============================================================
    @GetMapping("/search")
    public ResponseEntity<?> search(
            @RequestParam String  orgName,
            @RequestParam String  dateFrom,        // yyyy-MM-dd
            @RequestParam String  dateTo,
            @RequestParam(required = false) String  serviceName,
            @RequestParam(required = false) String  apiName,
            @RequestParam(required = false) Integer minDurationMs,
            @RequestParam(required = false) Integer maxDurationMs,
            @RequestParam(required = false) Integer statusCode,
            @RequestParam(required = false) Boolean errorsOnly,
            @RequestParam(defaultValue = "0")   int page,
            @RequestParam(defaultValue = "50")  int size) {

        // Resolve org name to ID
        Long orgId = orgCache.resolveNameToId(orgName);
        if (orgId == null) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "Unknown org: " + orgName));
        }

        // Build dynamic SQL -- org_id + date always present for partition pruning
        StringBuilder sql = new StringBuilder("""
            SELECT request_id, org_id, org_name, service_name, api_name,
                   total_duration_ms, api_duration_ms, router_duration_ms,
                   network_overhead_ms, status_code, has_router,
                   is_error, is_incomplete, apg_request_time,
                   apg_response_time, `date`, created_at
            FROM TRANSACTION
            WHERE org_id = ?
              AND `date` BETWEEN ? AND ?
            """);

        List<Object> params = new ArrayList<>(
            Arrays.asList(orgId, dateFrom, dateTo));

        if (serviceName   != null) { sql.append(" AND service_name = ?");       params.add(serviceName); }
        if (apiName       != null) { sql.append(" AND api_name = ?");           params.add(apiName); }
        if (minDurationMs != null) { sql.append(" AND total_duration_ms >= ?"); params.add(minDurationMs); }
        if (maxDurationMs != null) { sql.append(" AND total_duration_ms <= ?"); params.add(maxDurationMs); }
        if (statusCode    != null) { sql.append(" AND status_code = ?");        params.add(statusCode); }
        if (Boolean.TRUE.equals(errorsOnly)) { sql.append(" AND is_error = 1"); }

        sql.append(" ORDER BY apg_request_time DESC LIMIT ? OFFSET ?");
        params.add(size);
        params.add(page * size);

        List<Map<String, Object>> results = jdbc.queryForList(
            sql.toString(), params.toArray());

        return ResponseEntity.ok(results);
    }

    // -- Direct lookup by request_id --
    @GetMapping("/{requestId}")
    public ResponseEntity<?> getByRequestId(@PathVariable String requestId) {
        List<Map<String, Object>> results = jdbc.queryForList(
            "SELECT * FROM TRANSACTION WHERE request_id = ?", requestId);
        if (results.isEmpty()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(results.get(0));
    }

    // -- Fetch payload separately -- only on drill-down --
    @GetMapping("/{requestId}/payload")
    public ResponseEntity<?> getPayload(@PathVariable String requestId) {
        List<Map<String, Object>> results = jdbc.queryForList(
            "SELECT * FROM TRANSACTION_PAYLOAD WHERE request_id = ?", requestId);
        if (results.isEmpty()) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(results.get(0));
    }

    // -- Archive search -- for data older than 7 days --
    @GetMapping("/archive/search")
    public ResponseEntity<?> archiveSearch(
            @RequestParam String orgName,
            @RequestParam String monthFrom,      // yyyy-MM-dd (first of month)
            @RequestParam String monthTo,
            @RequestParam(required = false) Integer statusCode,
            @RequestParam(defaultValue = "0")   int page,
            @RequestParam(defaultValue = "50")  int size) {

        Long orgId = orgCache.resolveNameToId(orgName);
        if (orgId == null) {
            return ResponseEntity.badRequest()
                .body(Map.of("error", "Unknown org: " + orgName));
        }

        StringBuilder sql = new StringBuilder("""
            SELECT request_id, org_name, service_name, api_name,
                   total_duration_ms, status_code, `date`, `month`,
                   UNCOMPRESS(data_json) AS data_json
            FROM ARCHIVE_TRANSACTION
            WHERE org_id = ?
              AND `month` BETWEEN ? AND ?
            """);

        List<Object> params = new ArrayList<>(
            Arrays.asList(orgId, monthFrom, monthTo));

        if (statusCode != null) { sql.append(" AND status_code = ?"); params.add(statusCode); }

        sql.append(" ORDER BY `date` DESC LIMIT ? OFFSET ?");
        params.add(size);
        params.add(page * size);

        List<Map<String, Object>> results = jdbc.queryForList(
            sql.toString(), params.toArray());

        return ResponseEntity.ok(results);
    }
}
