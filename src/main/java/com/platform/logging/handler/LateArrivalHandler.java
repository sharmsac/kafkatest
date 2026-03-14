package com.platform.logging.handler;

import com.platform.logging.model.*;
import com.platform.logging.repository.TransactionPayloadRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LateArrivalHandler {

    @Autowired private JdbcTemplate jdbc;
    @Autowired private TransactionPayloadRepository payloadRepo;

    public void handle(TransactionEvent event) {
        switch (event.getEventType()) {
            case ROUTER_REQUEST, ROUTER_RESPONSE ->
                updateRouterTimes(event.getRequestId(), event.getStartTime(), event.getEndTime());
            case APG_RESPONSE ->
                updateApgEnd(event.getRequestId(), event.getEndTime());
            case CORE_PAYLOAD, BANKING_PAYLOAD, ESPX_HEADER ->
                handleLatePayload(event);
            default ->
                log.debug("Ignoring late arrival type {} for {}",
                    event.getEventType(), event.getRequestId());
        }
    }

    // -- Update router times on an already-written row --
    public void updateRouterTimes(String requestId, Long routerStart, Long routerEnd) {
        Integer durationMs = null;
        if (routerStart != null && routerEnd != null) {
            durationMs = (int) (routerEnd - routerStart);
        }

        jdbc.update("""
            UPDATE TRANSACTION
            SET router_request_time  = COALESCE(?, router_request_time),
                router_response_time = COALESCE(?, router_response_time),
                router_duration_ms   = COALESCE(?, router_duration_ms),
                has_router           = 1,
                is_incomplete        = 0
            WHERE request_id = ?
              AND router_request_time IS NULL
            """,
            routerStart, routerEnd, durationMs, requestId
        );
    }

    // -- Update APG end time (and recompute total duration) --
    public void updateApgEnd(String requestId, Long apgEnd) {
        if (apgEnd == null) return;

        jdbc.update("""
            UPDATE TRANSACTION
            SET apg_response_time = ?,
                total_duration_ms = CASE
                    WHEN apg_request_time IS NOT NULL
                    THEN CAST((? - apg_request_time) AS SIGNED)
                    ELSE total_duration_ms
                END,
                is_incomplete = 0
            WHERE request_id = ?
              AND apg_response_time IS NULL
            """,
            apgEnd, apgEnd, requestId
        );
    }

    // -- Late payload: upsert into TRANSACTION_PAYLOAD --
    public void handleLatePayload(TransactionEvent event) {
        payloadRepo.upsertPayload(
            event.getRequestId(),
            event.getEventType().name(),
            event.getData()
        );
    }
}
