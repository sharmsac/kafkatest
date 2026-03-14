package com.platform.logging.consumer;

import com.platform.logging.model.*;
import lombok.Data;
import java.time.*;

@Data
public class PartialTransaction {

    private final String  requestId;
    private final Instant createdAt = Instant.now();

    private Long    orgId;
    private String  orgName;
    private String  serviceName;
    private String  apiName;

    // -- Timestamps (epoch millis) --
    private Long    apgRequestTime;
    private Long    apgResponseTime;
    private Long    routerRequestTime;
    private Long    routerResponseTime;
    private Long    apiRequestTime;
    private Long    apiResponseTime;

    // -- Payloads --
    private String  espxHeader;
    private String  requestBody;
    private String  responseBody;
    private String  corePayload;
    private String  bankingPayload;

    private Integer statusCode;
    private boolean hasRouter = false;

    // -- Arrival tracking --
    private boolean hasApgStart = false;
    private boolean hasApgEnd   = false;
    private boolean hasApiCall  = false;

    // ===============================================================
    //  merge() -- called by consumer for each arriving event
    // ===============================================================
    public void merge(TransactionEvent event) {
        if (event.getOrgId()       != null) this.orgId       = event.getOrgId();
        if (event.getOrgName()     != null) this.orgName     = event.getOrgName();
        if (event.getServiceName() != null) this.serviceName = event.getServiceName();
        if (event.getApiName()     != null) this.apiName     = event.getApiName();
        if (event.getStatusCode()  != null) this.statusCode  = event.getStatusCode();

        switch (event.getEventType()) {
            case APG_REQUEST -> {
                this.apgRequestTime = event.getStartTime();
                this.hasApgStart    = true;
            }
            case APG_RESPONSE -> {
                this.apgResponseTime = event.getEndTime();
                this.hasApgEnd       = true;
            }
            case ROUTER_REQUEST -> {
                this.routerRequestTime = event.getStartTime();
                this.hasRouter         = true;
            }
            case ROUTER_RESPONSE -> {
                this.routerResponseTime = event.getEndTime();
            }
            case API_CALL -> {
                this.apiRequestTime  = event.getStartTime();
                this.apiResponseTime = event.getEndTime();
                this.hasApiCall      = true;
                if (event.getStatusCode() != null) this.statusCode = event.getStatusCode();
            }
            case ESPX_HEADER -> {
                this.espxHeader = event.getData();
            }
            case CORE_PAYLOAD -> {
                this.corePayload = event.getData();
            }
            case BANKING_PAYLOAD -> {
                this.bankingPayload = event.getData();
            }
        }
    }

    // ===============================================================
    //  Assembly readiness checks
    // ===============================================================

    /** Minimum to flush: API_CALL received (the innermost call is complete).
     *  APG/Router responses arrive later and are handled as late arrivals. */
    public boolean hasMinimumFields() {
        return hasApiCall;
    }

    /** Check if record has been in memory too long */
    public boolean isStale(int ttlSeconds) {
        return Instant.now().isAfter(createdAt.plusSeconds(ttlSeconds));
    }

    /** Only write payload for errors or if payload data exists */
    public boolean shouldWritePayload() {
        boolean isError = statusCode != null && statusCode >= 400;
        boolean hasPayload = corePayload != null
                           || bankingPayload != null
                           || requestBody != null
                           || responseBody != null;
        return isError || hasPayload;
    }

    // ===============================================================
    //  toTransaction() -- full duration computation with fallback chain
    //  Priority: APG (end-to-end) -> Router -> API
    // ===============================================================
    public Transaction toTransaction() {
        Transaction txn = new Transaction();
        txn.setRequestId(requestId);
        txn.setOrgId(orgId);
        txn.setOrgName(orgName);
        txn.setServiceName(serviceName);
        txn.setApiName(apiName);
        txn.setApgRequestTime(apgRequestTime);
        txn.setApgResponseTime(apgResponseTime);
        txn.setRouterRequestTime(routerRequestTime);
        txn.setRouterResponseTime(routerResponseTime);
        txn.setApiRequestTime(apiRequestTime);
        txn.setApiResponseTime(apiResponseTime);
        txn.setHasRouter(hasRouter);
        txn.setEspXHeader(espxHeader);
        txn.setStatusCode(statusCode != null ? statusCode : 0);
        txn.setError(statusCode != null && statusCode >= 400);
        txn.setDate(LocalDate.now());
        txn.setIncomplete(!hasMinimumFields());

        // -- API duration --
        Integer apiDur = durationMs(apiRequestTime, apiResponseTime);
        txn.setApiDurationMs(apiDur);

        // -- Router duration --
        Integer routerDur = durationMs(routerRequestTime, routerResponseTime);
        txn.setRouterDurationMs(routerDur);

        // -- Total duration: fallback chain APG -> Router -> API --
        Integer totalDur = durationMs(apgRequestTime, apgResponseTime);
        if (totalDur == null) totalDur = routerDur;
        if (totalDur == null) totalDur = apiDur;
        txn.setTotalDurationMs(totalDur);

        // -- Network overhead = total - (api + router) --
        if (totalDur != null && apiDur != null) {
            int accounted = apiDur + (routerDur != null ? routerDur : 0);
            txn.setNetworkOverheadMs(Math.max(0, totalDur - accounted));
        }

        return txn;
    }

    // ===============================================================
    //  toPayload() -- extract payload fields for separate table
    // ===============================================================
    public TransactionPayload toPayload() {
        return TransactionPayload.builder()
            .requestId(requestId)
            .requestBody(requestBody)
            .responseBody(responseBody)
            .corePayload(corePayload)
            .bankingPayload(bankingPayload)
            .date(LocalDate.now())
            .build();
    }

    private Integer durationMs(Long start, Long end) {
        if (start != null && end != null && end >= start) {
            return (int) (end - start);
        }
        return null;
    }
}
