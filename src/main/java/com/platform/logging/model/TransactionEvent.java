package com.platform.logging.model;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionEvent {

    private String    requestId;       // UUID — same across all events for one API call
    private Long      orgId;           // organization / client identifier
    private String    orgName;         // denormalized org name for UI
    private String    serviceName;
    private String    apiName;
    private EventType eventType;

    private Long      startTime;       // epoch millis
    private Long      endTime;         // epoch millis
    private String    data;            // payload / header / body (JSON string)
    private Integer   statusCode;
    private long      publishedAt;     // System.currentTimeMillis() at publish
}
