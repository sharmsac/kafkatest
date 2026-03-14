package com.platform.logging.model;

import lombok.*;
import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction {

    private String    requestId;
    private Long      orgId;
    private String    orgName;
    private String    serviceName;
    private String    apiName;

    // Timestamps (epoch millis)
    private Long      apgRequestTime;
    private Long      apgResponseTime;
    private Long      routerRequestTime;
    private Long      routerResponseTime;
    private Long      apiRequestTime;
    private Long      apiResponseTime;
    private boolean   hasRouter;

    // Pre-computed durations (ms)
    private Integer   totalDurationMs;
    private Integer   apiDurationMs;
    private Integer   routerDurationMs;
    private Integer   networkOverheadMs;

    // Metadata
    private String    espXHeader;
    private int       statusCode;
    private boolean   isError;
    private boolean   incomplete;

    private LocalDate date;
}
