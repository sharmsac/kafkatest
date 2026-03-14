package com.platform.logging.model;

import lombok.*;
import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionPayload {

    private String    requestId;
    private String    requestBody;
    private String    responseBody;
    private String    corePayload;
    private String    bankingPayload;
    private LocalDate date;
}
