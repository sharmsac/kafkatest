package com.platform.logging.model;

import org.junit.jupiter.api.*;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class TransactionPayloadTest {

    @Test
    @DisplayName("builder creates TransactionPayload with all fields")
    void builderAllFields() {
        TransactionPayload payload = TransactionPayload.builder()
            .requestId("req-1")
            .requestBody("{\"req\":1}")
            .responseBody("{\"resp\":1}")
            .corePayload("{\"core\":1}")
            .bankingPayload("{\"bank\":1}")
            .date(LocalDate.of(2025, 6, 15))
            .build();

        assertEquals("req-1", payload.getRequestId());
        assertEquals("{\"req\":1}", payload.getRequestBody());
        assertEquals("{\"resp\":1}", payload.getResponseBody());
        assertEquals("{\"core\":1}", payload.getCorePayload());
        assertEquals("{\"bank\":1}", payload.getBankingPayload());
        assertEquals(LocalDate.of(2025, 6, 15), payload.getDate());
    }

    @Test
    @DisplayName("builder handles null payload fields")
    void builderNullFields() {
        TransactionPayload payload = TransactionPayload.builder()
            .requestId("req-1")
            .date(LocalDate.now())
            .build();

        assertNull(payload.getRequestBody());
        assertNull(payload.getResponseBody());
        assertNull(payload.getCorePayload());
        assertNull(payload.getBankingPayload());
    }

    @Test
    @DisplayName("equals works correctly")
    void equalsWorks() {
        TransactionPayload p1 = TransactionPayload.builder()
            .requestId("r1").date(LocalDate.now()).build();
        TransactionPayload p2 = TransactionPayload.builder()
            .requestId("r1").date(LocalDate.now()).build();
        assertEquals(p1, p2);
    }
}
