package com.platform.logging.model;

import org.junit.jupiter.api.*;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class TransactionTest {

    @Test
    @DisplayName("builder creates Transaction with all fields")
    void builderAllFields() {
        Transaction txn = Transaction.builder()
            .requestId("req-1")
            .orgId(42L)
            .orgName("Acme")
            .serviceName("APG")
            .apiName("process")
            .apgRequestTime(1000L)
            .apgResponseTime(2000L)
            .routerRequestTime(1100L)
            .routerResponseTime(1900L)
            .apiRequestTime(1200L)
            .apiResponseTime(1800L)
            .hasRouter(true)
            .totalDurationMs(1000)
            .apiDurationMs(600)
            .routerDurationMs(800)
            .networkOverheadMs(100)
            .espXHeader("{\"esp\":true}")
            .statusCode(200)
            .isError(false)
            .incomplete(false)
            .date(LocalDate.of(2025, 6, 15))
            .build();

        assertEquals("req-1", txn.getRequestId());
        assertEquals(42L, txn.getOrgId());
        assertEquals("Acme", txn.getOrgName());
        assertTrue(txn.isHasRouter());
        assertEquals(1000, txn.getTotalDurationMs());
        assertFalse(txn.isError());
        assertFalse(txn.isIncomplete());
        assertEquals(LocalDate.of(2025, 6, 15), txn.getDate());
    }

    @Test
    @DisplayName("no-arg constructor defaults booleans to false")
    void noArgDefaults() {
        Transaction txn = new Transaction();

        assertFalse(txn.isHasRouter());
        assertFalse(txn.isError());
        assertFalse(txn.isIncomplete());
        assertEquals(0, txn.getStatusCode());
    }

    @Test
    @DisplayName("setIncomplete marks transaction as incomplete")
    void setIncomplete() {
        Transaction txn = new Transaction();
        txn.setIncomplete(true);
        assertTrue(txn.isIncomplete());
    }

    @Test
    @DisplayName("setError marks transaction as error")
    void setError() {
        Transaction txn = new Transaction();
        txn.setError(true);
        assertTrue(txn.isError());
    }

    @Test
    @DisplayName("equals works correctly for same data")
    void equalsWorks() {
        Transaction t1 = Transaction.builder().requestId("r1").date(LocalDate.now()).build();
        Transaction t2 = Transaction.builder().requestId("r1").date(LocalDate.now()).build();
        assertEquals(t1, t2);
    }
}
