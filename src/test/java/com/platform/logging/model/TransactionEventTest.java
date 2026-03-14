package com.platform.logging.model;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class TransactionEventTest {

    @Test
    @DisplayName("builder creates TransactionEvent with all fields")
    void builderAllFields() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .orgId(42L)
            .orgName("Acme")
            .serviceName("PaymentService")
            .apiName("processPayment")
            .eventType(EventType.API_CALL)
            .startTime(1000L)
            .endTime(2000L)
            .data("{\"key\":\"value\"}")
            .statusCode(200)
            .publishedAt(3000L)
            .build();

        assertEquals("req-1", event.getRequestId());
        assertEquals(42L, event.getOrgId());
        assertEquals("Acme", event.getOrgName());
        assertEquals("PaymentService", event.getServiceName());
        assertEquals("processPayment", event.getApiName());
        assertEquals(EventType.API_CALL, event.getEventType());
        assertEquals(1000L, event.getStartTime());
        assertEquals(2000L, event.getEndTime());
        assertEquals("{\"key\":\"value\"}", event.getData());
        assertEquals(200, event.getStatusCode());
        assertEquals(3000L, event.getPublishedAt());
    }

    @Test
    @DisplayName("no-arg constructor creates event with null fields")
    void noArgConstructor() {
        TransactionEvent event = new TransactionEvent();

        assertNull(event.getRequestId());
        assertNull(event.getOrgId());
        assertNull(event.getEventType());
        assertNull(event.getStartTime());
        assertNull(event.getEndTime());
        assertNull(event.getStatusCode());
        assertEquals(0L, event.getPublishedAt()); // primitive long defaults to 0
    }

    @Test
    @DisplayName("equals and hashCode work correctly")
    void equalsAndHashCode() {
        TransactionEvent e1 = TransactionEvent.builder()
            .requestId("req-1").eventType(EventType.APG_REQUEST).build();
        TransactionEvent e2 = TransactionEvent.builder()
            .requestId("req-1").eventType(EventType.APG_REQUEST).build();

        assertEquals(e1, e2);
        assertEquals(e1.hashCode(), e2.hashCode());
    }

    @Test
    @DisplayName("different events are not equal")
    void notEqual() {
        TransactionEvent e1 = TransactionEvent.builder()
            .requestId("req-1").eventType(EventType.APG_REQUEST).build();
        TransactionEvent e2 = TransactionEvent.builder()
            .requestId("req-2").eventType(EventType.APG_REQUEST).build();

        assertNotEquals(e1, e2);
    }

    @Test
    @DisplayName("setters work on mutable fields")
    void setters() {
        TransactionEvent event = new TransactionEvent();
        event.setRequestId("req-set");
        event.setOrgId(99L);
        event.setEventType(EventType.CORE_PAYLOAD);

        assertEquals("req-set", event.getRequestId());
        assertEquals(99L, event.getOrgId());
        assertEquals(EventType.CORE_PAYLOAD, event.getEventType());
    }
}
