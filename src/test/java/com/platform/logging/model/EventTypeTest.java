package com.platform.logging.model;

import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class EventTypeTest {

    @Test
    @DisplayName("EventType has exactly 8 values")
    void eventTypeCount() {
        assertEquals(8, EventType.values().length);
    }

    @Test
    @DisplayName("EventType contains all expected values")
    void eventTypeValues() {
        assertNotNull(EventType.APG_REQUEST);
        assertNotNull(EventType.APG_RESPONSE);
        assertNotNull(EventType.ROUTER_REQUEST);
        assertNotNull(EventType.ROUTER_RESPONSE);
        assertNotNull(EventType.API_CALL);
        assertNotNull(EventType.ESPX_HEADER);
        assertNotNull(EventType.CORE_PAYLOAD);
        assertNotNull(EventType.BANKING_PAYLOAD);
    }

    @Test
    @DisplayName("EventType valueOf works for all values")
    void eventTypeValueOf() {
        assertEquals(EventType.APG_REQUEST, EventType.valueOf("APG_REQUEST"));
        assertEquals(EventType.APG_RESPONSE, EventType.valueOf("APG_RESPONSE"));
        assertEquals(EventType.ROUTER_REQUEST, EventType.valueOf("ROUTER_REQUEST"));
        assertEquals(EventType.ROUTER_RESPONSE, EventType.valueOf("ROUTER_RESPONSE"));
        assertEquals(EventType.API_CALL, EventType.valueOf("API_CALL"));
        assertEquals(EventType.ESPX_HEADER, EventType.valueOf("ESPX_HEADER"));
        assertEquals(EventType.CORE_PAYLOAD, EventType.valueOf("CORE_PAYLOAD"));
        assertEquals(EventType.BANKING_PAYLOAD, EventType.valueOf("BANKING_PAYLOAD"));
    }

    @Test
    @DisplayName("EventType name() returns correct string")
    void eventTypeName() {
        assertEquals("APG_REQUEST", EventType.APG_REQUEST.name());
        assertEquals("BANKING_PAYLOAD", EventType.BANKING_PAYLOAD.name());
    }

    @Test
    @DisplayName("EventType valueOf throws for invalid value")
    void eventTypeInvalid() {
        assertThrows(IllegalArgumentException.class,
            () -> EventType.valueOf("INVALID_TYPE"));
    }
}
