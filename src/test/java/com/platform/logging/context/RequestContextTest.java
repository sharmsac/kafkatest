package com.platform.logging.context;

import org.junit.jupiter.api.*;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RequestContextTest {

    @AfterEach
    void tearDown() {
        RequestContext.clear();
    }

    @Test
    @DisplayName("set and get returns correct values")
    void setAndGet() {
        RequestContext.set("test-id-123", 42L, "Acme Corp");

        assertEquals("test-id-123", RequestContext.getRequestId());
        assertEquals(42L, RequestContext.getOrgId());
        assertEquals("Acme Corp", RequestContext.getOrgName());
    }

    @Test
    @DisplayName("getRequestId returns UUID when not set")
    void getRequestIdFallback() {
        String id = RequestContext.getRequestId();
        assertNotNull(id);
        // Should be a valid UUID format
        assertDoesNotThrow(() -> UUID.fromString(id));
    }

    @Test
    @DisplayName("getRequestId returns different UUIDs on each call when not set")
    void getRequestIdDifferentUuids() {
        String id1 = RequestContext.getRequestId();
        // Clear to ensure no cached value
        RequestContext.clear();
        String id2 = RequestContext.getRequestId();

        // Each call should generate a new UUID
        assertNotEquals(id1, id2);
    }

    @Test
    @DisplayName("getOrgId returns null when not set")
    void getOrgIdNull() {
        assertNull(RequestContext.getOrgId());
    }

    @Test
    @DisplayName("getOrgName returns null when not set")
    void getOrgNameNull() {
        assertNull(RequestContext.getOrgName());
    }

    @Test
    @DisplayName("clear removes all thread-local values")
    void clearRemovesAll() {
        RequestContext.set("test-id", 1L, "TestOrg");
        RequestContext.clear();

        assertNull(RequestContext.getOrgId());
        assertNull(RequestContext.getOrgName());
        // getRequestId generates a new UUID, so it won't be null
        assertNotEquals("test-id", RequestContext.getRequestId());
    }

    @Test
    @DisplayName("values are isolated between threads")
    void threadIsolation() throws Exception {
        RequestContext.set("main-thread-id", 1L, "MainOrg");

        AtomicReference<String> otherThreadRequestId = new AtomicReference<>();
        AtomicReference<Long> otherThreadOrgId = new AtomicReference<>();
        AtomicReference<String> otherThreadOrgName = new AtomicReference<>();

        CountDownLatch latch = new CountDownLatch(1);

        Thread otherThread = new Thread(() -> {
            RequestContext.set("other-thread-id", 2L, "OtherOrg");
            otherThreadRequestId.set(RequestContext.getRequestId());
            otherThreadOrgId.set(RequestContext.getOrgId());
            otherThreadOrgName.set(RequestContext.getOrgName());
            RequestContext.clear();
            latch.countDown();
        });

        otherThread.start();
        latch.await();

        // Main thread values unchanged
        assertEquals("main-thread-id", RequestContext.getRequestId());
        assertEquals(1L, RequestContext.getOrgId());
        assertEquals("MainOrg", RequestContext.getOrgName());

        // Other thread had its own values
        assertEquals("other-thread-id", otherThreadRequestId.get());
        assertEquals(2L, otherThreadOrgId.get());
        assertEquals("OtherOrg", otherThreadOrgName.get());
    }

    @Test
    @DisplayName("set allows null orgId and orgName")
    void setNullValues() {
        RequestContext.set("id", null, null);

        assertEquals("id", RequestContext.getRequestId());
        assertNull(RequestContext.getOrgId());
        assertNull(RequestContext.getOrgName());
    }

    @Test
    @DisplayName("set overwrites previous values")
    void setOverwrites() {
        RequestContext.set("id-1", 1L, "Org1");
        RequestContext.set("id-2", 2L, "Org2");

        assertEquals("id-2", RequestContext.getRequestId());
        assertEquals(2L, RequestContext.getOrgId());
        assertEquals("Org2", RequestContext.getOrgName());
    }
}
