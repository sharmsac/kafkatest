package com.platform.logging.consumer;

import com.platform.logging.handler.LateArrivalHandler;
import com.platform.logging.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionAssemblyConsumerTest {

    @InjectMocks
    private TransactionAssemblyConsumer consumer;

    @Mock
    private LateArrivalHandler lateArrivalHandler;

    @Mock
    private KafkaListenerEndpointRegistry registry;

    @Mock
    private Acknowledgment ack;

    @BeforeEach
    void setUp() {
        consumer.getAssemblyBuffer().clear();
        consumer.getHotBuffer().clear();
        consumer.getColdBuffer().clear();
        consumer.getPayloadBuffer().clear();
        consumer.getFlushedIds().invalidateAll();
    }

    // =====================================================================
    //  Normal assembly flow — API_CALL is the flush trigger
    // =====================================================================

    @Test
    @DisplayName("consume adds event to assembly buffer for new requestId")
    void consumeNewEvent() {
        consumer.consume(apgRequestEvent("req-1", 1000L), ack);

        assertEquals(1, consumer.getAssemblyBuffer().size());
        assertTrue(consumer.getAssemblyBuffer().containsKey("req-1"));
        assertTrue(consumer.getHotBuffer().isEmpty());
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("APG_REQUEST + APG_RESPONSE stays in assembly (no API_CALL yet)")
    void consumeApgOnlyStaysInAssembly() {
        consumer.consume(apgRequestEvent("req-1", 1000L), ack);
        consumer.consume(apgResponseEvent("req-1", 2000L), ack);

        // APG alone does NOT trigger flush — needs API_CALL
        assertEquals(1, consumer.getAssemblyBuffer().size());
        assertTrue(consumer.getHotBuffer().isEmpty());
    }

    @Test
    @DisplayName("API_CALL triggers flush to hot buffer")
    void consumeApiCallTriggersFlush() {
        consumer.consume(apgRequestEvent("req-1", 1000L), ack);
        consumer.consume(apiCallEvent("req-1", 1200L, 1800L, 200), ack);

        // API_CALL triggers hasMinimumFields → moved to hot buffer
        assertEquals(0, consumer.getAssemblyBuffer().size());
        assertEquals(1, consumer.getHotBuffer().size());
    }

    @Test
    @DisplayName("APG + Router + API_CALL all merged before flush")
    void consumeFullFlowBeforeFlush() {
        consumer.consume(apgRequestEvent("req-1", 1000L), ack);
        consumer.consume(routerRequestEvent("req-1", 1100L), ack);
        // Still waiting for API_CALL
        assertEquals(1, consumer.getAssemblyBuffer().size());
        assertTrue(consumer.getHotBuffer().isEmpty());

        consumer.consume(apiCallEvent("req-1", 1200L, 1700L, 200), ack);
        // NOW it flushes
        assertEquals(0, consumer.getAssemblyBuffer().size());
        assertEquals(1, consumer.getHotBuffer().size());

        Transaction txn = consumer.getHotBuffer().get(0);
        assertEquals("req-1", txn.getRequestId());
        assertEquals(500, txn.getApiDurationMs());
        assertTrue(txn.isHasRouter());
        assertFalse(txn.isIncomplete());
    }

    @Test
    @DisplayName("consume adds payload for error transactions on flush")
    void consumeErrorPayload() {
        consumer.consume(apgRequestEvent("req-1", 1000L), ack);

        // Add some payload data
        consumer.consume(TransactionEvent.builder()
            .requestId("req-1").eventType(EventType.CORE_PAYLOAD)
            .data("{\"error\":\"details\"}").build(), ack);

        // API_CALL with error status triggers flush
        consumer.consume(TransactionEvent.builder()
            .requestId("req-1").eventType(EventType.API_CALL)
            .startTime(1200L).endTime(1800L).statusCode(500).build(), ack);

        // Error status + payload data => shouldWritePayload is true
        assertEquals(1, consumer.getPayloadBuffer().size());
    }

    @Test
    @DisplayName("consume does not add payload for success transactions without payload data")
    void consumeSuccessNoPayload() {
        consumer.consume(apiCallEvent("req-1", 1000L, 2000L, 200), ack);

        assertTrue(consumer.getPayloadBuffer().isEmpty());
    }

    @Test
    @DisplayName("consume marks requestId in flushedIds after API_CALL flush")
    void consumeMarksFlushedIds() {
        consumer.consume(apiCallEvent("req-1", 1000L, 2000L, 200), ack);

        assertNotNull(consumer.getFlushedIds().getIfPresent("req-1"));
    }

    // =====================================================================
    //  Late arrival handling
    // =====================================================================

    @Test
    @DisplayName("consume delegates to LateArrivalHandler when requestId in flushedIds")
    void consumeLateArrival() {
        consumer.getFlushedIds().put("req-1", true);

        TransactionEvent lateEvent = TransactionEvent.builder()
            .requestId("req-1").eventType(EventType.ROUTER_RESPONSE)
            .endTime(1900L).build();

        consumer.consume(lateEvent, ack);

        verify(lateArrivalHandler).handle(lateEvent);
        verify(ack).acknowledge();
        assertTrue(consumer.getAssemblyBuffer().isEmpty());
    }

    @Test
    @DisplayName("APG_RESPONSE arriving after flush is handled as late arrival")
    void apgResponseLateArrival() {
        // API_CALL flushes first
        consumer.consume(apgRequestEvent("req-1", 1000L), ack);
        consumer.consume(apiCallEvent("req-1", 1200L, 1700L, 200), ack);
        assertEquals(1, consumer.getHotBuffer().size());

        // APG_RESPONSE arrives after flush — should be late arrival
        consumer.consume(apgResponseEvent("req-1", 2000L), ack);
        verify(lateArrivalHandler).handle(any());
    }

    @Test
    @DisplayName("late arrival does not create new assembly entry")
    void lateArrivalNoAssembly() {
        consumer.getFlushedIds().put("req-1", true);

        consumer.consume(apgRequestEvent("req-1", 1000L), ack);

        assertTrue(consumer.getAssemblyBuffer().isEmpty());
        assertTrue(consumer.getHotBuffer().isEmpty());
    }

    // =====================================================================
    //  Multiple request IDs
    // =====================================================================

    @Test
    @DisplayName("consume handles multiple request IDs independently")
    void consumeMultipleRequests() {
        consumer.consume(apgRequestEvent("req-1", 1000L), ack);
        consumer.consume(apgRequestEvent("req-2", 2000L), ack);
        // req-1 gets API_CALL — flushes
        consumer.consume(apiCallEvent("req-1", 1200L, 1500L, 200), ack);

        assertEquals(1, consumer.getHotBuffer().size());
        assertEquals("req-1", consumer.getHotBuffer().get(0).getRequestId());

        // req-2 still in assembly (no API_CALL yet)
        assertEquals(1, consumer.getAssemblyBuffer().size());
        assertTrue(consumer.getAssemblyBuffer().containsKey("req-2"));
    }

    // =====================================================================
    //  evictOldest
    // =====================================================================

    @Test
    @DisplayName("evictOldest moves records from assembly to cold buffer")
    void evictOldest() {
        for (int i = 0; i < 10; i++) {
            consumer.consume(apgRequestEvent("req-" + i, 1000L + i), ack);
        }

        assertEquals(10, consumer.getAssemblyBuffer().size());

        consumer.evictOldest(3);

        assertEquals(7, consumer.getAssemblyBuffer().size());
        assertEquals(3, consumer.getColdBuffer().size());
        consumer.getColdBuffer().forEach(txn -> assertTrue(txn.isIncomplete()));
    }

    @Test
    @DisplayName("evictOldest marks evicted IDs in flushedIds")
    void evictOldestMarksFlushedIds() {
        consumer.consume(apgRequestEvent("req-evict", 1000L), ack);
        consumer.evictOldest(1);

        assertNotNull(consumer.getFlushedIds().getIfPresent("req-evict"));
    }

    @Test
    @DisplayName("evictOldest handles empty assembly buffer gracefully")
    void evictOldestEmptyBuffer() {
        assertDoesNotThrow(() -> consumer.evictOldest(10));
        assertTrue(consumer.getColdBuffer().isEmpty());
    }

    // =====================================================================
    //  Realistic flow: APG → Router → API_CALL → flush → late arrivals
    // =====================================================================

    @Test
    @DisplayName("realistic flow: APG+Router arrive, API_CALL flushes, then responses arrive late")
    void consumeRealisticFlow() {
        String reqId = "req-full";

        // Phase 1: APG_REQUEST arrives
        consumer.consume(TransactionEvent.builder()
            .requestId(reqId).eventType(EventType.APG_REQUEST)
            .orgId(1L).orgName("Acme").serviceName("APG").apiName("process")
            .startTime(1000L).build(), ack);

        // Phase 2: ROUTER_REQUEST arrives
        consumer.consume(TransactionEvent.builder()
            .requestId(reqId).eventType(EventType.ROUTER_REQUEST)
            .startTime(1100L).build(), ack);

        // Still in assembly
        assertEquals(1, consumer.getAssemblyBuffer().size());

        // Phase 3: API_CALL arrives → FLUSH
        consumer.consume(TransactionEvent.builder()
            .requestId(reqId).eventType(EventType.API_CALL)
            .startTime(1200L).endTime(1700L).statusCode(200).build(), ack);

        assertEquals(0, consumer.getAssemblyBuffer().size());
        assertEquals(1, consumer.getHotBuffer().size());

        Transaction txn = consumer.getHotBuffer().get(0);
        assertEquals(reqId, txn.getRequestId());
        assertEquals(500, txn.getApiDurationMs());
        assertTrue(txn.isHasRouter());
        assertFalse(txn.isIncomplete());
        // APG total = null (no APG end yet), router total = null (no router end yet)
        // Falls back to API duration = 500
        assertEquals(500, txn.getTotalDurationMs());

        // Phase 4: ROUTER_RESPONSE arrives late
        consumer.consume(TransactionEvent.builder()
            .requestId(reqId).eventType(EventType.ROUTER_RESPONSE)
            .endTime(1800L).build(), ack);
        verify(lateArrivalHandler).handle(argThat(e ->
            e.getEventType() == EventType.ROUTER_RESPONSE));

        // Phase 5: APG_RESPONSE arrives late
        consumer.consume(TransactionEvent.builder()
            .requestId(reqId).eventType(EventType.APG_RESPONSE)
            .endTime(1900L).build(), ack);
        verify(lateArrivalHandler).handle(argThat(e ->
            e.getEventType() == EventType.APG_RESPONSE));
    }

    @Test
    @DisplayName("API_CALL alone (no APG/Router) triggers flush and is complete")
    void consumeApiCallAlone() {
        consumer.consume(apiCallEvent("req-solo", 1000L, 2000L, 200), ack);

        assertEquals(0, consumer.getAssemblyBuffer().size());
        assertEquals(1, consumer.getHotBuffer().size());

        Transaction txn = consumer.getHotBuffer().get(0);
        assertEquals(1000, txn.getTotalDurationMs());
        assertFalse(txn.isIncomplete());
    }

    // =====================================================================
    //  Helper methods
    // =====================================================================

    private TransactionEvent apgRequestEvent(String requestId, long startTime) {
        return TransactionEvent.builder()
            .requestId(requestId).eventType(EventType.APG_REQUEST)
            .startTime(startTime).build();
    }

    private TransactionEvent apgResponseEvent(String requestId, long endTime) {
        return TransactionEvent.builder()
            .requestId(requestId).eventType(EventType.APG_RESPONSE)
            .endTime(endTime).build();
    }

    private TransactionEvent routerRequestEvent(String requestId, long startTime) {
        return TransactionEvent.builder()
            .requestId(requestId).eventType(EventType.ROUTER_REQUEST)
            .startTime(startTime).build();
    }

    private TransactionEvent apiCallEvent(String requestId, long startTime, long endTime, int status) {
        return TransactionEvent.builder()
            .requestId(requestId).eventType(EventType.API_CALL)
            .startTime(startTime).endTime(endTime).statusCode(status).build();
    }
}
