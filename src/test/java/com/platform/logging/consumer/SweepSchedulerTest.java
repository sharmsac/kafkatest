package com.platform.logging.consumer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.platform.logging.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SweepSchedulerTest {

    @InjectMocks
    private SweepScheduler sweepScheduler;

    @Mock
    private TransactionAssemblyConsumer consumer;

    private ConcurrentHashMap<String, PartialTransaction> assemblyBuffer;
    private List<Transaction> coldBuffer;
    private List<TransactionPayload> payloadBuffer;
    private Cache<String, Boolean> flushedIds;

    @BeforeEach
    void setUp() {
        assemblyBuffer = new ConcurrentHashMap<>();
        coldBuffer = Collections.synchronizedList(new ArrayList<>());
        payloadBuffer = Collections.synchronizedList(new ArrayList<>());
        flushedIds = CacheBuilder.newBuilder().build();

        lenient().when(consumer.getAssemblyBuffer()).thenReturn(assemblyBuffer);
        lenient().when(consumer.getColdBuffer()).thenReturn(coldBuffer);
        lenient().when(consumer.getPayloadBuffer()).thenReturn(payloadBuffer);
        lenient().when(consumer.getFlushedIds()).thenReturn(flushedIds);
    }

    @Test
    @DisplayName("sweepStaleRecords removes stale entries from assembly buffer")
    void sweepRemovesStale() throws Exception {
        PartialTransaction stale = createStalePartial("stale-1", 10);
        assemblyBuffer.put("stale-1", stale);

        sweepScheduler.sweepStaleRecords();

        assertFalse(assemblyBuffer.containsKey("stale-1"));
        assertEquals(1, coldBuffer.size());
        assertTrue(coldBuffer.get(0).isIncomplete());
    }

    @Test
    @DisplayName("sweepStaleRecords keeps fresh entries in assembly buffer")
    void sweepKeepsFresh() {
        PartialTransaction fresh = new PartialTransaction("fresh-1");
        assemblyBuffer.put("fresh-1", fresh);

        sweepScheduler.sweepStaleRecords();

        assertTrue(assemblyBuffer.containsKey("fresh-1"));
        assertTrue(coldBuffer.isEmpty());
    }

    @Test
    @DisplayName("sweepStaleRecords handles mix of stale and fresh")
    void sweepMixedStaleAndFresh() throws Exception {
        assemblyBuffer.put("fresh-1", new PartialTransaction("fresh-1"));
        assemblyBuffer.put("stale-1", createStalePartial("stale-1", 10));
        assemblyBuffer.put("fresh-2", new PartialTransaction("fresh-2"));

        sweepScheduler.sweepStaleRecords();

        assertEquals(2, assemblyBuffer.size());
        assertTrue(assemblyBuffer.containsKey("fresh-1"));
        assertTrue(assemblyBuffer.containsKey("fresh-2"));
        assertFalse(assemblyBuffer.containsKey("stale-1"));
        assertEquals(1, coldBuffer.size());
    }

    @Test
    @DisplayName("sweepStaleRecords adds payload for stale records with payload data")
    void sweepAddsPayloadForError() throws Exception {
        PartialTransaction stale = createStalePartial("stale-err", 10);
        stale.setStatusCode(500);
        assemblyBuffer.put("stale-err", stale);

        sweepScheduler.sweepStaleRecords();

        assertEquals(1, payloadBuffer.size());
        assertEquals("stale-err", payloadBuffer.get(0).getRequestId());
    }

    @Test
    @DisplayName("sweepStaleRecords marks flushedIds for swept records")
    void sweepMarksFlushedIds() throws Exception {
        assemblyBuffer.put("stale-1", createStalePartial("stale-1", 10));

        sweepScheduler.sweepStaleRecords();

        assertNotNull(flushedIds.getIfPresent("stale-1"));
    }

    @Test
    @DisplayName("sweepStaleRecords does nothing on empty buffer")
    void sweepEmptyBuffer() {
        sweepScheduler.sweepStaleRecords();

        assertTrue(coldBuffer.isEmpty());
        assertTrue(payloadBuffer.isEmpty());
    }

    @Test
    @DisplayName("sweepStaleRecords does not add payload for non-error stale records without payload")
    void sweepNoPayloadForNonError() throws Exception {
        PartialTransaction stale = createStalePartial("stale-ok", 10);
        stale.setStatusCode(200);
        assemblyBuffer.put("stale-ok", stale);

        sweepScheduler.sweepStaleRecords();

        assertTrue(payloadBuffer.isEmpty());
    }

    @Test
    @DisplayName("logBufferMetrics runs without errors")
    void logBufferMetrics() {
        when(consumer.getAssemblyBuffer()).thenReturn(new ConcurrentHashMap<>());
        when(consumer.getHotBuffer()).thenReturn(Collections.synchronizedList(new ArrayList<>()));
        when(consumer.getColdBuffer()).thenReturn(Collections.synchronizedList(new ArrayList<>()));
        when(consumer.getPayloadBuffer()).thenReturn(Collections.synchronizedList(new ArrayList<>()));

        assertDoesNotThrow(() -> sweepScheduler.logBufferMetrics());
    }

    // =====================================================================
    //  Helper: create a PartialTransaction with createdAt in the past
    // =====================================================================
    private PartialTransaction createStalePartial(String requestId, int ageSeconds) throws Exception {
        PartialTransaction partial = new PartialTransaction(requestId);
        // Use reflection to set createdAt to the past
        Field createdAtField = PartialTransaction.class.getDeclaredField("createdAt");
        createdAtField.setAccessible(true);
        createdAtField.set(partial, Instant.now().minusSeconds(ageSeconds));
        return partial;
    }
}
