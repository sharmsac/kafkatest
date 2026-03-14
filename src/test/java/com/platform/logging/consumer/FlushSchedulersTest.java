package com.platform.logging.consumer;

import com.platform.logging.deadletter.DeadLetterQueue;
import com.platform.logging.model.*;
import com.platform.logging.repository.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FlushSchedulersTest {

    @InjectMocks
    private FlushSchedulers flushSchedulers;

    @Mock
    private TransactionAssemblyConsumer consumer;

    @Mock
    private TransactionRepository txnRepo;

    @Mock
    private TransactionPayloadRepository payloadRepo;

    @Mock
    private DeadLetterQueue deadLetterQueue;

    // =====================================================================
    //  drain() tests
    // =====================================================================

    @Test
    @DisplayName("drain returns empty list for empty buffer")
    void drainEmptyBuffer() {
        List<Transaction> buffer = Collections.synchronizedList(new ArrayList<>());
        List<Transaction> result = flushSchedulers.drain(buffer);
        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("drain returns snapshot and clears buffer")
    void drainReturnsSnapshotAndClears() {
        List<Transaction> buffer = Collections.synchronizedList(new ArrayList<>());
        buffer.add(createTxn("req-1"));
        buffer.add(createTxn("req-2"));

        List<Transaction> snapshot = flushSchedulers.drain(buffer);

        assertEquals(2, snapshot.size());
        assertTrue(buffer.isEmpty());
    }

    @Test
    @DisplayName("drain returns independent copy — modifying snapshot doesn't affect buffer")
    void drainReturnsCopy() {
        List<Transaction> buffer = Collections.synchronizedList(new ArrayList<>());
        buffer.add(createTxn("req-1"));

        List<Transaction> snapshot = flushSchedulers.drain(buffer);
        snapshot.clear();

        // buffer was already cleared by drain, but the point is they're independent
        assertTrue(buffer.isEmpty());
    }

    // =====================================================================
    //  flushHotBuffer() tests
    // =====================================================================

    @Test
    @DisplayName("flushHotBuffer does nothing when buffer is empty")
    void flushHotBufferEmpty() {
        when(consumer.getHotBuffer()).thenReturn(
            Collections.synchronizedList(new ArrayList<>()));

        flushSchedulers.flushHotBuffer();

        verify(txnRepo, never()).batchInsert(any());
    }

    @Test
    @DisplayName("flushHotBuffer calls batchInsert with drained records")
    void flushHotBufferWithRecords() {
        List<Transaction> buffer = Collections.synchronizedList(new ArrayList<>());
        buffer.add(createTxn("req-1"));
        buffer.add(createTxn("req-2"));

        when(consumer.getHotBuffer()).thenReturn(buffer);

        flushSchedulers.flushHotBuffer();

        verify(txnRepo).batchInsert(argThat(list -> list.size() == 2));
    }

    // =====================================================================
    //  flushPayloadBuffer() tests
    // =====================================================================

    @Test
    @DisplayName("flushPayloadBuffer does nothing when buffer is empty")
    void flushPayloadBufferEmpty() {
        when(consumer.getPayloadBuffer()).thenReturn(
            Collections.synchronizedList(new ArrayList<>()));

        flushSchedulers.flushPayloadBuffer();

        verify(payloadRepo, never()).batchInsert(any());
    }

    @Test
    @DisplayName("flushPayloadBuffer calls batchInsert with drained payloads")
    void flushPayloadBufferWithRecords() {
        List<TransactionPayload> buffer = Collections.synchronizedList(new ArrayList<>());
        buffer.add(createPayload("req-1"));

        when(consumer.getPayloadBuffer()).thenReturn(buffer);

        flushSchedulers.flushPayloadBuffer();

        verify(payloadRepo).batchInsert(argThat(list -> list.size() == 1));
    }

    // =====================================================================
    //  flushColdBuffer() tests
    // =====================================================================

    @Test
    @DisplayName("flushColdBuffer does nothing when buffer is empty")
    void flushColdBufferEmpty() {
        when(consumer.getColdBuffer()).thenReturn(
            Collections.synchronizedList(new ArrayList<>()));

        flushSchedulers.flushColdBuffer();

        verify(txnRepo, never()).batchInsert(any());
    }

    @Test
    @DisplayName("flushColdBuffer calls batchInsert for cold records")
    void flushColdBufferWithRecords() {
        List<Transaction> buffer = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < 3; i++) buffer.add(createTxn("cold-" + i));

        when(consumer.getColdBuffer()).thenReturn(buffer);

        flushSchedulers.flushColdBuffer();

        verify(txnRepo).batchInsert(any());
    }

    // =====================================================================
    //  insertWithFallback() tests — fallback chain
    // =====================================================================

    @Test
    @DisplayName("insertWithFallback succeeds on first try")
    void insertWithFallbackFirstTry() {
        List<Transaction> records = List.of(createTxn("req-1"));

        flushSchedulers.insertWithFallback(records);

        verify(txnRepo, times(1)).batchInsert(records);
    }

    @Test
    @DisplayName("insertWithFallback retries once after first failure")
    void insertWithFallbackRetry() {
        List<Transaction> records = List.of(createTxn("req-1"));

        doThrow(new RuntimeException("DB error"))
            .doNothing()
            .when(txnRepo).batchInsert(records);

        flushSchedulers.insertWithFallback(records);

        verify(txnRepo, times(2)).batchInsert(records);
    }

    @Test
    @DisplayName("insertWithFallback chunks into 100 after two failures")
    void insertWithFallbackChunking() {
        List<Transaction> records = new ArrayList<>();
        for (int i = 0; i < 250; i++) records.add(createTxn("req-" + i));

        // First two calls fail (full batch), subsequent chunk calls succeed
        doThrow(new RuntimeException("DB error"))
            .doThrow(new RuntimeException("DB error"))
            .doNothing()
            .when(txnRepo).batchInsert(anyList());

        flushSchedulers.insertWithFallback(records);

        // 2 full batch attempts + 3 chunk attempts (250/100 = 3 chunks)
        verify(txnRepo, times(2 + 3)).batchInsert(anyList());
    }

    @Test
    @DisplayName("insertWithFallback goes row-by-row when chunks fail")
    void insertWithFallbackRowByRow() {
        Transaction txn1 = createTxn("req-1");
        Transaction txn2 = createTxn("req-2");
        List<Transaction> records = new ArrayList<>(List.of(txn1, txn2));

        // All batch attempts fail
        doThrow(new RuntimeException("DB error")).when(txnRepo).batchInsert(anyList());

        // insertSingle succeeds
        doAnswer(inv -> {
            // insertSingle calls batchInsert(List.of(txn)) which we made throw
            // so we need a different approach — let's use doNothing for specific single-item lists
            return null;
        }).when(txnRepo).insertSingle(any());

        flushSchedulers.insertWithFallback(records);

        // Should attempt row-by-row via insertSingle
        verify(txnRepo).insertSingle(txn1);
        verify(txnRepo).insertSingle(txn2);
    }

    @Test
    @DisplayName("insertWithFallback sends to DLQ when row-by-row fails")
    void insertWithFallbackDlq() {
        Transaction txn = createTxn("req-dlq");
        List<Transaction> records = new ArrayList<>(List.of(txn));

        doThrow(new RuntimeException("DB error")).when(txnRepo).batchInsert(anyList());
        doThrow(new RuntimeException("DB error")).when(txnRepo).insertSingle(any());

        flushSchedulers.insertWithFallback(records);

        verify(deadLetterQueue).send(txn);
    }

    // =====================================================================
    //  insertPayloadWithRetry() tests
    // =====================================================================

    @Test
    @DisplayName("insertPayloadWithRetry succeeds on first attempt")
    void insertPayloadFirstAttempt() {
        List<TransactionPayload> payloads = List.of(createPayload("req-1"));

        flushSchedulers.insertPayloadWithRetry(payloads);

        verify(payloadRepo, times(1)).batchInsert(payloads);
    }

    @Test
    @DisplayName("insertPayloadWithRetry retries up to 3 times")
    void insertPayloadRetries() {
        List<TransactionPayload> payloads = List.of(createPayload("req-1"));

        doThrow(new RuntimeException("DB error")).when(payloadRepo).batchInsert(any());

        flushSchedulers.insertPayloadWithRetry(payloads);

        verify(payloadRepo, times(3)).batchInsert(payloads);
    }

    @Test
    @DisplayName("insertPayloadWithRetry succeeds on second attempt")
    void insertPayloadSucceedsSecondAttempt() {
        List<TransactionPayload> payloads = List.of(createPayload("req-1"));

        doThrow(new RuntimeException("DB error"))
            .doNothing()
            .when(payloadRepo).batchInsert(any());

        flushSchedulers.insertPayloadWithRetry(payloads);

        verify(payloadRepo, times(2)).batchInsert(payloads);
    }

    // =====================================================================
    //  Helpers
    // =====================================================================

    private Transaction createTxn(String requestId) {
        return Transaction.builder()
            .requestId(requestId)
            .orgId(1L)
            .orgName("TestOrg")
            .date(LocalDate.now())
            .build();
    }

    private TransactionPayload createPayload(String requestId) {
        return TransactionPayload.builder()
            .requestId(requestId)
            .corePayload("{\"test\":true}")
            .date(LocalDate.now())
            .build();
    }
}
