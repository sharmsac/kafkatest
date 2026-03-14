package com.platform.logging.consumer;

import com.google.common.collect.Lists;
import com.platform.logging.deadletter.DeadLetterQueue;
import com.platform.logging.model.*;
import com.platform.logging.repository.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class FlushSchedulers {

    @Autowired private TransactionAssemblyConsumer    consumer;
    @Autowired private TransactionRepository            txnRepo;
    @Autowired private TransactionPayloadRepository    payloadRepo;
    @Autowired private DeadLetterQueue                  deadLetterQueue;

    // -- Instrumentation counters --
    @Getter private final AtomicLong hotFlushCount     = new AtomicLong();
    @Getter private final AtomicLong hotFlushTimeMs    = new AtomicLong();
    @Getter private final AtomicLong hotFlushRecords   = new AtomicLong();
    @Getter private final AtomicLong coldFlushCount    = new AtomicLong();
    @Getter private final AtomicLong coldFlushTimeMs   = new AtomicLong();
    @Getter private final AtomicLong coldFlushRecords  = new AtomicLong();
    @Getter private final AtomicLong payloadFlushCount   = new AtomicLong();
    @Getter private final AtomicLong payloadFlushTimeMs  = new AtomicLong();
    @Getter private final AtomicLong payloadFlushRecords = new AtomicLong();
    @Getter private final AtomicLong batchRetryCount   = new AtomicLong();
    @Getter private final AtomicLong dlqCount          = new AtomicLong();
    // Latency tracking: sum of (flush_time - event_publish_time) for each record
    @Getter private final AtomicLong totalLatencyMs    = new AtomicLong();
    @Getter private final AtomicLong latencyCount      = new AtomicLong();
    @Getter private final AtomicLong maxLatencyMs      = new AtomicLong();
    @Getter private final AtomicLong minLatencyMs      = new AtomicLong(Long.MAX_VALUE);

    // ===============================================================
    //  HOT FLUSH -- every 500ms
    //  Completed records -> MySQL for near real-time UI visibility
    // ===============================================================
    @Scheduled(fixedRate = 500)
    public void flushHotBuffer() {
        List<Transaction> snapshot = drain(consumer.getHotBuffer());
        if (snapshot.isEmpty()) return;

        long start = System.currentTimeMillis();
        insertWithFallback(snapshot);
        long elapsed = System.currentTimeMillis() - start;

        hotFlushCount.incrementAndGet();
        hotFlushTimeMs.addAndGet(elapsed);
        hotFlushRecords.addAndGet(snapshot.size());

        // Track end-to-end latency: now - apgRequestTime (event creation time)
        long now = System.currentTimeMillis();
        for (Transaction txn : snapshot) {
            if (txn.getApgRequestTime() != null) {
                long latency = now - txn.getApgRequestTime();
                totalLatencyMs.addAndGet(latency);
                latencyCount.incrementAndGet();
                maxLatencyMs.updateAndGet(cur -> Math.max(cur, latency));
                minLatencyMs.updateAndGet(cur -> Math.min(cur, latency));
            }
        }
        log.debug("Hot flush: {} records", snapshot.size());
    }

    // ===============================================================
    //  PAYLOAD FLUSH -- every 500ms
    //  Error/slow-call payloads -> TRANSACTION_PAYLOAD
    // ===============================================================
    @Scheduled(fixedRate = 500)
    public void flushPayloadBuffer() {
        List<TransactionPayload> snapshot = drain(consumer.getPayloadBuffer());
        if (snapshot.isEmpty()) return;

        long start = System.currentTimeMillis();
        insertPayloadWithRetry(snapshot);
        long elapsed = System.currentTimeMillis() - start;

        payloadFlushCount.incrementAndGet();
        payloadFlushTimeMs.addAndGet(elapsed);
        payloadFlushRecords.addAndGet(snapshot.size());
        log.debug("Payload flush: {} records", snapshot.size());
    }

    // ===============================================================
    //  COLD FLUSH -- every 30s
    //  Stale/incomplete records from sweep
    // ===============================================================
    @Scheduled(fixedRate = 30_000)
    public void flushColdBuffer() {
        List<Transaction> snapshot = drain(consumer.getColdBuffer());
        if (snapshot.isEmpty()) return;

        long start = System.currentTimeMillis();
        Lists.partition(snapshot, 5_000).forEach(chunk -> {
            insertWithFallback(chunk);
        });
        long elapsed = System.currentTimeMillis() - start;

        coldFlushCount.incrementAndGet();
        coldFlushTimeMs.addAndGet(elapsed);
        coldFlushRecords.addAndGet(snapshot.size());
        log.debug("Cold flush: {} records", snapshot.size());
    }

    // ===============================================================
    //  INSERT WITH FALLBACK CHAIN
    //  1. Batch insert (full batch)
    //  2. Retry once
    //  3. Chunk into 100-record batches
    //  4. Row-by-row insert
    //  5. Dead letter queue
    // ===============================================================
    void insertWithFallback(List<Transaction> records) {
        // Attempt 1: full batch
        try {
            txnRepo.batchInsert(records);
            return;
        } catch (Exception e1) {
            log.warn("Batch insert failed ({} records), retrying...", records.size(), e1);
            batchRetryCount.incrementAndGet();
        }

        // Attempt 2: retry full batch once
        try {
            txnRepo.batchInsert(records);
            return;
        } catch (Exception e2) {
            log.warn("Retry failed, chunking into 100s...", e2);
            batchRetryCount.incrementAndGet();
        }

        // Attempt 3: chunk into 100
        List<Transaction> failures = new ArrayList<>();
        for (List<Transaction> chunk : Lists.partition(records, 100)) {
            try {
                txnRepo.batchInsert(chunk);
            } catch (Exception e3) {
                batchRetryCount.incrementAndGet();
                failures.addAll(chunk);
            }
        }

        if (failures.isEmpty()) return;

        // Attempt 4: row-by-row
        log.warn("Chunked insert had {} failures, going row-by-row", failures.size());
        for (Transaction txn : failures) {
            try {
                txnRepo.insertSingle(txn);
            } catch (Exception e4) {
                // Attempt 5: dead letter queue
                log.error("Row insert failed for {}, sending to DLQ", txn.getRequestId(), e4);
                deadLetterQueue.send(txn);
                dlqCount.incrementAndGet();
            }
        }
    }

    // ===============================================================
    //  INSERT PAYLOAD WITH RETRY (3 attempts then discard)
    // ===============================================================
    void insertPayloadWithRetry(List<TransactionPayload> payloads) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                payloadRepo.batchInsert(payloads);
                return;
            } catch (Exception e) {
                batchRetryCount.incrementAndGet();
                log.warn("Payload insert attempt {}/3 failed ({} records)",
                    attempt, payloads.size(), e);
                if (attempt == 3) {
                    log.error("Payload insert failed after 3 attempts, discarding {} records",
                        payloads.size());
                }
            }
        }
    }

    <T> List<T> drain(List<T> buffer) {
        if (buffer.isEmpty()) return Collections.emptyList();
        synchronized (buffer) {
            List<T> snapshot = new ArrayList<>(buffer);
            buffer.clear();
            return snapshot;
        }
    }
}
