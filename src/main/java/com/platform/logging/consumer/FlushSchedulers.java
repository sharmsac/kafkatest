package com.platform.logging.consumer;

import com.google.common.collect.Lists;
import com.platform.logging.deadletter.DeadLetterQueue;
import com.platform.logging.model.*;
import com.platform.logging.repository.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
@Slf4j
public class FlushSchedulers {

    @Autowired private TransactionAssemblyConsumer    consumer;
    @Autowired private TransactionRepository            txnRepo;
    @Autowired private TransactionPayloadRepository    payloadRepo;
    @Autowired private DeadLetterQueue                  deadLetterQueue;

    @Scheduled(fixedRate = 500)
    public void flushHotBuffer() {
        List<Transaction> snapshot = drain(consumer.getHotBuffer());
        if (snapshot.isEmpty()) return;

        insertWithFallback(snapshot);
        log.info("Hot flush: {} records", snapshot.size());
    }

    @Scheduled(fixedRate = 500)
    public void flushPayloadBuffer() {
        List<TransactionPayload> snapshot = drain(consumer.getPayloadBuffer());
        if (snapshot.isEmpty()) return;

        insertPayloadWithRetry(snapshot);
        log.info("Payload flush: {} records", snapshot.size());
    }

    @Scheduled(fixedRate = 30_000)
    public void flushColdBuffer() {
        List<Transaction> snapshot = drain(consumer.getColdBuffer());
        if (snapshot.isEmpty()) return;

        Lists.partition(snapshot, 5_000).forEach(this::insertWithFallback);
        log.info("Cold flush: {} records", snapshot.size());
    }

    void insertWithFallback(List<Transaction> records) {
        try {
            txnRepo.batchInsert(records);
            return;
        } catch (Exception e1) {
            log.warn("Batch insert failed ({} records), retrying...", records.size(), e1);
        }

        try {
            txnRepo.batchInsert(records);
            return;
        } catch (Exception e2) {
            log.warn("Retry failed, chunking into 100s...", e2);
        }

        List<Transaction> failures = new ArrayList<>();
        for (List<Transaction> chunk : Lists.partition(records, 100)) {
            try {
                txnRepo.batchInsert(chunk);
            } catch (Exception e3) {
                failures.addAll(chunk);
            }
        }

        if (failures.isEmpty()) return;

        log.warn("Chunked insert had {} failures, going row-by-row", failures.size());
        for (Transaction txn : failures) {
            try {
                txnRepo.insertSingle(txn);
            } catch (Exception e4) {
                log.error("Row insert failed for {}, sending to DLQ", txn.getRequestId(), e4);
                deadLetterQueue.send(txn);
            }
        }
    }

    void insertPayloadWithRetry(List<TransactionPayload> payloads) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                payloadRepo.batchInsert(payloads);
                return;
            } catch (Exception e) {
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
