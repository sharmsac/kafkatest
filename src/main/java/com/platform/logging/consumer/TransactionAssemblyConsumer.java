package com.platform.logging.consumer;

import com.platform.logging.model.*;
import com.platform.logging.handler.LateArrivalHandler;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

@Service
@Slf4j
@Getter
public class TransactionAssemblyConsumer {

    private static final int MAX_ASSEMBLY_BUFFER = 500_000;
    private static final int MAX_COLD_BUFFER     = 50_000;

    // -- Assembly: requestId:orgId -> partial record --
    private final ConcurrentHashMap<String, PartialTransaction> assemblyBuffer
        = new ConcurrentHashMap<>();

    // -- Hot buffer: completed records -> flush every 500ms --
    private final List<Transaction> hotBuffer
        = Collections.synchronizedList(new ArrayList<>());

    // -- Cold buffer: stale/incomplete -> flush every 30s --
    private final List<Transaction> coldBuffer
        = Collections.synchronizedList(new ArrayList<>());

    // -- Payload buffer: payloads for errors/slow calls -> flush every 500ms --
    private final List<TransactionPayload> payloadBuffer
        = Collections.synchronizedList(new ArrayList<>());

    // -- Recently flushed IDs: late arrivals within 30s trigger UPDATE --
    static final long FLUSHED_TTL_MS = 30_000;
    private final ConcurrentHashMap<String, Long> flushedIds = new ConcurrentHashMap<>();

    private final LateArrivalHandler lateArrivalHandler;
    private final KafkaListenerEndpointRegistry registry;

    public TransactionAssemblyConsumer(LateArrivalHandler lateArrivalHandler,
                                       KafkaListenerEndpointRegistry registry) {
        this.lateArrivalHandler = lateArrivalHandler;
        this.registry = registry;
    }

    // ===============================================================
    //  Main consumer loop
    // ===============================================================
    @KafkaListener(
        topics      = "${kafka.topic.transactions:transaction-events}",
        groupId     = "logging-consumer-group",
        concurrency = "10"
    )
    public void consume(TransactionEvent event, Acknowledgment ack) {
        String bufferKey = event.getRequestId() + ":" + event.getOrgId();

        // 1. Late arrival -- record already flushed to MySQL
        if (flushedIds.containsKey(bufferKey)) {
            lateArrivalHandler.handle(event);
            ack.acknowledge();
            return;
        }

        // 2. Backpressure -- cold buffer full, pause consumption
        if (coldBuffer.size() >= MAX_COLD_BUFFER) {
            pauseAndResume();
            return;  // don't ack -> Kafka will redeliver
        }

        // 3. Evict oldest if assembly buffer is too large
        if (assemblyBuffer.size() >= MAX_ASSEMBLY_BUFFER) {
            evictOldest(MAX_ASSEMBLY_BUFFER / 10);
        }

        // 4. Normal assembly
        PartialTransaction partial = assemblyBuffer
            .computeIfAbsent(bufferKey, k -> new PartialTransaction(event.getRequestId()));
        partial.merge(event);

        // 5. Minimum fields met -> move to hot buffer
        if (partial.hasMinimumFields()) {
            assemblyBuffer.remove(bufferKey);
            hotBuffer.add(partial.toTransaction());

            if (partial.shouldWritePayload()) {
                payloadBuffer.add(partial.toPayload());
            }

            flushedIds.put(bufferKey, System.currentTimeMillis());
        }

        ack.acknowledge();
    }

    // -- Backpressure: pause all listeners, resume after 1s if buffer drains --
    private void pauseAndResume() {
        log.warn("Cold buffer full ({}) -- pausing Kafka consumption", coldBuffer.size());
        registry.getListenerContainers().forEach(c -> c.pause());

        CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS).execute(() -> {
            if (coldBuffer.size() < MAX_COLD_BUFFER / 2) {
                registry.getListenerContainers().forEach(c -> c.resume());
                log.info("Kafka consumption resumed");
            }
        });
    }

    // -- Evict oldest assembly records to cold buffer --
    public void evictOldest(int count) {
        assemblyBuffer.entrySet().stream()
            .sorted(Comparator.comparing(e -> e.getValue().getCreatedAt()))
            .limit(count)
            .forEach(e -> {
                Transaction txn = e.getValue().toTransaction();
                txn.setIncomplete(true);
                coldBuffer.add(txn);
                assemblyBuffer.remove(e.getKey());
                flushedIds.put(e.getKey(), System.currentTimeMillis());
            });
        log.warn("Evicted {} oldest assembly records to cold buffer", count);
    }
}
