package com.platform.logging.consumer;

import com.platform.logging.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SweepScheduler {

    private static final int ASSEMBLY_TTL_SECONDS = 5;

    @Autowired private TransactionAssemblyConsumer consumer;

    // -- SWEEP: every 3s -- evict stale records whose events never all arrived --
    @Scheduled(fixedRate = 3_000)
    public void sweepStaleRecords() {
        consumer.getAssemblyBuffer().entrySet().removeIf(entry -> {
            PartialTransaction partial = entry.getValue();

            if (partial.isStale(ASSEMBLY_TTL_SECONDS)) {
                Transaction txn = partial.toTransaction();
                txn.setIncomplete(true);
                consumer.getColdBuffer().add(txn);

                if (partial.shouldWritePayload()) {
                    consumer.getPayloadBuffer().add(partial.toPayload());
                }

                consumer.getFlushedIds().put(entry.getKey(), true);
                return true;
            }
            return false;
        });
    }

    // -- METRICS: every 60s -- log buffer sizes for monitoring/alerting --
    @Scheduled(fixedRate = 60_000)
    public void logBufferMetrics() {
        log.info("Buffer metrics -- assembly: {}, hot: {}, cold: {}, payload: {}, flushedIds: {}",
            consumer.getAssemblyBuffer().size(),
            consumer.getHotBuffer().size(),
            consumer.getColdBuffer().size(),
            consumer.getPayloadBuffer().size(),
            consumer.getFlushedIds().size()
        );
    }
}
