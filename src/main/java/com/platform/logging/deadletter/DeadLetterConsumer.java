package com.platform.logging.deadletter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.platform.logging.model.Transaction;
import com.platform.logging.repository.TransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DeadLetterConsumer {

    @Autowired private TransactionRepository txnRepo;
    private final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    @KafkaListener(
        topics      = "${kafka.topic.dead-letter:transaction-dead-letter}",
        groupId     = "dlq-consumer-group",
        concurrency = "2"
    )
    public void consume(String json, Acknowledgment ack) {
        try {
            // Wait 5s before retrying -- gives MySQL time to recover
            Thread.sleep(5_000);

            Transaction txn = objectMapper.readValue(json, Transaction.class);
            txnRepo.insertSingle(txn);
            log.info("DLQ retry succeeded: {}", txn.getRequestId());
            ack.acknowledge();

        } catch (Exception e) {
            log.error("DLQ retry FAILED -- ALERT: record permanently lost. Payload: {}",
                json.substring(0, Math.min(200, json.length())), e);
            // ack to prevent infinite loop
            ack.acknowledge();
        }
    }
}
