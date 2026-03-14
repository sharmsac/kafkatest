package com.platform.logging.deadletter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.platform.logging.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DeadLetterQueue {

    @Value("${kafka.topic.dead-letter:transaction-dead-letter}")
    private String dlqTopic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    public void send(Transaction txn) {
        try {
            String json = objectMapper.writeValueAsString(txn);
            kafkaTemplate.send(dlqTopic, txn.getRequestId(), json);
            log.warn("Sent to DLQ: {}", txn.getRequestId());
        } catch (Exception e) {
            log.error("Failed to send to DLQ: {}", txn.getRequestId(), e);
        }
    }
}
