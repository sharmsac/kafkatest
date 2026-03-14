package com.platform.logging.deadletter;

import com.platform.logging.model.Transaction;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DeadLetterQueueTest {

    @InjectMocks
    private DeadLetterQueue deadLetterQueue;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(deadLetterQueue, "dlqTopic", "test-dlq");
    }

    @Test
    @DisplayName("send serializes Transaction and publishes to DLQ topic")
    void sendPublishesToDlq() {
        Transaction txn = Transaction.builder()
            .requestId("req-dlq-1")
            .orgId(1L)
            .orgName("TestOrg")
            .date(LocalDate.of(2025, 6, 1))
            .statusCode(200)
            .build();

        deadLetterQueue.send(txn);

        verify(kafkaTemplate).send(eq("test-dlq"), eq("req-dlq-1"), contains("req-dlq-1"));
    }

    @Test
    @DisplayName("send handles serialization errors gracefully")
    void sendHandlesSerializationError() {
        // The ObjectMapper should handle Transaction fine, but let's test
        // the case where kafkaTemplate.send throws
        Transaction txn = Transaction.builder()
            .requestId("req-err")
            .date(LocalDate.now())
            .build();

        doThrow(new RuntimeException("Kafka down")).when(kafkaTemplate).send(anyString(), anyString(), anyString());

        // Should not throw
        assertDoesNotThrow(() -> deadLetterQueue.send(txn));
    }

    @Test
    @DisplayName("send uses requestId as Kafka key")
    void sendUsesRequestIdAsKey() {
        Transaction txn = Transaction.builder()
            .requestId("my-key-123")
            .date(LocalDate.now())
            .build();

        deadLetterQueue.send(txn);

        verify(kafkaTemplate).send(anyString(), eq("my-key-123"), anyString());
    }
}
