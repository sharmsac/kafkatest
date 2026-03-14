package com.platform.logging.aspect;

import com.platform.logging.annotation.LogTransaction;
import com.platform.logging.context.RequestContext;
import com.platform.logging.model.*;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionLoggingAspectTest {

    @InjectMocks
    private TransactionLoggingAspect aspect;

    @Mock
    private KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    @Mock
    private ProceedingJoinPoint pjp;

    @Mock
    private LogTransaction logTransaction;

    @Mock
    private JoinPoint joinPoint;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(aspect, "topic", "test-topic");
    }

    @AfterEach
    void tearDown() {
        RequestContext.clear();
    }

    @Test
    @DisplayName("around publishes TransactionEvent to Kafka on success")
    void aroundSuccess() throws Throwable {
        RequestContext.set("req-123", 42L, "Acme");

        when(logTransaction.serviceName()).thenReturn("TestService");
        when(logTransaction.apiName()).thenReturn("testMethod");
        when(logTransaction.eventType()).thenReturn(EventType.API_CALL);
        when(pjp.proceed()).thenReturn("result");

        Object result = aspect.around(pjp, logTransaction);

        assertEquals("result", result);

        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(kafkaTemplate).send(eq("test-topic"), eq("req-123"), eventCaptor.capture());

        TransactionEvent event = eventCaptor.getValue();
        assertEquals("req-123", event.getRequestId());
        assertEquals(42L, event.getOrgId());
        assertEquals("Acme", event.getOrgName());
        assertEquals("TestService", event.getServiceName());
        assertEquals("testMethod", event.getApiName());
        assertEquals(EventType.API_CALL, event.getEventType());
        assertEquals(200, event.getStatusCode());
        assertNotNull(event.getStartTime());
        assertNotNull(event.getEndTime());
        assertTrue(event.getEndTime() >= event.getStartTime());
        assertTrue(event.getPublishedAt() > 0);
    }

    @Test
    @DisplayName("around publishes event with status 500 on exception")
    void aroundException() throws Throwable {
        RequestContext.set("req-err", 1L, "Org");

        when(logTransaction.serviceName()).thenReturn("Service");
        when(logTransaction.apiName()).thenReturn("method");
        when(logTransaction.eventType()).thenReturn(EventType.APG_REQUEST);
        when(pjp.proceed()).thenThrow(new RuntimeException("boom"));

        assertThrows(RuntimeException.class, () -> aspect.around(pjp, logTransaction));

        ArgumentCaptor<TransactionEvent> eventCaptor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(kafkaTemplate).send(eq("test-topic"), eq("req-err"), eventCaptor.capture());

        assertEquals(500, eventCaptor.getValue().getStatusCode());
    }

    @Test
    @DisplayName("around re-throws original exception")
    void aroundRethrows() throws Throwable {
        RequestContext.set("req-x", 1L, "Org");
        when(logTransaction.serviceName()).thenReturn("S");
        when(logTransaction.apiName()).thenReturn("m");
        when(logTransaction.eventType()).thenReturn(EventType.API_CALL);

        RuntimeException original = new RuntimeException("original");
        when(pjp.proceed()).thenThrow(original);

        RuntimeException thrown = assertThrows(RuntimeException.class,
            () -> aspect.around(pjp, logTransaction));
        assertSame(original, thrown);
    }

    @Test
    @DisplayName("around uses request_id from RequestContext as Kafka key")
    void aroundUsesRequestIdAsKey() throws Throwable {
        RequestContext.set("kafka-key-id", 1L, "Org");

        when(logTransaction.serviceName()).thenReturn("S");
        when(logTransaction.apiName()).thenReturn("m");
        when(logTransaction.eventType()).thenReturn(EventType.API_CALL);
        when(pjp.proceed()).thenReturn(null);

        aspect.around(pjp, logTransaction);

        verify(kafkaTemplate).send(anyString(), eq("kafka-key-id"), any());
    }

    @Test
    @DisplayName("around generates UUID when RequestContext has no requestId")
    void aroundNoRequestContext() throws Throwable {
        // RequestContext not set — getRequestId() generates UUID
        when(logTransaction.serviceName()).thenReturn("S");
        when(logTransaction.apiName()).thenReturn("m");
        when(logTransaction.eventType()).thenReturn(EventType.API_CALL);
        when(pjp.proceed()).thenReturn(null);

        aspect.around(pjp, logTransaction);

        verify(kafkaTemplate).send(anyString(), argThat(key -> {
            assertNotNull(key);
            assertTrue(key.length() > 0);
            return true;
        }), any());
    }

    @Test
    @DisplayName("capturePayload publishes CORE_PAYLOAD event")
    void capturePayloadCorePayload() {
        RequestContext.set("req-cp", 1L, "Org");

        when(logTransaction.eventType()).thenReturn(EventType.CORE_PAYLOAD);

        aspect.capturePayload(joinPoint, logTransaction, java.util.Map.of("key", "value"));

        verify(kafkaTemplate).send(eq("test-topic"), eq("req-cp"), argThat(event ->
            event.getEventType() == EventType.CORE_PAYLOAD &&
            event.getData() != null &&
            event.getData().contains("key")));
    }

    @Test
    @DisplayName("capturePayload publishes BANKING_PAYLOAD event")
    void capturePayloadBankingPayload() {
        RequestContext.set("req-bp", 1L, "Org");
        when(logTransaction.eventType()).thenReturn(EventType.BANKING_PAYLOAD);

        aspect.capturePayload(joinPoint, logTransaction, "banking data");

        verify(kafkaTemplate).send(eq("test-topic"), eq("req-bp"), argThat(event ->
            event.getEventType() == EventType.BANKING_PAYLOAD));
    }

    @Test
    @DisplayName("capturePayload publishes ESPX_HEADER event")
    void capturePayloadEspxHeader() {
        RequestContext.set("req-espx", 1L, "Org");
        when(logTransaction.eventType()).thenReturn(EventType.ESPX_HEADER);

        aspect.capturePayload(joinPoint, logTransaction, "espx header");

        verify(kafkaTemplate).send(eq("test-topic"), eq("req-espx"), argThat(event ->
            event.getEventType() == EventType.ESPX_HEADER));
    }

    @Test
    @DisplayName("capturePayload does NOT publish for non-payload event types")
    void capturePayloadIgnoresNonPayloadTypes() {
        when(logTransaction.eventType()).thenReturn(EventType.API_CALL);

        aspect.capturePayload(joinPoint, logTransaction, "result");

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("capturePayload does NOT publish for APG_REQUEST")
    void capturePayloadIgnoresApgRequest() {
        when(logTransaction.eventType()).thenReturn(EventType.APG_REQUEST);

        aspect.capturePayload(joinPoint, logTransaction, "result");

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }

}
