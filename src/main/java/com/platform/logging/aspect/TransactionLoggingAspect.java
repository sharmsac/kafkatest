package com.platform.logging.aspect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.logging.annotation.LogTransaction;
import com.platform.logging.context.RequestContext;
import com.platform.logging.model.*;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.*;
import org.aspectj.lang.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
public class TransactionLoggingAspect {

    @Autowired
    private KafkaTemplate<String, TransactionEvent> kafkaTemplate;

    @Value("${kafka.topic.transactions:transaction-events}")
    private String topic;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // -- Timing advice: captures start/end around method execution --
    @Around("@annotation(logTransaction)")
    public Object around(ProceedingJoinPoint pjp,
                         LogTransaction logTransaction) throws Throwable {

        String requestId = RequestContext.getRequestId();
        Long   orgId     = RequestContext.getOrgId();
        String orgName   = RequestContext.getOrgName();
        long   startMs   = System.currentTimeMillis();

        Object result;
        int statusCode = 200;

        try {
            result = pjp.proceed();
        } catch (Exception e) {
            statusCode = 500;
            throw e;
        } finally {
            long endMs = System.currentTimeMillis();

            TransactionEvent event = TransactionEvent.builder()
                .requestId(requestId)
                .orgId(orgId)
                .orgName(orgName)
                .serviceName(logTransaction.serviceName())
                .apiName(logTransaction.apiName())
                .eventType(logTransaction.eventType())
                .startTime(startMs)
                .endTime(endMs)
                .statusCode(statusCode)
                .publishedAt(System.currentTimeMillis())
                .build();

            // KEY = requestId -> all events land on same Kafka partition
            kafkaTemplate.send(topic, requestId, event);
        }

        return result;
    }

    // -- Payload advice: captures ESPX / core / banking payloads after return --
    @AfterReturning(
        pointcut  = "@annotation(logTransaction)",
        returning = "result"
    )
    public void capturePayload(JoinPoint jp,
                               LogTransaction logTransaction,
                               Object result) {

        EventType type = logTransaction.eventType();

        if (type == EventType.ESPX_HEADER ||
            type == EventType.CORE_PAYLOAD  ||
            type == EventType.BANKING_PAYLOAD) {

            TransactionEvent event = TransactionEvent.builder()
                .requestId(RequestContext.getRequestId())
                .orgId(RequestContext.getOrgId())
                .orgName(RequestContext.getOrgName())
                .eventType(type)
                .data(serialize(result))
                .publishedAt(System.currentTimeMillis())
                .build();

            kafkaTemplate.send(topic, event.getRequestId(), event);
        }
    }

    private String serialize(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.warn("Failed to serialize payload", e);
            return "{}";
        }
    }
}
