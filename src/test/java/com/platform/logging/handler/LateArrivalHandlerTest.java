package com.platform.logging.handler;

import com.platform.logging.model.*;
import com.platform.logging.repository.TransactionPayloadRepository;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LateArrivalHandlerTest {

    @InjectMocks
    private LateArrivalHandler handler;

    @Mock
    private JdbcTemplate jdbc;

    @Mock
    private TransactionPayloadRepository payloadRepo;

    // =====================================================================
    //  handle() routing tests
    // =====================================================================

    @Test
    @DisplayName("handle ROUTER_REQUEST calls updateRouterTimes")
    void handleRouterRequest() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.ROUTER_REQUEST)
            .startTime(1100L)
            .endTime(null)
            .build();

        handler.handle(event);

        verify(jdbc).update(anyString(), eq(1100L), isNull(), isNull(), eq("req-1"));
    }

    @Test
    @DisplayName("handle ROUTER_RESPONSE calls updateRouterTimes")
    void handleRouterResponse() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.ROUTER_RESPONSE)
            .startTime(null)
            .endTime(1900L)
            .build();

        handler.handle(event);

        verify(jdbc).update(anyString(), isNull(), eq(1900L), isNull(), eq("req-1"));
    }

    @Test
    @DisplayName("handle APG_RESPONSE calls updateApgEnd")
    void handleApgResponse() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.APG_RESPONSE)
            .endTime(2000L)
            .build();

        handler.handle(event);

        verify(jdbc).update(anyString(), eq(2000L), eq(2000L), eq("req-1"));
    }

    @Test
    @DisplayName("handle APG_RESPONSE with null endTime does nothing")
    void handleApgResponseNullEnd() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.APG_RESPONSE)
            .endTime(null)
            .build();

        handler.handle(event);

        verify(jdbc, never()).update(anyString(), any(), any(), any());
    }

    @Test
    @DisplayName("handle CORE_PAYLOAD delegates to handleLatePayload")
    void handleCorePayload() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.CORE_PAYLOAD)
            .data("{\"core\":true}")
            .build();

        handler.handle(event);

        verify(payloadRepo).upsertPayload("req-1", "CORE_PAYLOAD", "{\"core\":true}");
    }

    @Test
    @DisplayName("handle BANKING_PAYLOAD delegates to handleLatePayload")
    void handleBankingPayload() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.BANKING_PAYLOAD)
            .data("{\"bank\":true}")
            .build();

        handler.handle(event);

        verify(payloadRepo).upsertPayload("req-1", "BANKING_PAYLOAD", "{\"bank\":true}");
    }

    @Test
    @DisplayName("handle ESPX_HEADER delegates to handleLatePayload")
    void handleEspxHeader() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.ESPX_HEADER)
            .data("{\"esp\":true}")
            .build();

        handler.handle(event);

        verify(payloadRepo).upsertPayload("req-1", "ESPX_HEADER", "{\"esp\":true}");
    }

    @Test
    @DisplayName("handle APG_REQUEST is ignored (logged as debug)")
    void handleApgRequestIgnored() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.APG_REQUEST)
            .startTime(1000L)
            .build();

        handler.handle(event);

        verifyNoInteractions(jdbc);
        verify(payloadRepo, never()).upsertPayload(any(), any(), any());
    }

    @Test
    @DisplayName("handle API_CALL is ignored (logged as debug)")
    void handleApiCallIgnored() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId("req-1")
            .eventType(EventType.API_CALL)
            .startTime(1000L)
            .endTime(2000L)
            .build();

        handler.handle(event);

        verifyNoInteractions(jdbc);
        verify(payloadRepo, never()).upsertPayload(any(), any(), any());
    }

    // =====================================================================
    //  updateRouterTimes() tests
    // =====================================================================

    @Test
    @DisplayName("updateRouterTimes computes duration when both start and end provided")
    void updateRouterTimesWithDuration() {
        handler.updateRouterTimes("req-1", 1100L, 1900L);

        verify(jdbc).update(anyString(), eq(1100L), eq(1900L), eq(800), eq("req-1"));
    }

    @Test
    @DisplayName("updateRouterTimes passes null duration when start is null")
    void updateRouterTimesNullStart() {
        handler.updateRouterTimes("req-1", null, 1900L);

        verify(jdbc).update(anyString(), isNull(), eq(1900L), isNull(), eq("req-1"));
    }

    @Test
    @DisplayName("updateRouterTimes passes null duration when end is null")
    void updateRouterTimesNullEnd() {
        handler.updateRouterTimes("req-1", 1100L, null);

        verify(jdbc).update(anyString(), eq(1100L), isNull(), isNull(), eq("req-1"));
    }
}
