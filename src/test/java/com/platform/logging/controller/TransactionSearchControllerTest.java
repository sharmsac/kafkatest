package com.platform.logging.controller;

import com.platform.logging.cache.OrgCache;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TransactionSearchControllerTest {

    @InjectMocks
    private TransactionSearchController controller;

    @Mock
    private OrgCache orgCache;

    private JdbcTemplate jdbc;

    // Capture the SQL and params from queryForList calls
    private String capturedSql;
    private Object[] capturedParams;

    @BeforeEach
    void setUp() {
        capturedSql = null;
        capturedParams = null;

        // Use a custom answer-based mock for JdbcTemplate to handle varargs
        jdbc = mock(JdbcTemplate.class, (Answer<Object>) invocation -> {
            if ("queryForList".equals(invocation.getMethod().getName())) {
                Object[] args = invocation.getArguments();
                capturedSql = (String) args[0];
                if (args.length > 1) {
                    capturedParams = Arrays.copyOfRange(args, 1, args.length);
                }
                return Collections.emptyList();
            }
            return null;
        });

        // Inject the manually-created mock
        try {
            var field = TransactionSearchController.class.getDeclaredField("jdbc");
            field.setAccessible(true);
            field.set(controller, jdbc);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // =====================================================================
    //  /search endpoint
    // =====================================================================

    @Test
    @DisplayName("search returns 400 when org name is not found")
    void searchUnknownOrg() {
        when(orgCache.resolveNameToId("UnknownOrg")).thenReturn(null);

        ResponseEntity<?> response = controller.search(
            "UnknownOrg", "2025-06-01", "2025-06-07",
            null, null, null, null, null, null, 0, 50);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        @SuppressWarnings("unchecked")
        Map<String, String> body = (Map<String, String>) response.getBody();
        assertNotNull(body);
        assertTrue(body.get("error").contains("UnknownOrg"));
    }

    @Test
    @DisplayName("search builds correct base SQL with org_id and date range")
    void searchBaseSql() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        ResponseEntity<?> response = controller.search(
            "Acme", "2025-06-01", "2025-06-07",
            null, null, null, null, null, null, 0, 50);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(capturedSql);
        assertTrue(capturedSql.contains("org_id = ?"));
        assertTrue(capturedSql.contains("`date` BETWEEN ? AND ?"));
        assertEquals(42L, capturedParams[0]);
        assertEquals("2025-06-01", capturedParams[1]);
        assertEquals("2025-06-07", capturedParams[2]);
    }

    @Test
    @DisplayName("search adds serviceName filter when provided")
    void searchWithServiceName() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            "PaymentService", null, null, null, null, null, 0, 50);

        assertTrue(capturedSql.contains("service_name = ?"));
        assertEquals("PaymentService", capturedParams[3]);
    }

    @Test
    @DisplayName("search adds apiName filter when provided")
    void searchWithApiName() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            null, "processPayment", null, null, null, null, 0, 50);

        assertTrue(capturedSql.contains("api_name = ?"));
        assertEquals("processPayment", capturedParams[3]);
    }

    @Test
    @DisplayName("search adds duration range filters when provided")
    void searchWithDurationRange() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            null, null, 100, 5000, null, null, 0, 50);

        assertTrue(capturedSql.contains("total_duration_ms >= ?"));
        assertTrue(capturedSql.contains("total_duration_ms <= ?"));
        assertEquals(100, capturedParams[3]);
        assertEquals(5000, capturedParams[4]);
    }

    @Test
    @DisplayName("search adds statusCode filter when provided")
    void searchWithStatusCode() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            null, null, null, null, 500, null, 0, 50);

        assertTrue(capturedSql.contains("status_code = ?"));
        assertEquals(500, capturedParams[3]);
    }

    @Test
    @DisplayName("search adds errorsOnly filter when true")
    void searchErrorsOnly() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            null, null, null, null, null, true, 0, 50);

        assertTrue(capturedSql.contains("is_error = 1"));
    }

    @Test
    @DisplayName("search does not add errorsOnly filter when false")
    void searchNotErrorsOnly() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            null, null, null, null, null, false, 0, 50);

        assertFalse(capturedSql.contains("is_error = 1"));
    }

    @Test
    @DisplayName("search applies pagination correctly")
    void searchPagination() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            null, null, null, null, null, null, 2, 25);

        // Last two params are size=25 and offset=page*size=50
        assertEquals(25, capturedParams[capturedParams.length - 2]);
        assertEquals(50, capturedParams[capturedParams.length - 1]);
    }

    @Test
    @DisplayName("search orders by apg_request_time DESC")
    void searchOrdering() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            null, null, null, null, null, null, 0, 50);

        assertTrue(capturedSql.contains("ORDER BY apg_request_time DESC"));
    }

    @Test
    @DisplayName("search with all optional filters builds complete SQL")
    void searchAllFilters() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.search("Acme", "2025-06-01", "2025-06-07",
            "PayService", "pay", 100, 5000, 200, true, 1, 25);

        assertTrue(capturedSql.contains("service_name = ?"));
        assertTrue(capturedSql.contains("api_name = ?"));
        assertTrue(capturedSql.contains("total_duration_ms >= ?"));
        assertTrue(capturedSql.contains("total_duration_ms <= ?"));
        assertTrue(capturedSql.contains("status_code = ?"));
        assertTrue(capturedSql.contains("is_error = 1"));

        assertEquals(42L, capturedParams[0]);       // orgId
        assertEquals("2025-06-01", capturedParams[1]); // dateFrom
        assertEquals("2025-06-07", capturedParams[2]); // dateTo
        assertEquals("PayService", capturedParams[3]); // serviceName
        assertEquals("pay", capturedParams[4]);         // apiName
        assertEquals(100, capturedParams[5]);           // minDurationMs
        assertEquals(5000, capturedParams[6]);          // maxDurationMs
        assertEquals(200, capturedParams[7]);           // statusCode
        assertEquals(25, capturedParams[8]);            // size
        assertEquals(25, capturedParams[9]);            // offset = page * size
    }

    // =====================================================================
    //  /{requestId} endpoint
    // =====================================================================

    @Test
    @DisplayName("getByRequestId returns 404 when not found")
    void getByRequestIdNotFound() {
        ResponseEntity<?> response = controller.getByRequestId("req-nonexistent");
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    // =====================================================================
    //  /{requestId}/payload endpoint
    // =====================================================================

    @Test
    @DisplayName("getPayload returns 404 when not found")
    void getPayloadNotFound() {
        ResponseEntity<?> response = controller.getPayload("req-x");
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    // =====================================================================
    //  /archive/search endpoint
    // =====================================================================

    @Test
    @DisplayName("archiveSearch returns 400 when org unknown")
    void archiveSearchUnknownOrg() {
        when(orgCache.resolveNameToId("Unknown")).thenReturn(null);

        ResponseEntity<?> response = controller.archiveSearch(
            "Unknown", "2025-01-01", "2025-03-01", null, 0, 50);

        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    }

    @Test
    @DisplayName("archiveSearch builds correct query with month range")
    void archiveSearchSuccess() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        ResponseEntity<?> response = controller.archiveSearch(
            "Acme", "2025-01-01", "2025-03-01", null, 0, 50);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(capturedSql.contains("ARCHIVE_TRANSACTION"));
        assertTrue(capturedSql.contains("`month` BETWEEN ? AND ?"));
    }

    @Test
    @DisplayName("archiveSearch adds statusCode filter when provided")
    void archiveSearchWithStatusCode() {
        when(orgCache.resolveNameToId("Acme")).thenReturn(42L);

        controller.archiveSearch("Acme", "2025-01-01", "2025-03-01", 500, 0, 50);

        assertTrue(capturedSql.contains("status_code = ?"));
        assertEquals(500, capturedParams[3]);
    }
}
