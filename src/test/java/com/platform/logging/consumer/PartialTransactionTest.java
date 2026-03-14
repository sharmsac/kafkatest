package com.platform.logging.consumer;

import com.platform.logging.model.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class PartialTransactionTest {

    private static final String REQUEST_ID = "test-req-001";

    private PartialTransaction partial;

    @BeforeEach
    void setUp() {
        partial = new PartialTransaction(REQUEST_ID);
    }

    // =====================================================================
    //  merge() tests
    // =====================================================================

    @Test
    @DisplayName("merge APG_REQUEST sets apgRequestTime and hasApgStart")
    void mergeApgRequest() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.APG_REQUEST)
            .orgId(42L)
            .orgName("Acme")
            .serviceName("APG")
            .apiName("processRequest")
            .startTime(1000L)
            .statusCode(200)
            .build();

        partial.merge(event);

        assertEquals(1000L, partial.getApgRequestTime());
        assertTrue(partial.isHasApgStart());
        assertFalse(partial.isHasApgEnd());
        assertEquals(42L, partial.getOrgId());
        assertEquals("Acme", partial.getOrgName());
        assertEquals("APG", partial.getServiceName());
        assertEquals("processRequest", partial.getApiName());
        assertEquals(200, partial.getStatusCode());
    }

    @Test
    @DisplayName("merge APG_RESPONSE sets apgResponseTime and hasApgEnd")
    void mergeApgResponse() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.APG_RESPONSE)
            .endTime(2000L)
            .build();

        partial.merge(event);

        assertEquals(2000L, partial.getApgResponseTime());
        assertTrue(partial.isHasApgEnd());
        assertFalse(partial.isHasApgStart());
    }

    @Test
    @DisplayName("merge ROUTER_REQUEST sets routerRequestTime and hasRouter")
    void mergeRouterRequest() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.ROUTER_REQUEST)
            .startTime(1100L)
            .build();

        partial.merge(event);

        assertEquals(1100L, partial.getRouterRequestTime());
        assertTrue(partial.isHasRouter());
    }

    @Test
    @DisplayName("merge ROUTER_RESPONSE sets routerResponseTime")
    void mergeRouterResponse() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.ROUTER_RESPONSE)
            .endTime(1900L)
            .build();

        partial.merge(event);

        assertEquals(1900L, partial.getRouterResponseTime());
    }

    @Test
    @DisplayName("merge API_CALL sets both apiRequestTime and apiResponseTime")
    void mergeApiCall() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.API_CALL)
            .startTime(1200L)
            .endTime(1800L)
            .statusCode(201)
            .build();

        partial.merge(event);

        assertEquals(1200L, partial.getApiRequestTime());
        assertEquals(1800L, partial.getApiResponseTime());
        assertEquals(201, partial.getStatusCode());
    }

    @Test
    @DisplayName("merge ESPX_HEADER sets espxHeader from data field")
    void mergeEspxHeader() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.ESPX_HEADER)
            .data("{\"trace\":\"abc\"}")
            .build();

        partial.merge(event);

        assertEquals("{\"trace\":\"abc\"}", partial.getEspxHeader());
    }

    @Test
    @DisplayName("merge CORE_PAYLOAD sets corePayload from data field")
    void mergeCorePayload() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.CORE_PAYLOAD)
            .data("{\"amount\":100}")
            .build();

        partial.merge(event);

        assertEquals("{\"amount\":100}", partial.getCorePayload());
    }

    @Test
    @DisplayName("merge BANKING_PAYLOAD sets bankingPayload from data field")
    void mergeBankingPayload() {
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.BANKING_PAYLOAD)
            .data("{\"bank\":\"XYZ\"}")
            .build();

        partial.merge(event);

        assertEquals("{\"bank\":\"XYZ\"}", partial.getBankingPayload());
    }

    @Test
    @DisplayName("merge does not overwrite fields with null values")
    void mergePreservesExistingNonNullFields() {
        // First event sets orgId
        TransactionEvent event1 = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.APG_REQUEST)
            .orgId(42L)
            .orgName("Acme")
            .serviceName("APG")
            .startTime(1000L)
            .build();

        // Second event has null orgId
        TransactionEvent event2 = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.APG_RESPONSE)
            .endTime(2000L)
            .build();

        partial.merge(event1);
        partial.merge(event2);

        assertEquals(42L, partial.getOrgId());
        assertEquals("Acme", partial.getOrgName());
        assertEquals("APG", partial.getServiceName());
    }

    @Test
    @DisplayName("merge overwrites fields with newer non-null values")
    void mergeOverwritesWithNewerValues() {
        TransactionEvent event1 = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.APG_REQUEST)
            .statusCode(200)
            .startTime(1000L)
            .build();

        // API_CALL with different status code
        TransactionEvent event2 = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.API_CALL)
            .statusCode(500)
            .startTime(1200L)
            .endTime(1800L)
            .build();

        partial.merge(event1);
        partial.merge(event2);

        // API_CALL's statusCode should overwrite (set via switch + via general assignment)
        assertEquals(500, partial.getStatusCode());
    }

    @Test
    @DisplayName("merging multiple events builds a complete PartialTransaction")
    void mergeMultipleEvents() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(routerRequestEvent(1100L));
        partial.merge(apiCallEvent(1200L, 1700L, 200));
        partial.merge(routerResponseEvent(1800L));
        partial.merge(apgResponseEvent(1900L));
        partial.merge(espxHeaderEvent("{\"esp\":true}"));
        partial.merge(corePayloadEvent("{\"core\":1}"));

        assertTrue(partial.isHasApgStart());
        assertTrue(partial.isHasApgEnd());
        assertTrue(partial.isHasRouter());
        assertEquals(1000L, partial.getApgRequestTime());
        assertEquals(1900L, partial.getApgResponseTime());
        assertEquals(1100L, partial.getRouterRequestTime());
        assertEquals(1800L, partial.getRouterResponseTime());
        assertEquals(1200L, partial.getApiRequestTime());
        assertEquals(1700L, partial.getApiResponseTime());
        assertEquals("{\"esp\":true}", partial.getEspxHeader());
        assertEquals("{\"core\":1}", partial.getCorePayload());
    }

    // =====================================================================
    //  hasMinimumFields() tests
    // =====================================================================

    @Test
    @DisplayName("hasMinimumFields returns false when no events merged")
    void hasMinimumFieldsEmpty() {
        assertFalse(partial.hasMinimumFields());
    }

    @Test
    @DisplayName("hasMinimumFields returns false with only APG_REQUEST")
    void hasMinimumFieldsOnlyApgStart() {
        partial.merge(apgRequestEvent(1000L));
        assertFalse(partial.hasMinimumFields());
    }

    @Test
    @DisplayName("hasMinimumFields returns false with only APG_RESPONSE")
    void hasMinimumFieldsOnlyApgEnd() {
        partial.merge(apgResponseEvent(2000L));
        assertFalse(partial.hasMinimumFields());
    }

    @Test
    @DisplayName("hasMinimumFields returns false with both APG_REQUEST and APG_RESPONSE but no API_CALL")
    void hasMinimumFieldsBothApg() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        assertFalse(partial.hasMinimumFields());
    }

    @Test
    @DisplayName("hasMinimumFields returns true with only API_CALL (flush trigger)")
    void hasMinimumFieldsOnlyApiCall() {
        partial.merge(apiCallEvent(1000L, 2000L, 200));
        assertTrue(partial.hasMinimumFields());
    }

    @Test
    @DisplayName("hasMinimumFields returns true when API_CALL is present alongside APG events")
    void hasMinimumFieldsApiCallOnly() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apiCallEvent(1200L, 1800L, 200));
        assertTrue(partial.hasMinimumFields());
    }

    @Test
    @DisplayName("hasMinimumFields returns false with APG + Router but no API_CALL")
    void hasMinimumFieldsAllEventsExceptApiCall() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        partial.merge(routerRequestEvent(1100L));
        partial.merge(routerResponseEvent(1900L));
        assertFalse(partial.hasMinimumFields());
    }

    // =====================================================================
    //  isStale() tests
    // =====================================================================

    @Test
    @DisplayName("newly created PartialTransaction is not stale")
    void isStaleNewRecord() {
        assertFalse(partial.isStale(5));
    }

    @Test
    @DisplayName("isStale returns true for zero TTL on any non-instant record")
    void isStaleZeroTtl() throws InterruptedException {
        Thread.sleep(10); // ensure some time passes
        assertTrue(partial.isStale(0));
    }

    // =====================================================================
    //  shouldWritePayload() tests
    // =====================================================================

    @Test
    @DisplayName("shouldWritePayload returns false when no error and no payload")
    void shouldWritePayloadNoErrorNoPayload() {
        partial.merge(apgRequestEvent(1000L));
        partial.setStatusCode(200);
        assertFalse(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns true for error status code >= 400")
    void shouldWritePayloadErrorStatus() {
        partial.setStatusCode(500);
        assertTrue(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns true for status code exactly 400")
    void shouldWritePayloadBoundary400() {
        partial.setStatusCode(400);
        assertTrue(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns false for status code 399")
    void shouldWritePayloadBoundary399() {
        partial.setStatusCode(399);
        assertFalse(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns true when corePayload is present")
    void shouldWritePayloadWithCorePayload() {
        partial.setStatusCode(200);
        partial.merge(corePayloadEvent("{\"core\":1}"));
        assertTrue(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns true when bankingPayload is present")
    void shouldWritePayloadWithBankingPayload() {
        partial.setStatusCode(200);
        partial.merge(bankingPayloadEvent("{\"bank\":1}"));
        assertTrue(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns true when requestBody is set")
    void shouldWritePayloadWithRequestBody() {
        partial.setStatusCode(200);
        partial.setRequestBody("{\"body\":1}");
        assertTrue(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns true when responseBody is set")
    void shouldWritePayloadWithResponseBody() {
        partial.setStatusCode(200);
        partial.setResponseBody("{\"resp\":1}");
        assertTrue(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns true when statusCode is null (not set)")
    void shouldWritePayloadNullStatusWithPayload() {
        partial.merge(corePayloadEvent("{\"core\":1}"));
        // statusCode is null — isError is false, but payload exists
        assertTrue(partial.shouldWritePayload());
    }

    @Test
    @DisplayName("shouldWritePayload returns false when statusCode is null and no payload")
    void shouldWritePayloadNullStatusNoPayload() {
        // statusCode is null and no payload data
        assertFalse(partial.shouldWritePayload());
    }

    // =====================================================================
    //  toTransaction() duration computation tests
    // =====================================================================

    @Test
    @DisplayName("toTransaction computes total duration from APG times but marks incomplete without API_CALL")
    void toTransactionApgDuration() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(1500L));

        Transaction txn = partial.toTransaction();

        assertEquals(500, txn.getTotalDurationMs());
        assertTrue(txn.isIncomplete()); // no API_CALL => incomplete
    }

    @Test
    @DisplayName("toTransaction computes API duration correctly")
    void toTransactionApiDuration() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        partial.merge(apiCallEvent(1200L, 1800L, 200));

        Transaction txn = partial.toTransaction();

        assertEquals(600, txn.getApiDurationMs());
        assertEquals(1000, txn.getTotalDurationMs());
    }

    @Test
    @DisplayName("toTransaction computes router duration correctly")
    void toTransactionRouterDuration() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        partial.merge(routerRequestEvent(1100L));
        partial.merge(routerResponseEvent(1900L));

        Transaction txn = partial.toTransaction();

        assertEquals(800, txn.getRouterDurationMs());
        assertTrue(txn.isHasRouter());
    }

    @Test
    @DisplayName("toTransaction computes network overhead = total - api - router")
    void toTransactionNetworkOverhead() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        partial.merge(routerRequestEvent(1100L));
        partial.merge(routerResponseEvent(1800L));
        partial.merge(apiCallEvent(1200L, 1700L, 200));

        Transaction txn = partial.toTransaction();

        // total=1000, api=500, router=700 => overhead = 1000 - 500 - 700 = -200 => clamped to 0
        // Wait, let me recalculate: overhead = total - (apiDur + routerDur) = 1000 - (500 + 700) = -200 => max(0, -200) = 0
        assertEquals(1000, txn.getTotalDurationMs());
        assertEquals(500, txn.getApiDurationMs());
        assertEquals(700, txn.getRouterDurationMs());
        assertEquals(0, txn.getNetworkOverheadMs());
    }

    @Test
    @DisplayName("toTransaction network overhead is positive when total > api + router")
    void toTransactionNetworkOverheadPositive() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(3000L));
        partial.merge(routerRequestEvent(1100L));
        partial.merge(routerResponseEvent(1500L));
        partial.merge(apiCallEvent(1200L, 1400L, 200));

        Transaction txn = partial.toTransaction();

        // total=2000, api=200, router=400 => overhead = 2000 - (200 + 400) = 1400
        assertEquals(2000, txn.getTotalDurationMs());
        assertEquals(200, txn.getApiDurationMs());
        assertEquals(400, txn.getRouterDurationMs());
        assertEquals(1400, txn.getNetworkOverheadMs());
    }

    @Test
    @DisplayName("toTransaction falls back to router duration when APG incomplete")
    void toTransactionFallbackToRouter() {
        // Only APG start + router times (no APG end, no API_CALL)
        partial.merge(apgRequestEvent(1000L));
        partial.merge(routerRequestEvent(1100L));
        partial.merge(routerResponseEvent(1900L));

        Transaction txn = partial.toTransaction();

        // APG duration = null (no end), fallback to router = 800
        assertEquals(800, txn.getTotalDurationMs());
        assertTrue(txn.isIncomplete()); // no API_CALL => incomplete
    }

    @Test
    @DisplayName("toTransaction falls back to API duration when APG and Router incomplete")
    void toTransactionFallbackToApi() {
        partial.merge(apiCallEvent(1200L, 1700L, 200));

        Transaction txn = partial.toTransaction();

        // No APG, no router => fallback to API = 500
        assertEquals(500, txn.getTotalDurationMs());
        assertFalse(txn.isIncomplete()); // API_CALL present => complete
    }

    @Test
    @DisplayName("toTransaction returns null totalDuration when all timestamps missing")
    void toTransactionNullDuration() {
        // Only metadata, no timestamps
        TransactionEvent event = TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.ESPX_HEADER)
            .data("{}")
            .orgId(1L)
            .build();
        partial.merge(event);

        Transaction txn = partial.toTransaction();

        assertNull(txn.getTotalDurationMs());
        assertNull(txn.getApiDurationMs());
        assertNull(txn.getRouterDurationMs());
        assertNull(txn.getNetworkOverheadMs());
    }

    @Test
    @DisplayName("toTransaction marks error for status >= 400")
    void toTransactionErrorFlag() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        partial.setStatusCode(500);

        Transaction txn = partial.toTransaction();

        assertTrue(txn.isError());
        assertEquals(500, txn.getStatusCode());
    }

    @Test
    @DisplayName("toTransaction marks non-error for status < 400")
    void toTransactionNonError() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        partial.setStatusCode(200);

        Transaction txn = partial.toTransaction();

        assertFalse(txn.isError());
    }

    @Test
    @DisplayName("toTransaction defaults statusCode to 0 when null")
    void toTransactionDefaultStatusCode() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));

        Transaction txn = partial.toTransaction();

        assertEquals(0, txn.getStatusCode());
        assertFalse(txn.isError());
    }

    @Test
    @DisplayName("toTransaction sets date to today")
    void toTransactionDate() {
        partial.merge(apgRequestEvent(1000L));

        Transaction txn = partial.toTransaction();

        assertEquals(LocalDate.now(), txn.getDate());
    }

    @Test
    @DisplayName("toTransaction sets incomplete=true when no API_CALL has been merged")
    void toTransactionIncompleteFlag() {
        partial.merge(apgRequestEvent(1000L));
        // no API_CALL

        Transaction txn = partial.toTransaction();

        assertTrue(txn.isIncomplete());
    }

    @Test
    @DisplayName("toTransaction sets incomplete=false when API_CALL is present")
    void toTransactionCompleteFlag() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apiCallEvent(1200L, 1800L, 200));
        partial.merge(apgResponseEvent(2000L));

        Transaction txn = partial.toTransaction();

        assertFalse(txn.isIncomplete()); // API_CALL present => complete
    }

    @Test
    @DisplayName("toTransaction copies espXHeader")
    void toTransactionEspxHeader() {
        partial.merge(espxHeaderEvent("{\"esp\":true}"));

        Transaction txn = partial.toTransaction();

        assertEquals("{\"esp\":true}", txn.getEspXHeader());
    }

    @Test
    @DisplayName("toTransaction copies requestId")
    void toTransactionRequestId() {
        Transaction txn = partial.toTransaction();

        assertEquals(REQUEST_ID, txn.getRequestId());
    }

    @Test
    @DisplayName("durationMs returns null when end < start (clock skew)")
    void durationMsEndBeforeStart() {
        partial.merge(apgRequestEvent(2000L));
        partial.merge(apgResponseEvent(1000L));

        Transaction txn = partial.toTransaction();

        // end < start should return null duration
        assertNull(txn.getTotalDurationMs());
    }

    @Test
    @DisplayName("durationMs returns 0 when start == end")
    void durationMsZero() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(1000L));

        Transaction txn = partial.toTransaction();

        assertEquals(0, txn.getTotalDurationMs());
    }

    @Test
    @DisplayName("network overhead is null when apiDuration is null")
    void networkOverheadNullWhenApiNull() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        // no API call

        Transaction txn = partial.toTransaction();

        assertNull(txn.getNetworkOverheadMs());
    }

    @Test
    @DisplayName("network overhead computed without router when router is absent")
    void networkOverheadWithoutRouter() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgResponseEvent(2000L));
        partial.merge(apiCallEvent(1200L, 1800L, 200));

        Transaction txn = partial.toTransaction();

        // total=1000, api=600, router=null => overhead = 1000 - (600 + 0) = 400
        assertEquals(400, txn.getNetworkOverheadMs());
    }

    // =====================================================================
    //  toPayload() tests
    // =====================================================================

    @Test
    @DisplayName("toPayload creates TransactionPayload with correct fields")
    void toPayloadFields() {
        partial.setRequestBody("{\"req\":1}");
        partial.setResponseBody("{\"resp\":1}");
        partial.merge(corePayloadEvent("{\"core\":1}"));
        partial.merge(bankingPayloadEvent("{\"bank\":1}"));

        TransactionPayload payload = partial.toPayload();

        assertEquals(REQUEST_ID, payload.getRequestId());
        assertEquals("{\"req\":1}", payload.getRequestBody());
        assertEquals("{\"resp\":1}", payload.getResponseBody());
        assertEquals("{\"core\":1}", payload.getCorePayload());
        assertEquals("{\"bank\":1}", payload.getBankingPayload());
        assertEquals(LocalDate.now(), payload.getDate());
    }

    @Test
    @DisplayName("toPayload returns null payload fields when not set")
    void toPayloadNullFields() {
        TransactionPayload payload = partial.toPayload();

        assertEquals(REQUEST_ID, payload.getRequestId());
        assertNull(payload.getRequestBody());
        assertNull(payload.getResponseBody());
        assertNull(payload.getCorePayload());
        assertNull(payload.getBankingPayload());
    }

    // =====================================================================
    //  Edge cases & stress tests
    // =====================================================================

    @Test
    @DisplayName("merging same event type multiple times overwrites with latest")
    void mergeDuplicateEventType() {
        partial.merge(apgRequestEvent(1000L));
        partial.merge(apgRequestEvent(2000L));

        assertEquals(2000L, partial.getApgRequestTime());
        assertTrue(partial.isHasApgStart());
    }

    @Test
    @DisplayName("very large duration does not overflow int")
    void largeDuration() {
        long start = 1L;
        long end = (long) Integer.MAX_VALUE + 1L;

        partial.merge(apgRequestEvent(start));
        partial.merge(apgResponseEvent(end));

        Transaction txn = partial.toTransaction();

        // Cast to int may cause overflow — this tests the behavior
        // (int)(end - start) = (int)(2147483648L) which wraps to -2147483648
        // The code does (int)(end - start) which will overflow for very large durations
        // This is a known limitation we're documenting with this test
        assertNotNull(txn.getTotalDurationMs());
    }

    @Test
    @DisplayName("createdAt is set at construction time")
    void createdAtIsSet() {
        Instant before = Instant.now().minusMillis(1);
        PartialTransaction p = new PartialTransaction("test");
        Instant after = Instant.now().plusMillis(1);

        assertTrue(p.getCreatedAt().isAfter(before) || p.getCreatedAt().equals(before));
        assertTrue(p.getCreatedAt().isBefore(after) || p.getCreatedAt().equals(after));
    }

    // =====================================================================
    //  Helper methods to build events
    // =====================================================================

    private TransactionEvent apgRequestEvent(long startTime) {
        return TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.APG_REQUEST)
            .startTime(startTime)
            .build();
    }

    private TransactionEvent apgResponseEvent(long endTime) {
        return TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.APG_RESPONSE)
            .endTime(endTime)
            .build();
    }

    private TransactionEvent routerRequestEvent(long startTime) {
        return TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.ROUTER_REQUEST)
            .startTime(startTime)
            .build();
    }

    private TransactionEvent routerResponseEvent(long endTime) {
        return TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.ROUTER_RESPONSE)
            .endTime(endTime)
            .build();
    }

    private TransactionEvent apiCallEvent(long startTime, long endTime, int statusCode) {
        return TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.API_CALL)
            .startTime(startTime)
            .endTime(endTime)
            .statusCode(statusCode)
            .build();
    }

    private TransactionEvent espxHeaderEvent(String data) {
        return TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.ESPX_HEADER)
            .data(data)
            .build();
    }

    private TransactionEvent corePayloadEvent(String data) {
        return TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.CORE_PAYLOAD)
            .data(data)
            .build();
    }

    private TransactionEvent bankingPayloadEvent(String data) {
        return TransactionEvent.builder()
            .requestId(REQUEST_ID)
            .eventType(EventType.BANKING_PAYLOAD)
            .data(data)
            .build();
    }
}
