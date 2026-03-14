package com.platform.logging.context;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RequestContextFilterTest {

    private RequestContextFilter filter;

    @Mock private HttpServletRequest request;
    @Mock private HttpServletResponse response;
    @Mock private FilterChain chain;

    @BeforeEach
    void setUp() {
        filter = new RequestContextFilter();
    }

    @AfterEach
    void tearDown() {
        RequestContext.clear();
        MDC.clear();
    }

    @Test
    @DisplayName("doFilter extracts all headers and sets RequestContext")
    void doFilterExtractsHeaders() throws Exception {
        when(request.getHeader("X-Request-ID")).thenReturn("req-123");
        when(request.getHeader("X-Org-ID")).thenReturn("42");
        when(request.getHeader("X-Org-Name")).thenReturn("Acme");

        doAnswer(inv -> {
            // During chain.doFilter, context should be set
            assertEquals("req-123", RequestContext.getRequestId());
            assertEquals(42L, RequestContext.getOrgId());
            assertEquals("Acme", RequestContext.getOrgName());
            assertEquals("req-123", MDC.get("requestId"));
            return null;
        }).when(chain).doFilter(request, response);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
    }

    @Test
    @DisplayName("doFilter generates UUID when X-Request-ID header is missing")
    void doFilterMissingRequestId() throws Exception {
        when(request.getHeader("X-Request-ID")).thenReturn(null);

        doAnswer(inv -> {
            String requestId = RequestContext.getRequestId();
            assertNotNull(requestId);
            assertDoesNotThrow(() -> UUID.fromString(requestId));
            return null;
        }).when(chain).doFilter(request, response);

        filter.doFilter(request, response, chain);
    }

    @Test
    @DisplayName("doFilter generates UUID when X-Request-ID header is blank")
    void doFilterBlankRequestId() throws Exception {
        when(request.getHeader("X-Request-ID")).thenReturn("   ");

        doAnswer(inv -> {
            String requestId = RequestContext.getRequestId();
            assertNotNull(requestId);
            assertNotEquals("   ", requestId);
            return null;
        }).when(chain).doFilter(request, response);

        filter.doFilter(request, response, chain);
    }

    @Test
    @DisplayName("doFilter handles invalid X-Org-ID gracefully")
    void doFilterInvalidOrgId() throws Exception {
        when(request.getHeader("X-Request-ID")).thenReturn("req-1");
        when(request.getHeader("X-Org-ID")).thenReturn("not-a-number");

        doAnswer(inv -> {
            assertNull(RequestContext.getOrgId());
            return null;
        }).when(chain).doFilter(request, response);

        filter.doFilter(request, response, chain);
    }

    @Test
    @DisplayName("doFilter handles null X-Org-ID header")
    void doFilterNullOrgId() throws Exception {
        when(request.getHeader("X-Request-ID")).thenReturn("req-1");
        when(request.getHeader("X-Org-ID")).thenReturn(null);

        doAnswer(inv -> {
            assertNull(RequestContext.getOrgId());
            return null;
        }).when(chain).doFilter(request, response);

        filter.doFilter(request, response, chain);
    }

    @Test
    @DisplayName("doFilter clears context after chain completes")
    void doFilterClearsAfterCompletion() throws Exception {
        when(request.getHeader("X-Request-ID")).thenReturn("req-1");
        when(request.getHeader("X-Org-ID")).thenReturn("42");
        when(request.getHeader("X-Org-Name")).thenReturn("Acme");

        filter.doFilter(request, response, chain);

        // After filter completes, context should be cleared
        assertNull(RequestContext.getOrgId());
        assertNull(RequestContext.getOrgName());
        assertNull(MDC.get("requestId"));
    }

    @Test
    @DisplayName("doFilter clears context even when chain throws exception")
    void doFilterClearsOnException() throws Exception {
        when(request.getHeader("X-Request-ID")).thenReturn("req-1");
        when(request.getHeader("X-Org-ID")).thenReturn("42");
        when(request.getHeader("X-Org-Name")).thenReturn("Acme");

        doThrow(new RuntimeException("boom")).when(chain).doFilter(request, response);

        assertThrows(RuntimeException.class, () ->
            filter.doFilter(request, response, chain));

        // Context should still be cleared
        assertNull(RequestContext.getOrgId());
        assertNull(RequestContext.getOrgName());
        assertNull(MDC.get("requestId"));
    }

    @Test
    @DisplayName("doFilter sets MDC requestId for log correlation")
    void doFilterSetsMdc() throws Exception {
        when(request.getHeader("X-Request-ID")).thenReturn("req-mdc");

        doAnswer(inv -> {
            assertEquals("req-mdc", MDC.get("requestId"));
            return null;
        }).when(chain).doFilter(request, response);

        filter.doFilter(request, response, chain);
    }
}
