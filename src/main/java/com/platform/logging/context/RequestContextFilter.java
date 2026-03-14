package com.platform.logging.context;

import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.UUID;

@Component
@Order(1)   // run before any other filter
public class RequestContextFilter implements Filter {

    private static final String HEADER_REQUEST_ID = "X-Request-ID";
    private static final String HEADER_ORG_ID     = "X-Org-ID";
    private static final String HEADER_ORG_NAME   = "X-Org-Name";

    @Override
    public void doFilter(ServletRequest req, ServletResponse res,
                         FilterChain chain) throws IOException, ServletException {

        HttpServletRequest http = (HttpServletRequest) req;

        String requestId = http.getHeader(HEADER_REQUEST_ID);
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }

        Long orgId = null;
        String orgIdHeader = http.getHeader(HEADER_ORG_ID);
        if (orgIdHeader != null) {
            try { orgId = Long.parseLong(orgIdHeader); }
            catch (NumberFormatException ignored) {}
        }

        String orgName = http.getHeader(HEADER_ORG_NAME);

        // Set on ThreadLocal + SLF4J MDC for log correlation
        RequestContext.set(requestId, orgId, orgName);
        MDC.put("requestId", requestId);

        try {
            chain.doFilter(req, res);
        } finally {
            RequestContext.clear();
            MDC.remove("requestId");
        }
    }
}
