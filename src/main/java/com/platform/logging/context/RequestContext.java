package com.platform.logging.context;

import java.util.UUID;

public final class RequestContext {

    private static final ThreadLocal<String> REQUEST_ID  = new ThreadLocal<>();
    private static final ThreadLocal<Long>   ORG_ID      = new ThreadLocal<>();
    private static final ThreadLocal<String> ORG_NAME    = new ThreadLocal<>();

    private RequestContext() {}

    public static String getRequestId() {
        String id = REQUEST_ID.get();
        return id != null ? id : UUID.randomUUID().toString();
    }

    public static Long   getOrgId()     { return ORG_ID.get(); }
    public static String getOrgName()   { return ORG_NAME.get(); }

    public static void set(String requestId, Long orgId, String orgName) {
        REQUEST_ID.set(requestId);
        ORG_ID.set(orgId);
        ORG_NAME.set(orgName);
    }

    public static void clear() {
        REQUEST_ID.remove();
        ORG_ID.remove();
        ORG_NAME.remove();
    }
}
