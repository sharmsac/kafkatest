package com.platform.logging.model;

public enum EventType {
    /** APG gateway: request received from client */
    APG_REQUEST,

    /** APG gateway: response sent back to client */
    APG_RESPONSE,

    /** Router: request forwarded to downstream service */
    ROUTER_REQUEST,

    /** Router: response received from downstream service */
    ROUTER_RESPONSE,

    /** Downstream API: full call (start + end captured in one event) */
    API_CALL,

    /** ESP-X header payload captured at APG */
    ESPX_HEADER,

    /** Core banking payload (request/response bodies) */
    CORE_PAYLOAD,

    /** Banking-specific payload (additional metadata) */
    BANKING_PAYLOAD
}
