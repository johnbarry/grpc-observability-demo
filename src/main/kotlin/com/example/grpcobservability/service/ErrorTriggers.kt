package com.example.grpcobservability.service

/**
 * Well-known request names that trigger simulated error responses.
 * Shared between service implementation and tests.
 */
object ErrorTriggers {
    /** Greet: triggers a 500 Internal Server Error response. */
    const val GREET_ERROR_NAME = "error"

    /** Farewell: triggers a 404 Not Found response. */
    const val FAREWELL_NOT_FOUND_NAME = "unknown"
}
