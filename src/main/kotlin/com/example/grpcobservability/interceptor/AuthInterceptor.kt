package com.example.grpcobservability.interceptor

import com.example.grpcobservability.context.MdcProviders
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.Context
import io.grpc.Contexts
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor

private val logger = KotlinLogging.logger {}

/**
 * Extracts x-user-id and x-tenant-id from gRPC metadata into gRPC Context keys.
 *
 * Does NOT touch MDC — instead registers a derivation with [MdcProviders] so that
 * [ObservabilityContext] populates MDC from these keys on each coroutine thread switch.
 */
class AuthInterceptor : ServerInterceptor {

    companion object {
        val USER_ID_CTX_KEY: Context.Key<String> = Context.key("userId")
        val TENANT_ID_CTX_KEY: Context.Key<String> = Context.key("tenantId")

        private val USER_ID_METADATA_KEY =
            Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER)
        private val TENANT_ID_METADATA_KEY =
            Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER)

        init {
            MdcProviders.register { grpcContext ->
                buildMap {
                    USER_ID_CTX_KEY.get(grpcContext)?.let { put("userId", it) }
                    TENANT_ID_CTX_KEY.get(grpcContext)?.let { put("tenantId", it) }
                }
            }
        }
    }

    override fun <ReqT, RespT> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
        val userId = headers.get(USER_ID_METADATA_KEY) ?: "unknown"
        val tenantId = headers.get(TENANT_ID_METADATA_KEY) ?: "unknown"

        val grpcContext = Context.current()
            .withValue(USER_ID_CTX_KEY, userId)
            .withValue(TENANT_ID_CTX_KEY, tenantId)

        logger.debug { "Auth context: userId=$userId, tenantId=$tenantId" }

        return Contexts.interceptCall(grpcContext, call, headers, next)
    }
}
