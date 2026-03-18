package com.example.grpcobservability

import com.example.grpcobservability.context.ObservabilityContext
import com.example.grpcobservability.context.captureObservabilityContext
import com.example.grpcobservability.context.withFields
import com.example.grpcobservability.interceptor.AuthInterceptor
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import io.grpc.Context as GrpcContext

class MdcCoroutineTest {

    @Test
    fun `ObservabilityContext propagates gRPC context and OTel span across dispatcher switch and delay`() {
        val grpcCtx = GrpcContext.current()
            .withValue(AuthInterceptor.USER_ID_CTX_KEY, "test-user")
            .withValue(AuthInterceptor.TENANT_ID_CTX_KEY, "test-tenant")
        val previousGrpc = grpcCtx.attach()

        // Create a test span with a known trace ID
        val spanCtx = SpanContext.create(
            "aaf7651916cd43dd8448eb211c80319c",
            "b7ad6b7169203331",
            TraceFlags.getSampled(),
            TraceState.getDefault(),
        )
        val testSpan = Span.wrap(spanCtx)
        val otelCtx = Context.current().with(testSpan)
        val previousOtelScope = otelCtx.makeCurrent()

        try {
            runBlocking(ObservabilityContext()) {
                // MDC derived from gRPC context
                assertEquals("test-user", MDC.get("userId"))
                assertEquals("test-tenant", MDC.get("tenantId"))
                // MDC derived from OTel span
                assertEquals("aaf7651916cd43dd8448eb211c80319c", MDC.get("trace.id"))

                withContext(Dispatchers.IO) {
                    assertEquals("test-user", MDC.get("userId"), "MDC should propagate to IO")
                    assertEquals("aaf7651916cd43dd8448eb211c80319c", MDC.get("trace.id"), "trace.id should propagate to IO")
                    delay(10)
                    assertEquals("test-user", MDC.get("userId"), "MDC should survive delay")
                    assertEquals("aaf7651916cd43dd8448eb211c80319c", MDC.get("trace.id"), "trace.id should survive delay")
                }

                // gRPC Context keys should also work directly
                assertEquals("test-user", AuthInterceptor.USER_ID_CTX_KEY.get())
            }
        } finally {
            previousOtelScope.close()
            grpcCtx.detach(previousGrpc)
        }
    }

    @Test
    fun `gRPC context survives dispatcher switch and delay`() {
        val key = GrpcContext.key<String>("test-key")
        val grpcCtx = GrpcContext.current().withValue(key, "grpc-value")
        val previous = grpcCtx.attach()

        try {
            runBlocking(ObservabilityContext()) {
                assertEquals("grpc-value", key.get())

                withContext(Dispatchers.IO) {
                    assertEquals("grpc-value", key.get(), "gRPC context should propagate to IO")
                    delay(10)
                    assertEquals("grpc-value", key.get(), "gRPC context should survive delay on IO")
                }
            }
        } finally {
            grpcCtx.detach(previous)
        }
    }

    @Test
    fun `withFields adds ephemeral MDC entries scoped to block`() {
        val grpcCtx = GrpcContext.current()
            .withValue(AuthInterceptor.USER_ID_CTX_KEY, "user-1")
        val previous = grpcCtx.attach()

        try {
            runBlocking(captureObservabilityContext()) {
                assertEquals("user-1", MDC.get("userId"))
                assertNull(MDC.get("requestId"))

                withFields("requestId" to "req-123", "rpc.method" to "Test") {
                    assertEquals("user-1", MDC.get("userId"), "base fields should persist")
                    assertEquals("req-123", MDC.get("requestId"), "extra field should be present")
                    assertEquals("Test", MDC.get("rpc.method"))

                    withContext(Dispatchers.IO) {
                        delay(10)
                        assertEquals("user-1", MDC.get("userId"), "base + extra should survive IO + delay")
                        assertEquals("req-123", MDC.get("requestId"))
                    }
                }

                assertNull(MDC.get("requestId"), "extra fields should be cleaned up")
                assertEquals("user-1", MDC.get("userId"), "base fields should survive")
            }
        } finally {
            grpcCtx.detach(previous)
        }
    }

    @Test
    fun `single element propagates gRPC context, OTel context, and MDC together`() {
        // Setup both gRPC and OTel context
        val grpcCtx = GrpcContext.current()
            .withValue(AuthInterceptor.USER_ID_CTX_KEY, "composite-user")
        val previousGrpc = grpcCtx.attach()

        val spanCtx = SpanContext.create(
            "bbf7651916cd43dd8448eb211c80319c",
            "c7ad6b7169203331",
            TraceFlags.getSampled(),
            TraceState.getDefault(),
        )
        val otelCtx = Context.current().with(Span.wrap(spanCtx))
        val previousOtelScope = otelCtx.makeCurrent()

        try {
            // One element captures everything
            val composed = captureObservabilityContext()

            runBlocking(composed) {
                // All three systems propagated by one element
                assertEquals("composite-user", MDC.get("userId"))
                assertEquals("bbf7651916cd43dd8448eb211c80319c", MDC.get("trace.id"))
                assertEquals("composite-user", AuthInterceptor.USER_ID_CTX_KEY.get())

                withContext(Dispatchers.IO) {
                    assertEquals("composite-user", MDC.get("userId"))
                    assertEquals("bbf7651916cd43dd8448eb211c80319c", MDC.get("trace.id"))
                    assertEquals("composite-user", AuthInterceptor.USER_ID_CTX_KEY.get())
                    delay(10)
                    assertEquals("composite-user", MDC.get("userId"))
                    assertEquals("bbf7651916cd43dd8448eb211c80319c", MDC.get("trace.id"))
                    assertEquals("composite-user", AuthInterceptor.USER_ID_CTX_KEY.get())
                }
            }
        } finally {
            previousOtelScope.close()
            grpcCtx.detach(previousGrpc)
        }
    }
}
