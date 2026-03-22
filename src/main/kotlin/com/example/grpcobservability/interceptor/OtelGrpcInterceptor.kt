package com.example.grpcobservability.interceptor

import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.context.propagation.TextMapPropagator
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

object GrpcHeadersGetter : TextMapGetter<Metadata> {
    override fun keys(carrier: Metadata): MutableIterable<String> =
        carrier.keys().toMutableList()

    override fun get(carrier: Metadata?, key: String): String? =
        carrier?.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER))
}

/**
 * Extracts the W3C traceparent from gRPC metadata, starts a server span,
 * and manages the OTel context scope.
 *
 * Does NOT touch MDC — that is derived by [ObservabilityContext] in the coroutine layer.
 */
class OtelGrpcInterceptor(
    private val tracer: Tracer,
    private val propagator: TextMapPropagator,
) : ServerInterceptor {

    override fun <ReqT, RespT> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
        val extractedContext = propagator.extract(Context.current(), headers, GrpcHeadersGetter)

        val span = tracer.spanBuilder(call.methodDescriptor.fullMethodName)
            .setParent(extractedContext)
            .setSpanKind(SpanKind.SERVER)
            .startSpan()

        val scopedContext = extractedContext.with(span)
        val scope = scopedContext.makeCurrent()

        logger.debug { "Started span for ${call.methodDescriptor.fullMethodName}" }

        // Guard against double-close: both close() and onCancel() can fire in edge cases
        val closed = AtomicBoolean(false)

        fun endSpan(status: StatusCode, description: String) {
            if (closed.compareAndSet(false, true)) {
                if (status == StatusCode.ERROR) {
                    span.setStatus(status, description)
                }
                span.end()
                scope.close()
            }
        }

        val wrappedCall = object : SimpleForwardingServerCall<ReqT, RespT>(call) {
            override fun close(status: io.grpc.Status, trailers: Metadata) {
                val otelStatus = if (status.isOk) StatusCode.OK else StatusCode.ERROR
                endSpan(otelStatus, status.description ?: "")
                super.close(status, trailers)
            }
        }

        val listener = next.startCall(wrappedCall, headers)

        return object : SimpleForwardingServerCallListener<ReqT>(listener) {
            override fun onCancel() {
                endSpan(StatusCode.ERROR, "Cancelled")
                super.onCancel()
            }
        }
    }
}
