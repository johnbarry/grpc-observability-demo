package com.example.grpcobservability.interceptor

import com.example.grpcobservability.context.MdcProviders
import com.google.protobuf.Message
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener
import io.grpc.Metadata
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.Status
import io.opentelemetry.api.trace.Span
import org.slf4j.MDC
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import io.grpc.Context as GrpcContext

private val logger = KotlinLogging.logger {}

/**
 * Logs the start and end of every gRPC method call.
 *
 * Because this interceptor runs on the gRPC transport thread (not inside a coroutine),
 * MDC is not populated by ObservabilityContext. Instead, this interceptor derives MDC
 * entries directly from the current gRPC Context and OTel Span for each log call,
 * using the same MdcProviders registry that ObservabilityContext uses.
 *
 * On each response message (unary or streaming), inspects the protobuf for an `error`
 * field. If the error is present and its `http_code` is outside the 2xx range, the
 * message is logged as a failure at WARN level. For streaming RPCs, every message
 * is inspected individually.
 */
class LoggingInterceptor : ServerInterceptor {

    override fun <ReqT, RespT> interceptCall(
        call: ServerCall<ReqT, RespT>,
        headers: Metadata,
        next: ServerCallHandler<ReqT, RespT>,
    ): ServerCall.Listener<ReqT> {
        val methodName = call.methodDescriptor.fullMethodName
        val startNanos = System.nanoTime()

        // Visible across threads: sendMessage() runs on the coroutine thread (which may
        // vary across suspension points), while close() runs on the gRPC transport thread.
        val messageIndex = AtomicInteger(0)
        val lastError = AtomicReference<ErrorInfo?>(null)

        logWithContext { logger.info { "gRPC START $methodName" } }

        val wrappedCall = object : SimpleForwardingServerCall<ReqT, RespT>(call) {

            override fun sendMessage(message: RespT) {
                val idx = messageIndex.incrementAndGet()
                val errorResult = inspectForError(message)
                if (errorResult != null) {
                    lastError.set(errorResult)
                    val suffix = if (call.methodDescriptor.type.serverSendsOneMessage()) "" else " [message #$idx]"
                    logWithContext {
                        logger.warn {
                            "gRPC MSG   $methodName$suffix — response contains error: " +
                                "httpCode=${errorResult.httpCode} reason=\"${errorResult.reason}\""
                        }
                    }
                }
                super.sendMessage(message)
            }

            override fun close(status: Status, trailers: Metadata) {
                val durationMs = (System.nanoTime() - startNanos) / 1_000_000
                val error = lastError.get()

                logWithContext {
                    when {
                        !status.isOk -> {
                            logger.warn {
                                "gRPC END   $methodName — gRPC status=${status.code} " +
                                    "description=\"${status.description ?: ""}\" (${durationMs}ms)"
                            }
                        }
                        error != null -> {
                            logger.warn {
                                "gRPC END   $methodName — FAILED " +
                                    "httpCode=${error.httpCode} reason=\"${error.reason}\" (${durationMs}ms)"
                            }
                        }
                        else -> {
                            logger.info { "gRPC END   $methodName — OK (${durationMs}ms)" }
                        }
                    }
                }

                super.close(status, trailers)
            }
        }

        val listener = next.startCall(wrappedCall, headers)

        return object : SimpleForwardingServerCallListener<ReqT>(listener) {
            override fun onCancel() {
                val durationMs = (System.nanoTime() - startNanos) / 1_000_000
                logWithContext { logger.warn { "gRPC END   $methodName — CANCELLED (${durationMs}ms)" } }
                super.onCancel()
            }
        }
    }

    /**
     * Temporarily populates MDC from the current gRPC Context and OTel Span,
     * executes the logging block, then cleans up. This ensures interceptor-thread
     * logs have the same context fields as coroutine-thread logs.
     */
    private inline fun logWithContext(block: () -> Unit) {
        val entries = buildMdcEntries()
        entries.forEach { (k, v) -> MDC.put(k, v) }
        try {
            block()
        } finally {
            entries.keys.forEach { MDC.remove(it) }
        }
    }

    private fun buildMdcEntries(): Map<String, String> = buildMap {
        putAll(MdcProviders.deriveFrom(GrpcContext.current()))
        val spanCtx = Span.current().spanContext
        if (spanCtx.isValid) {
            put("trace.id", spanCtx.traceId)
            put("span.id", spanCtx.spanId)
        }
    }

    /**
     * Inspect a protobuf response for an `error` field with a non-2xx `http_code`.
     * Uses protobuf descriptors so this works generically across all response types
     * that follow the convention of having an `optional Error error` field.
     */
    private fun <RespT> inspectForError(message: RespT): ErrorInfo? {
        if (message !is Message) return null

        val descriptor = message.descriptorForType
        val errorField = descriptor.findFieldByName("error") ?: return null

        if (!message.hasField(errorField)) return null

        val errorMsg = message.getField(errorField) as? Message ?: return null
        val httpCodeField = errorMsg.descriptorForType.findFieldByName("http_code") ?: return null
        val reasonField = errorMsg.descriptorForType.findFieldByName("reason")

        val httpCode = errorMsg.getField(httpCodeField) as? Int ?: return null
        val reason = reasonField?.let { errorMsg.getField(it) as? String } ?: ""

        if (httpCode in 200..299) return null

        return ErrorInfo(httpCode, reason)
    }

    private data class ErrorInfo(val httpCode: Int, val reason: String)
}
