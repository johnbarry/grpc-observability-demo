package com.example.grpcobservability

import com.example.grpcobservability.proto.GreetingServiceGrpc
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.Logger
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.layout.PatternLayout
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Shared test utilities for building gRPC stubs with metadata headers.
 */
object GrpcTestSupport {

    fun blockingStub(
        channel: ManagedChannel,
        userId: String = "test-user",
        tenantId: String = "test-tenant",
        traceparent: String? = null,
    ): GreetingServiceGrpc.GreetingServiceBlockingStub {
        val metadata = buildMetadata(userId, tenantId, traceparent)
        return GreetingServiceGrpc.newBlockingStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
    }

    fun asyncStub(
        channel: ManagedChannel,
        userId: String = "test-user",
        tenantId: String = "test-tenant",
        traceparent: String? = null,
    ): GreetingServiceGrpc.GreetingServiceStub {
        val metadata = buildMetadata(userId, tenantId, traceparent)
        return GreetingServiceGrpc.newStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
    }

    fun traceparent(traceId: String): String = "00-$traceId-b7ad6b7169203331-01"

    private fun buildMetadata(userId: String, tenantId: String, traceparent: String?): Metadata =
        Metadata().apply {
            put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), userId)
            put(Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER), tenantId)
            traceparent?.let {
                put(Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER), it)
            }
        }
}

/**
 * Installs a Log4j2 appender that captures log events, runs [block] with the
 * captured events, and cleans up the appender afterwards.
 */
fun withCapturedLogs(block: (ConcurrentLinkedQueue<LogEvent>) -> Unit) {
    val capturedEvents = ConcurrentLinkedQueue<LogEvent>()
    val appender = object : AbstractAppender(
        "TestCapture-${System.nanoTime()}",
        null,
        PatternLayout.createDefaultLayout(),
        true,
        emptyArray(),
    ) {
        override fun append(event: LogEvent) {
            capturedEvents.add(event.toImmutable())
        }
    }
    appender.start()

    val rootLogger = LogManager.getRootLogger() as Logger
    rootLogger.addAppender(appender)
    try {
        block(capturedEvents)
    } finally {
        rootLogger.removeAppender(appender)
        appender.stop()
    }
}
