package com.example.grpcobservability

import com.example.grpcobservability.proto.FarewellRequest
import com.example.grpcobservability.proto.GreetRequest
import com.example.grpcobservability.proto.GreetResponse
import com.example.grpcobservability.proto.GreetingServiceGrpc
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import io.grpc.stub.StreamObserver
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.Logger
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.layout.PatternLayout
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SpringBootTest
@Import(TestOtelConfig::class)
class GreetingServiceIntegrationTest {

    @Autowired
    lateinit var spanExporter: InMemorySpanExporter

    companion object {
        private lateinit var channel: ManagedChannel

        @JvmStatic
        @BeforeAll
        fun setup() {
            channel = ManagedChannelBuilder.forAddress("localhost", 9090)
                .usePlaintext()
                .build()
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS)
        }
    }

    @AfterEach
    fun resetSpans() {
        spanExporter.reset()
    }

    private fun buildStubWithMetadata(
        userId: String,
        tenantId: String,
        traceparent: String,
    ): GreetingServiceGrpc.GreetingServiceBlockingStub {
        val metadata = Metadata().apply {
            put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), userId)
            put(Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER), tenantId)
            put(Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER), traceparent)
        }
        return GreetingServiceGrpc.newBlockingStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
    }

    private fun buildAsyncStubWithMetadata(
        userId: String,
        tenantId: String,
        traceparent: String,
    ): GreetingServiceGrpc.GreetingServiceStub {
        val metadata = Metadata().apply {
            put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), userId)
            put(Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER), tenantId)
            put(Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER), traceparent)
        }
        return GreetingServiceGrpc.newStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
    }

    @Test
    fun testGreetPopulatesFullContext() {
        val knownTraceId = "0af7651916cd43dd8448eb211c80319c"
        val traceparent = "00-$knownTraceId-b7ad6b7169203331-01"

        val stub = buildStubWithMetadata("user-42", "tenant-abc", traceparent)

        val request = GreetRequest.newBuilder()
            .setName("Alice")
            .setUserId("user-42")
            .setTenantId("tenant-abc")
            .build()

        val response = stub.greet(request)

        assertEquals("Hello, Alice!", response.message)
        assertTrue(response.requestId.isNotBlank())

        // Wait briefly for async span export
        Thread.sleep(200)

        val spans = spanExporter.finishedSpanItems
        assertTrue(spans.isNotEmpty(), "Expected at least one span to be exported")

        val greetSpan = spans.find { it.name.contains("Greet") }
        assertTrue(greetSpan != null, "Expected a span named containing 'Greet'")
        assertEquals(knownTraceId, greetSpan!!.traceId, "Span trace ID should match injected traceparent")
    }

    @Test
    fun testGreetStreamPropagatesContext() {
        val knownTraceId = "1af7651916cd43dd8448eb211c80319c"
        val traceparent = "00-$knownTraceId-b7ad6b7169203331-01"

        val stub = buildAsyncStubWithMetadata("user-stream", "tenant-stream", traceparent)

        val request = GreetRequest.newBuilder()
            .setName("Bob")
            .setUserId("user-stream")
            .setTenantId("tenant-stream")
            .build()

        val responses = mutableListOf<GreetResponse>()
        val latch = CountDownLatch(1)

        stub.greetStream(request, object : StreamObserver<GreetResponse> {
            override fun onNext(value: GreetResponse) {
                responses.add(value)
            }

            override fun onError(t: Throwable) {
                latch.countDown()
            }

            override fun onCompleted() {
                latch.countDown()
            }
        })

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Stream should complete within 10s")
        assertEquals(5, responses.size, "Expected 5 streamed responses")

        // Wait briefly for span export
        Thread.sleep(200)

        val spans = spanExporter.finishedSpanItems
        assertTrue(spans.isNotEmpty(), "Expected spans to be exported for streaming call")

        val streamSpan = spans.find { it.name.contains("GreetStream") }
        assertTrue(streamSpan != null, "Expected a span named containing 'GreetStream'")
        assertEquals(knownTraceId, streamSpan!!.traceId, "Stream span trace ID should match injected traceparent")
    }

    @Test
    fun testContextPropagationAcrossDispatcherSwitch() {
        val capturedEvents = ConcurrentLinkedQueue<LogEvent>()

        val appender = object : AbstractAppender(
            "TestCapture",
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
            val knownTraceId = "2af7651916cd43dd8448eb211c80319c"
            val traceparent = "00-$knownTraceId-b7ad6b7169203331-01"

            val stub = buildStubWithMetadata("user-io-test", "tenant-io-test", traceparent)

            val request = GreetRequest.newBuilder()
                .setName("Charlie")
                .setUserId("user-io-test")
                .setTenantId("tenant-io-test")
                .build()

            stub.greet(request)

            // Wait for async coroutine processing
            Thread.sleep(500)

            // Assert context on the log BEFORE delay (proves dispatcher switch)
            val beforeDelayLog = capturedEvents.find {
                it.message.formattedMessage.contains("Processing on IO dispatcher (before delay)")
            }
            assertTrue(beforeDelayLog != null, "Expected log from IO dispatcher before delay")

            val beforeMdc = beforeDelayLog!!.contextData.toMap()
            assertEquals("user-io-test", beforeMdc["userId"], "userId should propagate to IO dispatcher")
            assertEquals("tenant-io-test", beforeMdc["tenantId"], "tenantId should propagate to IO dispatcher")
            assertTrue(
                beforeMdc["trace.id"]?.isNotBlank() == true,
                "trace.id should be present on IO dispatcher",
            )

            // Assert context on the log AFTER delay (proves context survives
            // suspension + resumption on a potentially different IO pool thread)
            val afterDelayLog = capturedEvents.find {
                it.message.formattedMessage.contains("Processing on IO dispatcher (after delay)")
            }
            assertTrue(afterDelayLog != null, "Expected log from IO dispatcher after delay")

            val afterMdc = afterDelayLog!!.contextData.toMap()
            assertEquals("user-io-test", afterMdc["userId"], "userId should survive delay on IO dispatcher")
            assertEquals("tenant-io-test", afterMdc["tenantId"], "tenantId should survive delay on IO dispatcher")
            assertTrue(
                afterMdc["trace.id"]?.isNotBlank() == true,
                "trace.id should survive delay on IO dispatcher",
            )
        } finally {
            rootLogger.removeAppender(appender)
            appender.stop()
        }
    }

    // ── Farewell method tests ───────────────────────────────────────────

    @Test
    fun testFarewellReturnsSuccessResponse() {
        val stub = buildStubWithMetadata("user-42", "tenant-abc", "00-3af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

        val request = FarewellRequest.newBuilder()
            .setName("Alice")
            .setUserId("user-42")
            .setTenantId("tenant-abc")
            .build()

        val response = stub.farewell(request)

        assertEquals("Goodbye, Alice! See you next time.", response.message)
        assertTrue(response.requestId.isNotBlank())
        assertFalse(response.hasError(), "Successful farewell should not have an error")
    }

    @Test
    fun testFarewellReturnsErrorForUnknownUser() {
        val stub = buildStubWithMetadata("user-42", "tenant-abc", "00-4af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

        val request = FarewellRequest.newBuilder()
            .setName("unknown")
            .setUserId("user-42")
            .setTenantId("tenant-abc")
            .build()

        val response = stub.farewell(request)

        assertTrue(response.hasError(), "Farewell for 'unknown' should have an error")
        assertEquals(404, response.error.httpCode)
        assertEquals("User not found", response.error.reason)
    }

    @Test
    fun testGreetReturnsErrorForErrorName() {
        val stub = buildStubWithMetadata("user-42", "tenant-abc", "00-5af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

        val request = GreetRequest.newBuilder()
            .setName("error")
            .setUserId("user-42")
            .setTenantId("tenant-abc")
            .build()

        val response = stub.greet(request)

        assertTrue(response.hasError(), "Greet for 'error' should have an error")
        assertEquals(500, response.error.httpCode)
        assertEquals("Simulated internal error for testing", response.error.reason)
    }

    // ── Logging interceptor tests ───────────────────────────────────────

    @Test
    fun testLoggingInterceptorLogsStartAndEndForSuccessfulCall() {
        val capturedEvents = ConcurrentLinkedQueue<LogEvent>()
        val appender = createTestAppender(capturedEvents)
        val rootLogger = LogManager.getRootLogger() as Logger
        rootLogger.addAppender(appender)

        try {
            val stub = buildStubWithMetadata("user-42", "tenant-abc", "00-6af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
            stub.greet(
                GreetRequest.newBuilder()
                    .setName("LogTest")
                    .setUserId("user-42")
                    .setTenantId("tenant-abc")
                    .build(),
            )

            Thread.sleep(500)

            val startLog = capturedEvents.find {
                it.message.formattedMessage.contains("gRPC START") &&
                    it.message.formattedMessage.contains("GreetingService/Greet")
            }
            assertTrue(startLog != null, "Expected 'gRPC START' log for Greet")

            val endLog = capturedEvents.find {
                it.message.formattedMessage.contains("gRPC END") &&
                    it.message.formattedMessage.contains("GreetingService/Greet") &&
                    it.message.formattedMessage.contains("OK")
            }
            assertTrue(endLog != null, "Expected 'gRPC END ... OK' log for successful Greet")
        } finally {
            rootLogger.removeAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun testLoggingInterceptorLogsFailureForErrorResponse() {
        val capturedEvents = ConcurrentLinkedQueue<LogEvent>()
        val appender = createTestAppender(capturedEvents)
        val rootLogger = LogManager.getRootLogger() as Logger
        rootLogger.addAppender(appender)

        try {
            val stub = buildStubWithMetadata("user-42", "tenant-abc", "00-7af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
            stub.greet(
                GreetRequest.newBuilder()
                    .setName("error")
                    .setUserId("user-42")
                    .setTenantId("tenant-abc")
                    .build(),
            )

            Thread.sleep(500)

            val endLog = capturedEvents.find {
                it.message.formattedMessage.contains("gRPC END") &&
                    it.message.formattedMessage.contains("GreetingService/Greet") &&
                    it.message.formattedMessage.contains("httpCode=500")
            }
            assertTrue(endLog != null, "Expected 'gRPC END' log with httpCode=500 for error response")
            assertEquals(
                org.apache.logging.log4j.Level.WARN,
                endLog!!.level,
                "Error response should be logged at WARN level",
            )
        } finally {
            rootLogger.removeAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun testLoggingInterceptorLogsFarewellStartAndEnd() {
        val capturedEvents = ConcurrentLinkedQueue<LogEvent>()
        val appender = createTestAppender(capturedEvents)
        val rootLogger = LogManager.getRootLogger() as Logger
        rootLogger.addAppender(appender)

        try {
            val stub = buildStubWithMetadata("user-42", "tenant-abc", "00-8af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
            stub.farewell(
                FarewellRequest.newBuilder()
                    .setName("LogFarewell")
                    .setUserId("user-42")
                    .setTenantId("tenant-abc")
                    .build(),
            )

            Thread.sleep(500)

            val startLog = capturedEvents.find {
                it.message.formattedMessage.contains("gRPC START") &&
                    it.message.formattedMessage.contains("GreetingService/Farewell")
            }
            assertTrue(startLog != null, "Expected 'gRPC START' log for Farewell")

            val endLog = capturedEvents.find {
                it.message.formattedMessage.contains("gRPC END") &&
                    it.message.formattedMessage.contains("GreetingService/Farewell") &&
                    it.message.formattedMessage.contains("OK")
            }
            assertTrue(endLog != null, "Expected 'gRPC END ... OK' log for successful Farewell")
        } finally {
            rootLogger.removeAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun testLoggingInterceptorLogsFarewellFailure() {
        val capturedEvents = ConcurrentLinkedQueue<LogEvent>()
        val appender = createTestAppender(capturedEvents)
        val rootLogger = LogManager.getRootLogger() as Logger
        rootLogger.addAppender(appender)

        try {
            val stub = buildStubWithMetadata("user-42", "tenant-abc", "00-9af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
            stub.farewell(
                FarewellRequest.newBuilder()
                    .setName("unknown")
                    .setUserId("user-42")
                    .setTenantId("tenant-abc")
                    .build(),
            )

            Thread.sleep(500)

            val endLog = capturedEvents.find {
                it.message.formattedMessage.contains("gRPC END") &&
                    it.message.formattedMessage.contains("GreetingService/Farewell") &&
                    it.message.formattedMessage.contains("httpCode=404")
            }
            assertTrue(endLog != null, "Expected 'gRPC END' log with httpCode=404 for unknown user")
            assertEquals(
                org.apache.logging.log4j.Level.WARN,
                endLog!!.level,
                "404 response should be logged at WARN level",
            )
        } finally {
            rootLogger.removeAppender(appender)
            appender.stop()
        }
    }

    private fun createTestAppender(capturedEvents: ConcurrentLinkedQueue<LogEvent>): AbstractAppender {
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
        return appender
    }
}
