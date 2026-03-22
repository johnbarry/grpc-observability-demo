package com.example.grpcobservability

import com.example.grpcobservability.proto.FarewellRequest
import com.example.grpcobservability.proto.GreetRequest
import com.example.grpcobservability.proto.GreetResponse
import com.example.grpcobservability.service.ErrorTriggers
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@SpringBootTest
@Import(TestOtelConfig::class)
class GreetingServiceIntegrationTest {

    @Autowired
    lateinit var spanExporter: InMemorySpanExporter

    @Autowired
    lateinit var openTelemetry: OpenTelemetry

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

    // ── Parent-child span verification ──────────────────────────────────

    @Test
    fun testGreetCreatesChildSpanOfParent() {
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-greet-client")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)

        val response = stub.greet(
            GreetRequest.newBuilder().setName("Alice").setUserId("test-user").setTenantId("test-tenant").build(),
        )

        assertEquals("Hello, Alice!", response.message)
        parent.span.end()
        Thread.sleep(300)

        val spans = spanExporter.finishedSpanItems
        assertTrue(spans.size >= 2, "Expected at least parent + child span, got ${spans.size}")

        val parentExported = spans.find { it.spanId == parent.spanId }
        assertTrue(parentExported != null, "Parent span should be exported")
        assertEquals(parent.traceId, parentExported!!.traceId)

        val serverSpan = spans.find {
            it.name.contains("GreetingService/Greet") && it.kind == SpanKind.SERVER
        }
        assertTrue(serverSpan != null, "Expected a SERVER span for GreetingService/Greet")
        assertEquals(parent.traceId, serverSpan!!.traceId, "Child span must share parent's traceId")
        assertEquals(parent.spanId, serverSpan.parentSpanId, "Child span's parentSpanId must be the parent's spanId")
        assertNotEquals(parent.spanId, serverSpan.spanId, "Child span must have its own spanId")

        println("=== Parent-Child Span Relationship (Greet) ===")
        println("  Parent: traceId=${parent.traceId} spanId=${parent.spanId} name=${parentExported.name}")
        println("  Child:  traceId=${serverSpan.traceId} spanId=${serverSpan.spanId} parentSpanId=${serverSpan.parentSpanId} name=${serverSpan.name}")
    }

    @Test
    fun testFarewellCreatesChildSpanOfParent() {
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-farewell-client")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)

        val response = stub.farewell(
            FarewellRequest.newBuilder().setName("Alice").setUserId("test-user").setTenantId("test-tenant").build(),
        )

        assertEquals("Goodbye, Alice! See you next time.", response.message)
        parent.span.end()
        Thread.sleep(300)

        val spans = spanExporter.finishedSpanItems
        val serverSpan = spans.find {
            it.name.contains("GreetingService/Farewell") && it.kind == SpanKind.SERVER
        }
        assertTrue(serverSpan != null, "Expected a SERVER span for GreetingService/Farewell")
        assertEquals(parent.traceId, serverSpan!!.traceId, "Child span must share parent's traceId")
        assertEquals(parent.spanId, serverSpan.parentSpanId, "Child span's parentSpanId must be the parent's spanId")
        assertNotEquals(parent.spanId, serverSpan.spanId, "Child span must have its own spanId")

        println("=== Parent-Child Span Relationship (Farewell) ===")
        println("  Parent: traceId=${parent.traceId} spanId=${parent.spanId}")
        println("  Child:  traceId=${serverSpan.traceId} spanId=${serverSpan.spanId} parentSpanId=${serverSpan.parentSpanId} name=${serverSpan.name}")
    }

    @Test
    fun testGreetStreamCreatesChildSpanOfParent() {
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-stream-client")
        val stub = GrpcTestSupport.asyncStub(channel, traceparent = parent.traceparent)

        val responses = mutableListOf<GreetResponse>()
        val latch = CountDownLatch(1)

        stub.greetStream(
            GreetRequest.newBuilder().setName("Bob").setUserId("test-user").setTenantId("test-tenant").build(),
            object : StreamObserver<GreetResponse> {
                override fun onNext(value: GreetResponse) { responses.add(value) }
                override fun onError(t: Throwable) { latch.countDown() }
                override fun onCompleted() { latch.countDown() }
            },
        )

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Stream should complete within 10s")
        assertEquals(5, responses.size, "Expected 5 streamed responses")
        parent.span.end()
        Thread.sleep(300)

        val spans = spanExporter.finishedSpanItems
        val serverSpan = spans.find {
            it.name.contains("GreetingService/GreetStream") && it.kind == SpanKind.SERVER
        }
        assertTrue(serverSpan != null, "Expected a SERVER span for GreetingService/GreetStream")
        assertEquals(parent.traceId, serverSpan!!.traceId, "Stream child span must share parent's traceId")
        assertEquals(parent.spanId, serverSpan.parentSpanId, "Stream child span's parentSpanId must be the parent's spanId")

        println("=== Parent-Child Span Relationship (GreetStream) ===")
        println("  Parent: traceId=${parent.traceId} spanId=${parent.spanId}")
        println("  Child:  traceId=${serverSpan.traceId} spanId=${serverSpan.spanId} parentSpanId=${serverSpan.parentSpanId} name=${serverSpan.name}")
    }

    // ── Context propagation across dispatcher switch ────────────────────

    @Test
    fun testContextPropagationAcrossDispatcherSwitch() = withCapturedLogs { capturedEvents ->
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-dispatcher-switch")
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-io-test", "tenant-io-test", parent.traceparent,
        )

        stub.greet(
            GreetRequest.newBuilder().setName("Charlie").setUserId("user-io-test").setTenantId("tenant-io-test").build(),
        )
        parent.span.end()

        Thread.sleep(500)

        val beforeDelayLog = capturedEvents.find {
            it.message.formattedMessage.contains("Processing on IO dispatcher (before delay)")
        }
        assertTrue(beforeDelayLog != null, "Expected log from IO dispatcher before delay")
        val beforeMdc = beforeDelayLog!!.contextData.toMap()
        assertEquals("user-io-test", beforeMdc["userId"], "userId should propagate to IO dispatcher")
        assertEquals("tenant-io-test", beforeMdc["tenantId"], "tenantId should propagate to IO dispatcher")
        assertEquals(parent.traceId, beforeMdc["trace.id"], "trace.id should match parent span's traceId on IO dispatcher")

        val afterDelayLog = capturedEvents.find {
            it.message.formattedMessage.contains("Processing on IO dispatcher (after delay)")
        }
        assertTrue(afterDelayLog != null, "Expected log from IO dispatcher after delay")
        val afterMdc = afterDelayLog!!.contextData.toMap()
        assertEquals("user-io-test", afterMdc["userId"], "userId should survive delay on IO dispatcher")
        assertEquals("tenant-io-test", afterMdc["tenantId"], "tenantId should survive delay on IO dispatcher")
        assertEquals(parent.traceId, afterMdc["trace.id"], "trace.id should survive delay on IO dispatcher")
    }

    // ── Farewell method and Error field ─────────────────────────────────

    @Test
    fun testFarewellReturnsSuccessResponse() {
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-farewell-success")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)

        val response = stub.farewell(
            FarewellRequest.newBuilder().setName("Alice").setUserId("test-user").setTenantId("test-tenant").build(),
        )

        assertEquals("Goodbye, Alice! See you next time.", response.message)
        assertTrue(response.requestId.isNotBlank())
        assertFalse(response.hasError(), "Successful farewell should not have an error")
        parent.span.end()
    }

    @Test
    fun testFarewellReturnsErrorForUnknownUser() {
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-farewell-404")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)

        val response = stub.farewell(
            FarewellRequest.newBuilder()
                .setName(ErrorTriggers.FAREWELL_NOT_FOUND_NAME)
                .setUserId("test-user").setTenantId("test-tenant").build(),
        )

        assertTrue(response.hasError(), "Farewell for '${ErrorTriggers.FAREWELL_NOT_FOUND_NAME}' should have an error")
        assertEquals(404, response.error.httpCode)
        assertEquals("User not found", response.error.reason)
        parent.span.end()
    }

    @Test
    fun testGreetReturnsErrorForErrorName() {
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-greet-500")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)

        val response = stub.greet(
            GreetRequest.newBuilder()
                .setName(ErrorTriggers.GREET_ERROR_NAME)
                .setUserId("test-user").setTenantId("test-tenant").build(),
        )

        assertTrue(response.hasError(), "Greet for '${ErrorTriggers.GREET_ERROR_NAME}' should have an error")
        assertEquals(500, response.error.httpCode)
        assertEquals("Simulated internal error for testing", response.error.reason)
        parent.span.end()
    }

    // ── Logging interceptor ─────────────────────────────────────────────

    @Test
    fun testLoggingInterceptorLogsStartAndEndForSuccessfulCall() = withCapturedLogs { capturedEvents ->
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-log-success")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)
        stub.greet(
            GreetRequest.newBuilder().setName("LogTest").setUserId("test-user").setTenantId("test-tenant").build(),
        )
        parent.span.end()

        Thread.sleep(500)

        assertTrue(
            capturedEvents.any { it.message.formattedMessage.contains("gRPC START") && it.message.formattedMessage.contains("GreetingService/Greet") },
            "Expected 'gRPC START' log for Greet",
        )
        assertTrue(
            capturedEvents.any { it.message.formattedMessage.contains("gRPC END") && it.message.formattedMessage.contains("GreetingService/Greet") && it.message.formattedMessage.contains("OK") },
            "Expected 'gRPC END ... OK' log for successful Greet",
        )
    }

    @Test
    fun testLoggingInterceptorLogsFailureForErrorResponse() = withCapturedLogs { capturedEvents ->
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-log-error")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)
        stub.greet(
            GreetRequest.newBuilder().setName(ErrorTriggers.GREET_ERROR_NAME).setUserId("test-user").setTenantId("test-tenant").build(),
        )
        parent.span.end()

        Thread.sleep(500)

        val endLog = capturedEvents.find {
            it.message.formattedMessage.contains("gRPC END") &&
                it.message.formattedMessage.contains("GreetingService/Greet") &&
                it.message.formattedMessage.contains("httpCode=500")
        }
        assertTrue(endLog != null, "Expected 'gRPC END' log with httpCode=500 for error response")
        assertEquals(org.apache.logging.log4j.Level.WARN, endLog!!.level, "Error response should be logged at WARN level")
    }

    @Test
    fun testLoggingInterceptorLogsFarewellStartAndEnd() = withCapturedLogs { capturedEvents ->
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-log-farewell")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)
        stub.farewell(
            FarewellRequest.newBuilder().setName("LogFarewell").setUserId("test-user").setTenantId("test-tenant").build(),
        )
        parent.span.end()

        Thread.sleep(500)

        assertTrue(
            capturedEvents.any { it.message.formattedMessage.contains("gRPC START") && it.message.formattedMessage.contains("GreetingService/Farewell") },
            "Expected 'gRPC START' log for Farewell",
        )
        assertTrue(
            capturedEvents.any { it.message.formattedMessage.contains("gRPC END") && it.message.formattedMessage.contains("GreetingService/Farewell") && it.message.formattedMessage.contains("OK") },
            "Expected 'gRPC END ... OK' log for successful Farewell",
        )
    }

    @Test
    fun testLoggingInterceptorLogsFarewellFailure() = withCapturedLogs { capturedEvents ->
        val parent = GrpcTestSupport.createParentSpan(openTelemetry, "test-log-farewell-404")
        val stub = GrpcTestSupport.blockingStub(channel, traceparent = parent.traceparent)
        stub.farewell(
            FarewellRequest.newBuilder().setName(ErrorTriggers.FAREWELL_NOT_FOUND_NAME).setUserId("test-user").setTenantId("test-tenant").build(),
        )
        parent.span.end()

        Thread.sleep(500)

        val endLog = capturedEvents.find {
            it.message.formattedMessage.contains("gRPC END") &&
                it.message.formattedMessage.contains("GreetingService/Farewell") &&
                it.message.formattedMessage.contains("httpCode=404")
        }
        assertTrue(endLog != null, "Expected 'gRPC END' log with httpCode=404 for unknown user")
        assertEquals(org.apache.logging.log4j.Level.WARN, endLog!!.level, "404 response should be logged at WARN level")
    }
}
