package com.example.grpcobservability

import com.example.grpcobservability.proto.FarewellRequest
import com.example.grpcobservability.proto.GreetRequest
import com.example.grpcobservability.proto.GreetResponse
import com.example.grpcobservability.service.ErrorTriggers
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
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

    // ── Context propagation ─────────────────────────────────────────────

    @Test
    fun testGreetPopulatesFullContext() {
        val knownTraceId = "0af7651916cd43dd8448eb211c80319c"
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-42", "tenant-abc", GrpcTestSupport.traceparent(knownTraceId),
        )

        val response = stub.greet(
            GreetRequest.newBuilder().setName("Alice").setUserId("user-42").setTenantId("tenant-abc").build(),
        )

        assertEquals("Hello, Alice!", response.message)
        assertTrue(response.requestId.isNotBlank())

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
        val stub = GrpcTestSupport.asyncStub(
            channel, "user-stream", "tenant-stream", GrpcTestSupport.traceparent(knownTraceId),
        )

        val responses = mutableListOf<GreetResponse>()
        val latch = CountDownLatch(1)

        stub.greetStream(
            GreetRequest.newBuilder().setName("Bob").setUserId("user-stream").setTenantId("tenant-stream").build(),
            object : StreamObserver<GreetResponse> {
                override fun onNext(value: GreetResponse) { responses.add(value) }
                override fun onError(t: Throwable) { latch.countDown() }
                override fun onCompleted() { latch.countDown() }
            },
        )

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Stream should complete within 10s")
        assertEquals(5, responses.size, "Expected 5 streamed responses")

        Thread.sleep(200)

        val spans = spanExporter.finishedSpanItems
        assertTrue(spans.isNotEmpty(), "Expected spans to be exported for streaming call")
        val streamSpan = spans.find { it.name.contains("GreetStream") }
        assertTrue(streamSpan != null, "Expected a span named containing 'GreetStream'")
        assertEquals(knownTraceId, streamSpan!!.traceId, "Stream span trace ID should match injected traceparent")
    }

    @Test
    fun testContextPropagationAcrossDispatcherSwitch() = withCapturedLogs { capturedEvents ->
        val knownTraceId = "2af7651916cd43dd8448eb211c80319c"
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-io-test", "tenant-io-test", GrpcTestSupport.traceparent(knownTraceId),
        )

        stub.greet(
            GreetRequest.newBuilder().setName("Charlie").setUserId("user-io-test").setTenantId("tenant-io-test").build(),
        )

        Thread.sleep(500)

        val beforeDelayLog = capturedEvents.find {
            it.message.formattedMessage.contains("Processing on IO dispatcher (before delay)")
        }
        assertTrue(beforeDelayLog != null, "Expected log from IO dispatcher before delay")
        val beforeMdc = beforeDelayLog!!.contextData.toMap()
        assertEquals("user-io-test", beforeMdc["userId"], "userId should propagate to IO dispatcher")
        assertEquals("tenant-io-test", beforeMdc["tenantId"], "tenantId should propagate to IO dispatcher")
        assertTrue(beforeMdc["trace.id"]?.isNotBlank() == true, "trace.id should be present on IO dispatcher")

        val afterDelayLog = capturedEvents.find {
            it.message.formattedMessage.contains("Processing on IO dispatcher (after delay)")
        }
        assertTrue(afterDelayLog != null, "Expected log from IO dispatcher after delay")
        val afterMdc = afterDelayLog!!.contextData.toMap()
        assertEquals("user-io-test", afterMdc["userId"], "userId should survive delay on IO dispatcher")
        assertEquals("tenant-io-test", afterMdc["tenantId"], "tenantId should survive delay on IO dispatcher")
        assertTrue(afterMdc["trace.id"]?.isNotBlank() == true, "trace.id should survive delay on IO dispatcher")
    }

    // ── Farewell method and Error field ─────────────────────────────────

    @Test
    fun testFarewellReturnsSuccessResponse() {
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-42", "tenant-abc", GrpcTestSupport.traceparent("3af7651916cd43dd8448eb211c80319c"),
        )

        val response = stub.farewell(
            FarewellRequest.newBuilder().setName("Alice").setUserId("user-42").setTenantId("tenant-abc").build(),
        )

        assertEquals("Goodbye, Alice! See you next time.", response.message)
        assertTrue(response.requestId.isNotBlank())
        assertFalse(response.hasError(), "Successful farewell should not have an error")
    }

    @Test
    fun testFarewellReturnsErrorForUnknownUser() {
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-42", "tenant-abc", GrpcTestSupport.traceparent("4af7651916cd43dd8448eb211c80319c"),
        )

        val response = stub.farewell(
            FarewellRequest.newBuilder()
                .setName(ErrorTriggers.FAREWELL_NOT_FOUND_NAME)
                .setUserId("user-42").setTenantId("tenant-abc").build(),
        )

        assertTrue(response.hasError(), "Farewell for '${ErrorTriggers.FAREWELL_NOT_FOUND_NAME}' should have an error")
        assertEquals(404, response.error.httpCode)
        assertEquals("User not found", response.error.reason)
    }

    @Test
    fun testGreetReturnsErrorForErrorName() {
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-42", "tenant-abc", GrpcTestSupport.traceparent("5af7651916cd43dd8448eb211c80319c"),
        )

        val response = stub.greet(
            GreetRequest.newBuilder()
                .setName(ErrorTriggers.GREET_ERROR_NAME)
                .setUserId("user-42").setTenantId("tenant-abc").build(),
        )

        assertTrue(response.hasError(), "Greet for '${ErrorTriggers.GREET_ERROR_NAME}' should have an error")
        assertEquals(500, response.error.httpCode)
        assertEquals("Simulated internal error for testing", response.error.reason)
    }

    // ── Logging interceptor ─────────────────────────────────────────────

    @Test
    fun testLoggingInterceptorLogsStartAndEndForSuccessfulCall() = withCapturedLogs { capturedEvents ->
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-42", "tenant-abc", GrpcTestSupport.traceparent("6af7651916cd43dd8448eb211c80319c"),
        )
        stub.greet(
            GreetRequest.newBuilder().setName("LogTest").setUserId("user-42").setTenantId("tenant-abc").build(),
        )

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
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-42", "tenant-abc", GrpcTestSupport.traceparent("7af7651916cd43dd8448eb211c80319c"),
        )
        stub.greet(
            GreetRequest.newBuilder().setName(ErrorTriggers.GREET_ERROR_NAME).setUserId("user-42").setTenantId("tenant-abc").build(),
        )

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
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-42", "tenant-abc", GrpcTestSupport.traceparent("8af7651916cd43dd8448eb211c80319c"),
        )
        stub.farewell(
            FarewellRequest.newBuilder().setName("LogFarewell").setUserId("user-42").setTenantId("tenant-abc").build(),
        )

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
        val stub = GrpcTestSupport.blockingStub(
            channel, "user-42", "tenant-abc", GrpcTestSupport.traceparent("9af7651916cd43dd8448eb211c80319c"),
        )
        stub.farewell(
            FarewellRequest.newBuilder().setName(ErrorTriggers.FAREWELL_NOT_FOUND_NAME).setUserId("user-42").setTenantId("tenant-abc").build(),
        )

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
