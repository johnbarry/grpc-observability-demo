package com.example.grpcobservability

import com.example.grpcobservability.proto.GreetRequest
import com.example.grpcobservability.proto.GreetResponse
import com.example.grpcobservability.proto.GreetingServiceGrpc
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import io.grpc.stub.StreamObserver
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.observation.ObservationRegistry
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Verifies that Spring gRPC's built-in ObservationGrpcServerInterceptor
 * auto-instruments gRPC calls — producing both Micrometer metrics and
 * OTel traces without any manual interceptor code.
 *
 * The spring-grpc starter registers ObservationGrpcServerInterceptor at @Order(0)
 * when spring-boot-starter-actuator is on the classpath. This interceptor creates
 * Micrometer Observations for every gRPC call, which in turn produce:
 *   - Timer metrics (grpc.server) with tags: rpc.method, rpc.service, grpc.status_code
 *   - OTel spans via the micrometer-tracing-bridge-otel bridge
 */
@SpringBootTest
@Import(TestOtelConfig::class)
class AutoInstrumentationTest {

    @Autowired
    lateinit var meterRegistry: MeterRegistry

    @Autowired
    lateinit var spanExporter: InMemorySpanExporter

    @Autowired
    lateinit var observationRegistry: ObservationRegistry

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
    fun reset() {
        spanExporter.reset()
    }

    private fun buildBlockingStub(): GreetingServiceGrpc.GreetingServiceBlockingStub {
        val metadata = Metadata().apply {
            put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), "test-user")
            put(Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER), "test-tenant")
        }
        return GreetingServiceGrpc.newBlockingStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
    }

    private fun buildAsyncStub(): GreetingServiceGrpc.GreetingServiceStub {
        val metadata = Metadata().apply {
            put(Metadata.Key.of("x-user-id", Metadata.ASCII_STRING_MARSHALLER), "test-user")
            put(Metadata.Key.of("x-tenant-id", Metadata.ASCII_STRING_MARSHALLER), "test-tenant")
        }
        return GreetingServiceGrpc.newStub(channel)
            .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
    }

    // ── Verify ObservationRegistry is active ────────────────────────────

    @Test
    fun `ObservationRegistry is configured and not no-op`() {
        assertNotNull(observationRegistry)
        assertTrue(
            observationRegistry != ObservationRegistry.NOOP,
            "ObservationRegistry should not be NOOP — actuator must be on the classpath",
        )
    }

    // ── Micrometer metrics from unary gRPC call ─────────────────────────

    @Test
    fun `unary gRPC call produces grpc server timer metric`() {
        val request = GreetRequest.newBuilder()
            .setName("MetricsTest")
            .setUserId("test-user")
            .setTenantId("test-tenant")
            .build()

        val response = buildBlockingStub().greet(request)
        assertEquals("Hello, MetricsTest!", response.message)

        // Allow async observation to complete
        Thread.sleep(300)

        // The ObservationGrpcServerInterceptor creates a `grpc.server` timer
        val timers = meterRegistry.find("grpc.server").timers()
        assertTrue(timers.isNotEmpty(), "Expected grpc.server timer to be registered by observation interceptor")

        val timer = timers.first()
        assertTrue(timer.count() > 0, "Timer should have recorded at least one call")
        assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) > 0, "Timer should have recorded non-zero duration")

        // Verify tags
        val methodTag = timer.id.getTag("rpc.method")
        val serviceTag = timer.id.getTag("rpc.service")
        val statusTag = timer.id.getTag("grpc.status_code")

        assertEquals("Greet", methodTag, "rpc.method tag should be 'Greet'")
        assertEquals("greeting.GreetingService", serviceTag, "rpc.service tag should match proto service")
        assertEquals("OK", statusTag, "grpc.status_code should be OK for successful call")

        println("=== Unary gRPC Metrics ===")
        println("  Timer: grpc.server")
        println("  Count: ${timer.count()}")
        println("  Total time: ${"%.2f".format(timer.totalTime(TimeUnit.MILLISECONDS))}ms")
        println("  Mean: ${"%.2f".format(timer.mean(TimeUnit.MILLISECONDS))}ms")
        println("  Tags: rpc.method=$methodTag, rpc.service=$serviceTag, grpc.status_code=$statusTag")
    }

    // ── Micrometer metrics from streaming gRPC call ─────────────────────

    @Test
    fun `streaming gRPC call produces grpc server timer metric`() {
        val request = GreetRequest.newBuilder()
            .setName("StreamMetrics")
            .setUserId("test-user")
            .setTenantId("test-tenant")
            .build()

        val responses = mutableListOf<GreetResponse>()
        val latch = CountDownLatch(1)

        buildAsyncStub().greetStream(request, object : StreamObserver<GreetResponse> {
            override fun onNext(value: GreetResponse) { responses.add(value) }
            override fun onError(t: Throwable) { latch.countDown() }
            override fun onCompleted() { latch.countDown() }
        })

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Stream should complete")
        assertEquals(5, responses.size)

        Thread.sleep(300)

        val timers = meterRegistry.find("grpc.server").timers()
        val streamTimer = timers.find { it.id.getTag("rpc.method") == "GreetStream" }
        assertNotNull(streamTimer, "Expected grpc.server timer for GreetStream method")

        assertTrue(streamTimer!!.count() > 0, "Stream timer should have recorded at least one call")

        val serviceTag = streamTimer.id.getTag("rpc.service")
        val statusTag = streamTimer.id.getTag("grpc.status_code")

        assertEquals("greeting.GreetingService", serviceTag)
        assertEquals("OK", statusTag)

        println("=== Streaming gRPC Metrics ===")
        println("  Timer: grpc.server (GreetStream)")
        println("  Count: ${streamTimer.count()}")
        println("  Total time: ${"%.2f".format(streamTimer.totalTime(TimeUnit.MILLISECONDS))}ms")
        println("  Tags: rpc.method=GreetStream, rpc.service=$serviceTag, grpc.status_code=$statusTag")
    }

    // ── OTel spans from observation bridge ───────────────────────────────

    @Test
    fun `observation bridge produces OTel spans for gRPC calls`() {
        val request = GreetRequest.newBuilder()
            .setName("SpanBridgeTest")
            .setUserId("test-user")
            .setTenantId("test-tenant")
            .build()

        buildBlockingStub().greet(request)

        Thread.sleep(300)

        val spans = spanExporter.finishedSpanItems
        assertTrue(spans.isNotEmpty(), "Expected OTel spans to be created via observation bridge")

        // The observation interceptor creates a span with the full method name
        // Our manual OtelGrpcInterceptor also creates one — we expect at least one from each
        println("=== OTel Spans After Unary Call ===")
        spans.forEach { span ->
            println("  Span: name='${span.name}', traceId=${span.traceId}, kind=${span.kind}")
            span.attributes.forEach { key, value ->
                println("    ${key.key} = $value")
            }
        }

        assertTrue(spans.size >= 1, "Expected at least one span from auto-instrumentation")
    }

    // ── All registered grpc meters ──────────────────────────────────────

    @Test
    fun `list all grpc-related meters after multiple calls`() {
        // Make several calls to populate metrics
        val stub = buildBlockingStub()
        repeat(3) {
            stub.greet(
                GreetRequest.newBuilder()
                    .setName("Meter-$it")
                    .setUserId("test-user")
                    .setTenantId("test-tenant")
                    .build(),
            )
        }

        Thread.sleep(300)

        val grpcMeters = meterRegistry.meters.filter { it.id.name.startsWith("grpc.") }
        assertTrue(grpcMeters.isNotEmpty(), "Expected grpc.* meters to exist")

        println("=== All gRPC-related Meters ===")
        grpcMeters
            .map { it.id }
            .distinctBy { "${it.name}:${it.tags}" }
            .sortedBy { it.name }
            .forEach { id ->
                val tags = id.tags.joinToString(", ") { "${it.key}=${it.value}" }
                println("  ${id.name} [${id.type}] $tags")
            }
    }
}
