package com.example.grpcobservability

import com.example.grpcobservability.proto.GreetRequest
import com.example.grpcobservability.proto.GreetResponse
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
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

    private fun greetRequest(name: String): GreetRequest =
        GreetRequest.newBuilder().setName(name).setUserId("test-user").setTenantId("test-tenant").build()

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
        val response = GrpcTestSupport.blockingStub(channel).greet(greetRequest("MetricsTest"))
        assertEquals("Hello, MetricsTest!", response.message)

        Thread.sleep(300)

        val timers = meterRegistry.find("grpc.server").timers()
        assertTrue(timers.isNotEmpty(), "Expected grpc.server timer to be registered by observation interceptor")

        val timer = timers.first()
        assertTrue(timer.count() > 0, "Timer should have recorded at least one call")
        assertTrue(timer.totalTime(TimeUnit.MILLISECONDS) > 0, "Timer should have recorded non-zero duration")

        assertEquals("Greet", timer.id.getTag("rpc.method"), "rpc.method tag should be 'Greet'")
        assertEquals("greeting.GreetingService", timer.id.getTag("rpc.service"), "rpc.service tag should match proto service")
        assertEquals("OK", timer.id.getTag("grpc.status_code"), "grpc.status_code should be OK for successful call")

        println("=== Unary gRPC Metrics ===")
        println("  Timer: grpc.server")
        println("  Count: ${timer.count()}")
        println("  Total time: ${"%.2f".format(timer.totalTime(TimeUnit.MILLISECONDS))}ms")
        println("  Mean: ${"%.2f".format(timer.mean(TimeUnit.MILLISECONDS))}ms")
        println("  Tags: rpc.method=${timer.id.getTag("rpc.method")}, rpc.service=${timer.id.getTag("rpc.service")}, grpc.status_code=${timer.id.getTag("grpc.status_code")}")
    }

    // ── Micrometer metrics from streaming gRPC call ─────────────────────

    @Test
    fun `streaming gRPC call produces grpc server timer metric`() {
        val responses = mutableListOf<GreetResponse>()
        val latch = CountDownLatch(1)

        GrpcTestSupport.asyncStub(channel).greetStream(greetRequest("StreamMetrics"), object : StreamObserver<GreetResponse> {
            override fun onNext(value: GreetResponse) { responses.add(value) }
            override fun onError(t: Throwable) { latch.countDown() }
            override fun onCompleted() { latch.countDown() }
        })

        assertTrue(latch.await(10, TimeUnit.SECONDS), "Stream should complete")
        assertEquals(5, responses.size)

        Thread.sleep(300)

        val streamTimer = meterRegistry.find("grpc.server").timers()
            .find { it.id.getTag("rpc.method") == "GreetStream" }
        assertNotNull(streamTimer, "Expected grpc.server timer for GreetStream method")
        assertTrue(streamTimer!!.count() > 0, "Stream timer should have recorded at least one call")
        assertEquals("greeting.GreetingService", streamTimer.id.getTag("rpc.service"))
        assertEquals("OK", streamTimer.id.getTag("grpc.status_code"))

        println("=== Streaming gRPC Metrics ===")
        println("  Timer: grpc.server (GreetStream)")
        println("  Count: ${streamTimer.count()}")
        println("  Total time: ${"%.2f".format(streamTimer.totalTime(TimeUnit.MILLISECONDS))}ms")
        println("  Tags: rpc.method=GreetStream, rpc.service=${streamTimer.id.getTag("rpc.service")}, grpc.status_code=${streamTimer.id.getTag("grpc.status_code")}")
    }

    // ── OTel spans from observation bridge ───────────────────────────────

    @Test
    fun `observation bridge produces OTel spans for gRPC calls`() {
        GrpcTestSupport.blockingStub(channel).greet(greetRequest("SpanBridgeTest"))

        Thread.sleep(300)

        val spans = spanExporter.finishedSpanItems
        assertTrue(spans.isNotEmpty(), "Expected OTel spans to be created via observation bridge")

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
        val stub = GrpcTestSupport.blockingStub(channel)
        repeat(3) { stub.greet(greetRequest("Meter-$it")) }

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
