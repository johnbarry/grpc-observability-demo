package com.example.grpcobservability.context

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Scope
import kotlinx.coroutines.ThreadContextElement
import kotlinx.coroutines.withContext
import org.slf4j.MDC
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import io.grpc.Context as GrpcContext
import io.opentelemetry.context.Context as OtelContext

/**
 * Registry of functions that derive MDC entries from a gRPC Context.
 *
 * Interceptors register their context keys here rather than writing to MDC directly.
 * This decouples the interceptor layer from the logging layer — adding a new
 * interceptor that stores data in gRPC Context only requires registering a provider.
 *
 * Thread safety: register() is called during class init (AuthInterceptor companion
 * init block), which runs during Spring bean creation — before any request threads
 * start. deriveFrom() is called on request threads but only reads the list.
 * This is safe because Spring's single-threaded bean initialization provides a
 * happens-before guarantee. If you need to register providers after startup,
 * switch to CopyOnWriteArrayList.
 */
object MdcProviders {
    private val providers = mutableListOf<(GrpcContext) -> Map<String, String>>()

    fun register(provider: (GrpcContext) -> Map<String, String>) {
        providers += provider
    }

    fun deriveFrom(grpcContext: GrpcContext): Map<String, String> = buildMap {
        providers.forEach { putAll(it(grpcContext)) }
    }
}

/**
 * A single ThreadContextElement that propagates gRPC Context, OTel Context,
 * and SLF4J MDC across coroutine thread switches.
 *
 * On each resumption this element:
 *  1. Attaches the captured gRPC Context (so Context.Key.get() works)
 *  2. Makes the captured OTel Context current (so Span.current() works)
 *  3. Derives MDC from registered [MdcProviders] + OTel span + extra fields
 *
 * This replaces the need for three separate context elements (MDCContext,
 * GrpcContextElement, Span.asContextElement). MDC is derived — never
 * independently propagated — so it cannot become stale or inconsistent.
 */
class ObservabilityContext private constructor(
    private val grpcContext: GrpcContext,
    private val otelContext: OtelContext,
    private val extraFields: Map<String, String>,
) : ThreadContextElement<ObservabilityContext.ThreadState> {

    constructor(
        grpcContext: GrpcContext = GrpcContext.current(),
        otelContext: OtelContext = OtelContext.current(),
    ) : this(grpcContext, otelContext, emptyMap())

    companion object Key : CoroutineContext.Key<ObservabilityContext>

    override val key: CoroutineContext.Key<ObservabilityContext> get() = Key

    data class ThreadState(
        val previousGrpcContext: GrpcContext,
        val previousOtelScope: Scope,
        val previousMdc: Map<String, String?>,
    )

    override fun updateThreadContext(context: CoroutineContext): ThreadState {
        // 1. Attach gRPC context
        val previousGrpc = GrpcContext.current()
        grpcContext.attach()

        // 2. Make OTel context current (so Span.current() returns the right span)
        val previousOtelScope = otelContext.makeCurrent()

        // 3. Derive MDC from all sources
        val entries = buildMdcEntries()
        val previousMdc = entries.keys.associateWith { MDC.get(it) }
        entries.forEach { (k, v) -> MDC.put(k, v) }

        return ThreadState(previousGrpc, previousOtelScope, previousMdc)
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: ThreadState) {
        oldState.previousMdc.forEach { (k, v) ->
            if (v == null) MDC.remove(k) else MDC.put(k, v)
        }
        oldState.previousOtelScope.close()
        oldState.previousGrpcContext.attach()
    }

    /** Create a child context with additional MDC fields. */
    fun withFields(vararg pairs: Pair<String, String>): ObservabilityContext =
        ObservabilityContext(grpcContext, otelContext, extraFields + pairs)

    private fun buildMdcEntries(): Map<String, String> = buildMap {
        // From gRPC context via registered providers
        putAll(MdcProviders.deriveFrom(grpcContext))

        // From OTel span — uses Span.current() which reads from the thread-local
        // installed by otelContext.makeCurrent() on line 72 above. This ordering is
        // safe because buildMdcEntries() is only called from updateThreadContext()
        // after makeCurrent() has executed.
        val spanCtx = Span.current().spanContext
        if (spanCtx.isValid) {
            put("trace.id", spanCtx.traceId)
            put("span.id", spanCtx.spanId)
        }

        // Per-request fields added via withFields()
        putAll(extraFields)
    }
}

/**
 * Captures the full observability state from the current thread into a
 * CoroutineContext element that propagates it across dispatcher switches.
 *
 * One element handles everything: gRPC Context, OTel Context, and MDC derivation.
 * No need for separate MDCContext(), Span.asContextElement(), or GrpcContextElement.
 */
fun captureObservabilityContext(): CoroutineContext =
    ObservabilityContext()

/**
 * Adds ephemeral MDC fields for the duration of [block].
 * Fields are scoped — they appear inside the block and are removed when it exits.
 * Base fields (userId, tenantId, trace.id) are unaffected.
 */
suspend fun <T> withFields(vararg pairs: Pair<String, String>, block: suspend () -> T): T {
    val current = coroutineContext[ObservabilityContext]
        ?: ObservabilityContext()
    return withContext(current.withFields(*pairs)) {
        block()
    }
}
