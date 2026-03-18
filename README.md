# gRPC Observability Demo

A Spring Boot 4.0 / Kotlin project that solves a deceptively hard problem: making sure your log lines contain useful information like `userId`, `tenantId`, and `traceId` — even when your code is running across different threads in Kotlin coroutines.

## How to Run

```bash
# Plain text logging (default)
LOG_FORMAT=PLAIN ./gradlew bootRun

# JSON (ECS) logging for production (Elasticsearch-compatible)
LOG_FORMAT=JSON ./gradlew bootRun

# Run tests
./gradlew test
```

---

## The Problem This Project Solves

### What is "context" and why does it matter?

When a request arrives at your server, you know things about it: who sent it (`userId`), which tenant it belongs to (`tenantId`), and a trace ID that lets you follow this request across multiple services. This is **context** — metadata about the current operation.

You want every log line to include this context so that when something goes wrong in production, you can filter your logs by `userId=user-42` or `traceId=abc123` and see exactly what happened for that user or that request.

### The easy version (no coroutines)

In a traditional thread-per-request server, this is simple. Each request gets its own thread, and Java provides `ThreadLocal` — a variable that has a separate value for each thread. You store `userId` in a `ThreadLocal` at the start of the request, and every piece of code running on that thread can read it. SLF4J's MDC (Mapped Diagnostic Context) is exactly this: a `ThreadLocal<Map<String, String>>` that logging frameworks automatically include in every log line.

```
Thread-1 handles Request A:  MDC = {userId=alice, traceId=aaa}
Thread-2 handles Request B:  MDC = {userId=bob,   traceId=bbb}
```

Every log line on Thread-1 automatically gets `userId=alice`. Simple.

### The hard version (with coroutines)

Kotlin coroutines break this model. A coroutine can **suspend** (pause) on one thread and **resume** (continue) on a completely different thread. This is by design — it's how coroutines efficiently share a small pool of threads across many concurrent operations.

```
Time 0:  Thread-1 runs Coroutine A     MDC = {userId=alice}
Time 1:  Coroutine A suspends (e.g., calls delay() or does I/O)
Time 2:  Thread-1 runs Coroutine B     MDC = {userId=bob}    ← Thread-1 is reused!
Time 3:  Coroutine A resumes on Thread-3   MDC = {}           ← alice is GONE
```

When Coroutine A resumes on Thread-3, the MDC is empty (or worse, contains data from whatever was running on Thread-3 before). Your log line says `userId=null` or `userId=someone-else`. This is **context loss**, and it's the central problem this project solves.

### It's not just MDC — there are three thread-local systems

This project uses three libraries that each store state in thread-locals:

| System | What it stores | Why it exists |
|--------|---------------|---------------|
| **SLF4J MDC** | Key-value pairs (`userId`, `tenantId`, `trace.id`) | So logging frameworks automatically include these fields in every log line |
| **gRPC Context** | Typed keys set by interceptors (`Context.Key<String>`) | So service code can read request metadata (like auth info) that interceptors extracted from gRPC headers |
| **OpenTelemetry Context** | The current Span (a unit of tracing) | So when you create child spans or log events, they're correctly linked to the parent trace |

All three break when a coroutine moves between threads. All three need to be "carried along" with the coroutine.

### What is "context propagation"?

**Context propagation** is the act of carrying thread-local state across a boundary where it would otherwise be lost. In this project, that boundary is a coroutine thread switch.

Kotlin provides a mechanism for this: `ThreadContextElement`. It's a coroutine context element with two hooks:
- `updateThreadContext()` — called when a coroutine is about to run on a thread. You **install** your state into the thread-locals here.
- `restoreThreadContext()` — called when the coroutine is done with this thread (it suspended, completed, or another coroutine is about to run). You **clean up** by restoring whatever was in the thread-locals before.

Every time a coroutine hops to a new thread, these hooks fire, keeping the thread-local state consistent with the coroutine that's actually running.

---

## How This Project Works

### The architecture: interceptors set context, one element propagates everything

```
Client sends gRPC request with headers:
  traceparent: 00-abc123...-01
  x-user-id: alice
  x-tenant-id: acme

        |
        v

  OtelGrpcInterceptor (runs first)
    - Reads "traceparent" header
    - Creates an OTel Span and makes it the current span (thread-local)
    - Does NOT write to MDC

        |
        v

  AuthInterceptor (runs second)
    - Reads "x-user-id" and "x-tenant-id" headers
    - Stores them as gRPC Context keys (thread-local)
    - Does NOT write to MDC

        |
        v

  GreetingServiceImpl.greet()
    - Calls responseObserver.respondWith { ... }
    - respondWith captures an ObservabilityContext snapshot
      (records the current gRPC Context + OTel Context)
    - Launches a coroutine with that snapshot

        |  the coroutine may run on any thread
        v

  ObservabilityContext.updateThreadContext() fires on each thread
    1. Attaches the gRPC Context → Context.Key.get() works
    2. Makes OTel Context current → Span.current() works
    3. Reads userId/tenantId from gRPC Context keys
    4. Reads trace.id/span.id from the current OTel Span
    5. Writes all of it into MDC

        |
        v

  Log4j2 reads MDC → every log line has userId, tenantId, trace.id
```

The key insight: **interceptors only manage their own native context**. The `OtelGrpcInterceptor` manages OTel spans. The `AuthInterceptor` manages gRPC Context keys. Neither one writes to MDC. MDC is populated by `ObservabilityContext` as a derived view — recomputed fresh on every thread switch.

### Why derive MDC instead of propagating it independently?

The common approach is to have interceptors write to MDC directly, then use a separate `MDCContext()` element to snapshot and restore those MDC values across thread switches. This has problems:

- **Scattered writes**: MDC is written in interceptors, cleaned up in `close()` handlers, and snapshotted by the coroutine element. If any of these are missed or happen in the wrong order, MDC becomes stale.
- **Stale values on thread reuse**: If the `MDCContext` restores a snapshot but the gRPC Context has changed (e.g., a new interceptor added a key), MDC won't reflect the change.
- **Manual cleanup**: Every `MDC.put()` needs a matching `MDC.remove()` in a `finally` block, or you leak values to other requests sharing that thread.

By deriving MDC from the other contexts on every thread attach, we eliminate all of these issues. MDC is always consistent with the actual state of the gRPC and OTel contexts.

### File-by-file guide

```
src/main/kotlin/com/example/grpcobservability/
  context/
    ObservabilityContext.kt   -- The single ThreadContextElement that propagates
                                 gRPC Context, OTel Context, and derives MDC.
                                 Also contains MdcProviders registry, withFields(),
                                 and captureObservabilityContext().
    GrpcCoroutineScope.kt     -- respondWith() and streamWith() extension functions
                                 on StreamObserver that capture context and launch
                                 coroutines with error handling.
  interceptor/
    OtelGrpcInterceptor.kt    -- Extracts W3C traceparent from gRPC headers, starts
                                 an OTel server span. Does not touch MDC.
    AuthInterceptor.kt        -- Extracts x-user-id and x-tenant-id from gRPC headers,
                                 stores in gRPC Context. Registers an MdcProvider so
                                 ObservabilityContext knows how to derive MDC from these.
  service/
    GreetingServiceImpl.kt    -- The gRPC service. Uses respondWith/streamWith to handle
                                 requests in coroutines with full context propagation.
  config/
    GrpcConfig.kt             -- Registers the two interceptors as Spring beans.
```

### The `withFields` helper

Sometimes you need to add per-request MDC fields that don't come from interceptors — like a `requestId` generated inside your service code, or a `streamIndex` for the Nth response in a stream.

`withFields("requestId" to "abc-123")` creates a child `ObservabilityContext` with extra MDC entries. These entries appear in log lines inside the block and are automatically cleaned up when the block exits:

```kotlin
withFields("requestId" to requestId, "rpc.method" to "Greet") {
    logger.info { "Processing" }  // MDC has userId + tenantId + trace.id + requestId + rpc.method
}
logger.info { "Done" }  // MDC has userId + tenantId + trace.id (requestId is gone)
```

### Why `delay()` matters in the tests

In the tests, you'll see `delay(10)` calls inside `withContext(Dispatchers.IO)`. This isn't just simulating work. `withContext(Dispatchers.IO)` moves the coroutine to the IO thread pool, but `delay()` inside it **suspends** the coroutine, which means it may resume on a *different thread within that pool*. This is the hardest case for context propagation — the coroutine doesn't just switch pools once, it potentially switches threads twice: once for `withContext`, once after `delay`. The tests assert that MDC, gRPC Context, and OTel trace ID are all still correct after this double switch.

---

## Why We Didn't Use These Libraries/Approaches

### `kotlinx-coroutines-slf4j` (MDCContext)

This library provides `MDCContext()`, a `ThreadContextElement` that snapshots the current MDC map and restores it on thread switches. We evaluated it and removed it because:

- It propagates MDC as an **independent snapshot**. If the gRPC Context changes but the MDC snapshot is stale, they diverge.
- It requires interceptors to write to MDC directly (the "scattered writes" problem).
- Our `ObservabilityContext` derives MDC from the source-of-truth contexts, making `MDCContext` redundant.

### `opentelemetry-extension-kotlin` (Span.asContextElement)

This library provides `Span.current().asContextElement()`, a `ThreadContextElement` that propagates the current OTel Span across coroutine thread switches. We evaluated it and removed it because:

- Our `ObservabilityContext` already captures `OtelContext.current()` and calls `makeCurrent()` on thread switches, which achieves the same thing.
- Using it alongside `ObservabilityContext` would mean two elements both trying to manage the OTel thread-local, which is confusing and potentially conflicting.
- Removing it means one fewer dependency and one fewer moving part.

### Spring Framework 7's `PropagationContextElement`

Spring Framework 7.0 (which ships with Spring Boot 4.0) introduces `PropagationContextElement`, a framework-provided `ThreadContextElement` that uses Micrometer's context-propagation library. It propagates any thread-local state that has a registered `ThreadLocalAccessor`. We evaluated it and decided against using it as our primary mechanism because:

- **It doesn't cover gRPC Context out of the box.** There's no built-in `ThreadLocalAccessor` for `io.grpc.Context`. You'd need to write a custom one and register it.
- **OTel span propagation requires a registered accessor too.** The OTel Java agent registers one automatically, but if you're using the SDK directly (as we do in tests), you need manual setup.
- **It doesn't derive MDC.** It propagates MDC as a snapshot (same problem as `MDCContext`), so you still need interceptors to write to MDC.
- **Known issue (Micrometer #5221):** In gRPC + coroutine scenarios, Micrometer's observation scope validation can emit warnings about scope mismatches caused by thread switches. Our manual approach avoids this entirely.

`PropagationContextElement` is a great choice for standard Spring WebFlux/MVC apps where Micrometer auto-configures everything. For gRPC + coroutines, our explicit approach is more reliable.

### Spring gRPC's auto-configured observation interceptors

When you add `spring-boot-starter-actuator` to a Spring gRPC project, Spring auto-configures `ObservationGrpcServerInterceptor` which handles tracing via Micrometer's Observation API. We didn't use this because:

- The goal of this project is to show the **mechanics** of context propagation, not to hide them behind auto-configuration.
- The auto-configured interceptor feeds into Micrometer, which bridges to OTel. Our manual `OtelGrpcInterceptor` talks to OTel directly, which is easier to understand and debug.
- In production, you might well use the auto-configured interceptors. This demo is about understanding what they do under the hood.

### `grpc-kotlin-stub` (Kotlin coroutine stubs)

The `grpc-kotlin` project can generate coroutine-based service stubs where each RPC method is a `suspend fun`. We used the standard Java stubs instead because:

- The Java stubs (`StreamObserver`-based) make the context propagation challenge more visible. You explicitly see where the coroutine is launched and how context is captured.
- With Kotlin stubs, the coroutine machinery is hidden inside the generated code, which makes it harder to teach what's happening.
- Our `respondWith`/`streamWith` extensions provide a clean API on top of the Java stubs while keeping the context capture explicit.

---

## What Each Test Proves

### `GreetingServiceIntegrationTest` (3 tests, full Spring Boot context)

These tests start the actual gRPC server and make real gRPC calls to it.

**`testGreetPopulatesFullContext`** — Sends a unary gRPC call with a known `traceparent` header (containing trace ID `0af765...`), `x-user-id`, and `x-tenant-id`. Asserts that the response is correct, and that an OTel span was exported with a trace ID matching the one we sent. This proves end-to-end trace propagation: client header -> interceptor -> OTel span -> exporter.

**`testGreetStreamPropagatesContext`** — Sends a server-streaming call and collects all 5 responses. Asserts that all responses arrive and that a span with the correct trace ID is exported. This proves context propagation works for streaming RPCs (not just unary).

**`testContextPropagationAcrossDispatcherSwitch`** — The most important test. Installs a custom Log4j2 appender that captures `LogEvent` objects into a queue. Makes a gRPC call, then inspects the captured log events for the ones emitted from `Dispatchers.IO` — both **before** and **after** a `delay()`. Asserts that `userId`, `tenantId`, and `trace.id` are present in the MDC of those log events. This proves that all three context systems survive a dispatcher switch AND a suspension/resumption on a potentially different thread within the IO pool.

### `MdcCoroutineTest` (4 tests, no Spring, pure unit tests)

These tests verify the context propagation mechanisms in isolation, without starting a server.

**`ObservabilityContext propagates gRPC context and OTel span across dispatcher switch and delay`** — Sets up both a gRPC Context (with userId/tenantId) and an OTel span (with a known trace ID), creates an `ObservabilityContext`, and runs a coroutine that switches to `Dispatchers.IO` with a `delay`. Asserts that MDC contains all derived fields after the switch and delay.

**`gRPC context survives dispatcher switch and delay`** — A focused test that only checks gRPC `Context.Key.get()` (not MDC). Proves the gRPC Context half of `ObservabilityContext` works correctly.

**`withFields adds ephemeral MDC entries scoped to block`** — Verifies that `withFields("requestId" to "req-123")` adds the field inside the block, that it survives `Dispatchers.IO` + `delay`, and that it's removed after the block exits — while base fields like `userId` persist.

**`single element propagates gRPC context, OTel context, and MDC together`** — The capstone unit test. Sets up both gRPC and OTel context, creates a single `ObservabilityContext()`, and asserts that all three systems (gRPC keys, OTel trace ID in MDC, gRPC Context.Key.get()) work across `Dispatchers.IO` + `delay`.

---

## Glossary

| Term | Meaning |
|------|---------|
| **MDC** | Mapped Diagnostic Context. A per-thread map of string key-value pairs that logging frameworks (Log4j2, Logback) automatically include in log output. Backed by `ThreadLocal`. |
| **ThreadLocal** | A Java mechanism where each thread has its own independent copy of a variable. Used by MDC, gRPC Context, and OTel Context. |
| **ThreadContextElement** | A Kotlin coroutines interface that lets you hook into thread switches. You install state when a coroutine arrives on a thread and clean up when it leaves. |
| **gRPC Context** | A request-scoped key-value store provided by the gRPC framework. Interceptors populate it, service code reads from it. Similar to MDC but typed (`Context.Key<T>`) and not tied to logging. |
| **OTel Span** | A unit of work in a distributed trace. Has a trace ID (shared across services) and a span ID (unique to this unit). Stored in `io.opentelemetry.context.Context`. |
| **traceparent** | A W3C standard HTTP header (`00-{traceId}-{spanId}-{flags}`) that carries trace context between services. Our `OtelGrpcInterceptor` reads this from gRPC metadata. |
| **Dispatcher** | In Kotlin coroutines, the thread pool a coroutine runs on. `Dispatchers.Default` is for CPU work, `Dispatchers.IO` is for blocking I/O. `withContext(Dispatchers.IO)` switches the coroutine to the IO pool. |
| **suspend / resume** | When a coroutine calls `delay()` or another suspending function, it **suspends** — it pauses and frees its thread. Later it **resumes**, possibly on a different thread. This is when context can be lost. |
| **Interceptor** | gRPC middleware. Runs before/after your service code. Used here to extract headers into context objects. Similar to servlet filters or Spring `HandlerInterceptor`. |
| **MdcProviders** | Our registry where interceptors declare how their gRPC Context keys map to MDC fields. `ObservabilityContext` calls all registered providers on each thread switch. |
