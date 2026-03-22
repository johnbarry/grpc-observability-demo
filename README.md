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

  ObservationGrpcServerInterceptor (@Order(0), auto-configured)
    - Creates a Micrometer Observation for the call
    - Produces grpc.server timer metrics and OTel spans via the bridge
    - Closes the observation when the call ends (via ServerCall.close()
      or ServerCall.Listener.onCancel())

        |
        v

  OtelGrpcInterceptor (@Order(1), manual)
    - Reads "traceparent" header
    - Creates an OTel Span and makes it the current span (thread-local)
    - Does NOT write to MDC

        |
        v

  AuthInterceptor (@Order(2), manual)
    - Reads "x-user-id" and "x-tenant-id" headers
    - Stores them as gRPC Context keys (thread-local)
    - Does NOT write to MDC

        |
        v

  LoggingInterceptor (@Order(3), manual)
    - Logs "gRPC START {method}" at INFO
    - Wraps ServerCall.sendMessage() to inspect every response message
      (unary or streaming) for an Error field with non-2xx http_code
    - Logs "gRPC MSG" at WARN per-message if error detected
    - Logs "gRPC END {method} — OK" at INFO, or "FAILED" at WARN

        |
        v

  GreetingServiceImpl.greet() / farewell()
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

The key insight: **interceptors only manage their own native context**. The `OtelGrpcInterceptor` manages OTel spans. The `AuthInterceptor` manages gRPC Context keys. The `LoggingInterceptor` only reads response data — it doesn't modify any context. None of them write to MDC. MDC is populated by `ObservabilityContext` as a derived view — recomputed fresh on every thread switch.

### What the logging interceptor produces

For a successful call (note the trace ID and user context in the log prefix):
```
INFO  [abc123,def456] [alice,acme] LoggingInterceptor - gRPC START greeting.GreetingService/Greet
INFO  [abc123,def456] [alice,acme] LoggingInterceptor - gRPC END   greeting.GreetingService/Greet — OK (35ms)
```

For a call whose response contains an `Error` with non-2xx `http_code`, the interceptor logs the error when the message is sent (`gRPC MSG`) and again in the end summary (`gRPC END`):
```
INFO  [abc123,ghi789] [alice,acme] LoggingInterceptor - gRPC START greeting.GreetingService/Farewell
WARN  [abc123,ghi789] [alice,acme] LoggingInterceptor - gRPC MSG   greeting.GreetingService/Farewell — response contains error: httpCode=404 reason="User not found"
WARN  [abc123,ghi789] [alice,acme] LoggingInterceptor - gRPC END   greeting.GreetingService/Farewell — FAILED httpCode=404 reason="User not found" (31ms)
```

The `LoggingInterceptor` runs on the gRPC transport thread where `ObservabilityContext` hasn't populated MDC yet. It solves this by temporarily deriving MDC entries from the current gRPC Context and OTel Span for each log call — using the same `MdcProviders` registry that `ObservabilityContext` uses — then cleaning up afterwards.

For streaming RPCs, every message is inspected individually. If any message carries an error, the per-message `gRPC MSG` log fires immediately, and the `gRPC END` summary reports the last error seen.

Note that both calls succeeded at the gRPC transport level — the `LoggingInterceptor` looks inside the response protobuf to detect domain-level failures. This is important because many monitoring systems only look at gRPC status codes, which would miss these business errors entirely.

### How gRPC interceptors manage lifecycle (not just start)

A common misconception is that gRPC interceptors only fire at the start of a call. In fact, `interceptCall()` returns a `ServerCall.Listener` and receives a `ServerCall` — the interceptor can wrap both to hook into the **full call lifecycle**:

- **`ServerCall.sendMessage()`** — called when the server sends a response. The `LoggingInterceptor` overrides this to capture the response protobuf for inspection.
- **`ServerCall.close(Status, Metadata)`** — called when the call ends. This is where the `LoggingInterceptor` logs the end of the call, and where the `ObservationGrpcServerInterceptor` stops its observation (recording the timer and closing the span).
- **`ServerCall.Listener.onCancel()`** — called if the client cancels. Both the `LoggingInterceptor` and `OtelGrpcInterceptor` use this to handle premature termination.

### Why derive MDC instead of propagating it independently?

The common approach is to have interceptors write to MDC directly, then use a separate `MDCContext()` element to snapshot and restore those MDC values across thread switches. This has problems:

- **Scattered writes**: MDC is written in interceptors, cleaned up in `close()` handlers, and snapshotted by the coroutine element. If any of these are missed or happen in the wrong order, MDC becomes stale.
- **Stale values on thread reuse**: If the `MDCContext` restores a snapshot but the gRPC Context has changed (e.g., a new interceptor added a key), MDC won't reflect the change.
- **Manual cleanup**: Every `MDC.put()` needs a matching `MDC.remove()` in a `finally` block, or you leak values to other requests sharing that thread.

By deriving MDC from the other contexts on every thread attach, we eliminate all of these issues. MDC is always consistent with the actual state of the gRPC and OTel contexts.

### The protobuf schema

The service is defined in `src/main/proto/greeting.proto`:

```protobuf
service GreetingService {
  rpc Greet (GreetRequest) returns (GreetResponse);        // unary
  rpc Farewell (FarewellRequest) returns (FarewellResponse); // unary
  rpc GreetStream (GreetRequest) returns (stream GreetResponse); // server streaming
}
```

Both unary responses include an `optional Error error` field:

```protobuf
message Error {
  int32 http_code = 1;   // e.g. 404, 500
  string reason = 2;     // human-readable explanation
}
```

This is a domain-level error — separate from the gRPC transport status. A call can succeed at the gRPC level (status `OK`) but still carry a business error in the response body. The `LoggingInterceptor` detects this pattern: it inspects every response protobuf for an `error` field, and if the `http_code` is outside the 2xx range, it logs the call as a failure at WARN level.

### File-by-file guide

```
src/main/proto/
  greeting.proto              -- Service definition with 3 RPCs (Greet, Farewell,
                                 GreetStream). Response messages carry optional Error.

src/main/kotlin/com/example/grpcobservability/
  context/
    ObservabilityContext.kt   -- The single ThreadContextElement that propagates
                                 gRPC Context, OTel Context, and derives MDC.
                                 Uses Span.current() after otelContext.makeCurrent()
                                 to read trace/span IDs for MDC derivation.
                                 Also contains MdcProviders registry, withFields(),
                                 and captureObservabilityContext().
    GrpcCoroutineScope.kt     -- respondWith() and streamWith() extension functions
                                 on StreamObserver that capture context and launch
                                 coroutines with error handling.
  interceptor/
    OtelGrpcInterceptor.kt    -- Extracts W3C traceparent from gRPC headers, starts
                                 an OTel server span. Uses AtomicBoolean guard to
                                 prevent double-close if both close() and onCancel()
                                 fire. Does not touch MDC.
    AuthInterceptor.kt        -- Extracts x-user-id and x-tenant-id from gRPC headers,
                                 stores in gRPC Context. Registers an MdcProvider so
                                 ObservabilityContext knows how to derive MDC from these.
    LoggingInterceptor.kt     -- Logs start/end of every gRPC call with full context
                                 (trace ID, span ID, userId, tenantId) by temporarily
                                 deriving MDC from the current gRPC/OTel contexts.
                                 Inspects every response message (unary and streaming)
                                 for an Error field with non-2xx http_code. Uses protobuf
                                 descriptors so it works generically across response types.
  service/
    GreetingServiceImpl.kt    -- The gRPC service (Greet, Farewell, GreetStream).
                                 Unary methods delegate to a shared handleUnary() that
                                 manages context extraction, IO dispatcher switching,
                                 and error resolution.
    ErrorTriggers.kt          -- Constants for well-known request names that trigger
                                 simulated error responses (shared between service and
                                 tests to avoid magic strings).
  config/
    GrpcConfig.kt             -- Registers the three manual interceptors as Spring beans
                                 at @Order(1), @Order(2), @Order(3). The auto-configured
                                 ObservationGrpcServerInterceptor runs at @Order(0).
```

#### Dependencies for auto-instrumentation

The auto-instrumentation is enabled by two dependencies working together:

- **`spring-boot-starter-actuator`** — provides `MeterRegistry` and `ObservationRegistry`. Spring gRPC detects the `ObservationRegistry` bean and auto-registers `ObservationGrpcServerInterceptor`.
- **`micrometer-tracing-bridge-otel`** — bridges Micrometer observations to OTel spans. Without this, you get metrics but not traces from the observation interceptor.

The `spring.grpc.server.observation.enabled` property (default `true`) controls whether the observation interceptor is registered.

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

When you add `spring-boot-starter-actuator` to a Spring gRPC project, Spring auto-configures `ObservationGrpcServerInterceptor` (from Micrometer core, at `@Order(0)`) which instruments every gRPC call via the Micrometer Observation API. This project **includes both** the auto-configured observation interceptor and our manual `OtelGrpcInterceptor`:

- **`ObservationGrpcServerInterceptor`** (auto-configured, `@Order(0)`) — produces Micrometer metrics (`grpc.server` timer, `grpc.server.active` long task timer, `grpc.server.received`/`sent` counters) with tags like `rpc.method`, `rpc.service`, `grpc.status_code`, and `rpc.type`. With `micrometer-tracing-bridge-otel` on the classpath, it also produces OTel spans via the bridge.
- **`OtelGrpcInterceptor`** (manual, `@Order(1)`) — creates OTel spans directly and manages the OTel context scope for our coroutine context propagation machinery.

The manual interceptor exists because the goal of this project is to show the **mechanics** of context propagation, not to hide them behind auto-configuration. In production, you might rely solely on the auto-configured interceptor and remove the manual one. `AutoInstrumentationTest` proves the auto-instrumentation works; the other tests prove the manual context propagation works.

The observation interceptor closes its observation (and thus the span/timer) by wrapping both the `ServerCall` (stopping on `close()`) and the `ServerCall.Listener` (stopping on `onCancel()`). It doesn't just fire at the start — gRPC's interceptor API gives it hooks into the full call lifecycle.

### `grpc-kotlin-stub` (Kotlin coroutine stubs)

The `grpc-kotlin` project can generate coroutine-based service stubs where each RPC method is a `suspend fun`. We used the standard Java stubs instead because:

- The Java stubs (`StreamObserver`-based) make the context propagation challenge more visible. You explicitly see where the coroutine is launched and how context is captured.
- With Kotlin stubs, the coroutine machinery is hidden inside the generated code, which makes it harder to teach what's happening.
- Our `respondWith`/`streamWith` extensions provide a clean API on top of the Java stubs while keeping the context capture explicit.

---

## What Each Test Proves

### `GreetingServiceIntegrationTest` (11 tests, full Spring Boot context)

These tests start the actual gRPC server and make real gRPC calls to it. Every test creates a real parent span using the test's `OpenTelemetry` instance and passes it via the `traceparent` header, so all span assertions verify actual parent-child relationships — not fabricated trace IDs.

#### Parent-child span verification (3 tests)

**`testGreetCreatesChildSpanOfParent`** — Creates a real `CLIENT` parent span, sends a unary gRPC call with its traceparent. Asserts that the exported server span shares the parent's `traceId`, has `parentSpanId` equal to the parent's `spanId`, and has its own unique `spanId`. This proves the `OtelGrpcInterceptor` correctly extracts the W3C traceparent and creates a child span.

**`testFarewellCreatesChildSpanOfParent`** — Same verification for the `Farewell` RPC. Proves both unary methods create proper child spans.

**`testGreetStreamCreatesChildSpanOfParent`** — Same verification for the server-streaming `GreetStream` RPC. Proves child span creation works for streaming calls.

#### Context propagation across dispatcher switch (1 test)

**`testContextPropagationAcrossDispatcherSwitch`** — The most important test. Creates a parent span, makes a gRPC call, then inspects captured log events from `Dispatchers.IO` — both **before** and **after** a `delay()`. Asserts that `userId`, `tenantId`, and the exact parent `trace.id` are present in MDC. This proves that all three context systems survive a dispatcher switch AND a suspension/resumption on a potentially different thread within the IO pool.

#### Farewell method and Error field (3 tests)

**`testFarewellReturnsSuccessResponse`** — Calls the `Farewell` RPC with a normal name. Asserts the goodbye message is correct, the `requestId` is populated, and the `error` field is **not** set. Proves the happy path for the second unary method.

**`testFarewellReturnsErrorForUnknownUser`** — Calls `Farewell` with name `"unknown"`. Asserts the `error` field is present with `http_code=404` and `reason="User not found"`. Proves the domain-level error convention works — the gRPC call itself succeeds (status OK) but the response body carries a business error.

**`testGreetReturnsErrorForErrorName`** — Calls `Greet` with name `"error"`. Asserts the `error` field is present with `http_code=500` and `reason="Simulated internal error for testing"`.

#### Logging interceptor (4 tests)

These tests install a custom Log4j2 appender to capture log events, make gRPC calls, then assert that the `LoggingInterceptor` produced the expected log entries.

**`testLoggingInterceptorLogsStartAndEndForSuccessfulCall`** — Calls `Greet` with a normal name. Asserts that both a `"gRPC START greeting.GreetingService/Greet"` log and a `"gRPC END ... OK"` log were emitted.

**`testLoggingInterceptorLogsFailureForErrorResponse`** — Calls `Greet` with name `"error"`, which returns a response with `http_code=500`. Asserts that the end log contains `httpCode=500` and was emitted at `WARN` level (not INFO).

**`testLoggingInterceptorLogsFarewellStartAndEnd`** — Same as the first logging test but for the `Farewell` method. Asserts START/END logs contain `GreetingService/Farewell`.

**`testLoggingInterceptorLogsFarewellFailure`** — Calls `Farewell` with name `"unknown"` (404 error). Asserts the end log contains `httpCode=404` at WARN level.

### `MdcCoroutineTest` (4 tests, no Spring, pure unit tests)

These tests verify the context propagation mechanisms in isolation, without starting a server.

**`ObservabilityContext propagates gRPC context and OTel span across dispatcher switch and delay`** — Sets up both a gRPC Context (with userId/tenantId) and an OTel span (with a known trace ID), creates an `ObservabilityContext`, and runs a coroutine that switches to `Dispatchers.IO` with a `delay`. Asserts that MDC contains all derived fields after the switch and delay.

**`gRPC context survives dispatcher switch and delay`** — A focused test that only checks gRPC `Context.Key.get()` (not MDC). Proves the gRPC Context half of `ObservabilityContext` works correctly.

**`withFields adds ephemeral MDC entries scoped to block`** — Verifies that `withFields("requestId" to "req-123")` adds the field inside the block, that it survives `Dispatchers.IO` + `delay`, and that it's removed after the block exits — while base fields like `userId` persist.

**`single element propagates gRPC context, OTel context, and MDC together`** — The capstone unit test. Sets up both gRPC and OTel context, creates a single `ObservabilityContext()`, and asserts that all three systems (gRPC keys, OTel trace ID in MDC, gRPC Context.Key.get()) work across `Dispatchers.IO` + `delay`.

### `AutoInstrumentationTest` (5 tests, full Spring Boot context)

These tests verify that Spring gRPC's built-in `ObservationGrpcServerInterceptor` auto-instruments gRPC calls — producing both Micrometer metrics and OTel traces without any manual interceptor code.

**`ObservationRegistry is configured and not no-op`** — Asserts that `ObservationRegistry` is properly wired (not `NOOP`), confirming that `spring-boot-starter-actuator` is on the classpath and the observation infrastructure is active.

**`unary gRPC call produces grpc server timer metric`** — Makes a unary gRPC call, then queries the `MeterRegistry` for a `grpc.server` timer. Asserts the timer exists, has recorded at least one call with non-zero duration, and carries the correct tags: `rpc.method=Greet`, `rpc.service=greeting.GreetingService`, `grpc.status_code=OK`.

**`streaming gRPC call produces grpc server timer metric`** — Same as above for server-streaming. Asserts that `grpc.server` timer exists with `rpc.method=GreetStream` after collecting all 5 streamed responses.

**`observation bridge produces OTel spans for gRPC calls`** — Makes a gRPC call and inspects the `InMemorySpanExporter` for spans. Proves that the Micrometer-to-OTel tracing bridge creates OTel spans from observations, without relying on our manual `OtelGrpcInterceptor`.

**`list all grpc-related meters after multiple calls`** — Makes several gRPC calls and dumps every meter whose name starts with `grpc.`. Shows the full set of auto-registered meters: `grpc.server` (timer), `grpc.server.active` (long task timer), `grpc.server.received` (counter), `grpc.server.sent` (counter) — each with tags for method, service, status code, peer info, and RPC type.

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
| **Interceptor** | gRPC middleware. Wraps the `ServerCall` and `ServerCall.Listener` to hook into the full call lifecycle (start, message, close, cancel). Used here to extract headers, log calls, and create spans. Similar to servlet filters or Spring `HandlerInterceptor`. |
| **Domain error vs transport error** | gRPC has its own status codes (OK, INTERNAL, NOT_FOUND, etc.) at the transport level. This project also uses a protobuf `Error` message inside the response body for domain-level errors — a call can succeed at the transport level (gRPC status OK) but carry a business error (e.g. `http_code=404`) in the response. The `LoggingInterceptor` detects this pattern. |
| **MdcProviders** | Our registry where interceptors declare how their gRPC Context keys map to MDC fields. `ObservabilityContext` calls all registered providers on each thread switch. |
| **Observation** | A Micrometer concept that represents a unit of work. An observation produces both metrics (timers, counters) and traces (spans) from a single instrumentation point. Spring gRPC's `ObservationGrpcServerInterceptor` creates one observation per gRPC call. |
| **ObservationRegistry** | The central Micrometer registry that manages observations. Provided by `spring-boot-starter-actuator`. Observation handlers attached to it determine what gets produced (metrics, traces, or both). |
| **MeterRegistry** | Micrometer's registry for metrics (timers, counters, gauges). The observation system automatically creates meters from observations. In tests, a `SimpleMeterRegistry` is used; in production, you'd use Prometheus, Datadog, etc. |
