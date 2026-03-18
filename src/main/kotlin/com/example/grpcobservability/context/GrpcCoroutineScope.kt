package com.example.grpcobservability.context

import io.grpc.Status
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

/**
 * Executes a suspend block in a coroutine with full observability context,
 * sending the result as a unary gRPC response.
 *
 * Captures gRPC Context + OTel Span, propagates them across dispatchers,
 * and derives MDC automatically. Handles errors by translating exceptions
 * to gRPC status codes.
 */
fun <T> StreamObserver<T>.respondWith(block: suspend CoroutineScope.() -> T) {
    val scope = CoroutineScope(captureObservabilityContext())
    scope.launch {
        try {
            val result = block()
            onNext(result)
            onCompleted()
        } catch (e: Exception) {
            onError(Status.INTERNAL.withDescription(e.message).withCause(e).asException())
        }
    }
}

/**
 * Executes a suspend block that can emit multiple responses for server streaming RPCs.
 *
 * The [emit] function sends each response to the client. The stream is completed
 * automatically when the block returns.
 */
fun <T> StreamObserver<T>.streamWith(block: suspend CoroutineScope.(emit: suspend (T) -> Unit) -> Unit) {
    val scope = CoroutineScope(captureObservabilityContext())
    scope.launch {
        try {
            block { response -> onNext(response) }
            onCompleted()
        } catch (e: Exception) {
            onError(Status.INTERNAL.withDescription(e.message).withCause(e).asException())
        }
    }
}
