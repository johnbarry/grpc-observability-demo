package com.example.grpcobservability.service

import com.example.grpcobservability.context.respondWith
import com.example.grpcobservability.context.streamWith
import com.example.grpcobservability.context.withFields
import com.example.grpcobservability.interceptor.AuthInterceptor
import com.example.grpcobservability.proto.Error
import com.example.grpcobservability.proto.FarewellRequest
import com.example.grpcobservability.proto.FarewellResponse
import com.example.grpcobservability.proto.GreetRequest
import com.example.grpcobservability.proto.GreetResponse
import com.example.grpcobservability.proto.GreetingServiceGrpc
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.springframework.grpc.server.service.GrpcService
import java.util.UUID

private val logger = KotlinLogging.logger {}

@GrpcService
class GreetingServiceImpl : GreetingServiceGrpc.GreetingServiceImplBase() {

    override fun greet(request: GreetRequest, responseObserver: StreamObserver<GreetResponse>) {
        responseObserver.respondWith {
            handleUnary(request.name, "Greet") { requestId, error ->
                GreetResponse.newBuilder()
                    .setMessage("Hello, ${request.name}!")
                    .setRequestId(requestId)
                    .apply { if (error != null) setError(error) }
                    .build()
            }
        }
    }

    override fun farewell(request: FarewellRequest, responseObserver: StreamObserver<FarewellResponse>) {
        responseObserver.respondWith {
            handleUnary(request.name, "Farewell") { requestId, error ->
                FarewellResponse.newBuilder()
                    .setMessage("Goodbye, ${request.name}! See you next time.")
                    .setRequestId(requestId)
                    .apply { if (error != null) setError(error) }
                    .build()
            }
        }
    }

    override fun greetStream(request: GreetRequest, responseObserver: StreamObserver<GreetResponse>) {
        responseObserver.streamWith { emit ->
            val requestId = UUID.randomUUID().toString()

            for (i in 1..5) {
                withFields(
                    "requestId" to requestId,
                    "rpc.method" to "GreetStream",
                    "streamIndex" to i.toString(),
                ) {
                    delay(5)

                    val response = GreetResponse.newBuilder()
                        .setMessage("Hello #$i, ${request.name}!")
                        .setRequestId(requestId)
                        .build()

                    logger.info { "Streaming response #$i for name=${request.name}" }

                    emit(response)
                }
            }
        }
    }

    /**
     * Shared handler for unary RPCs. Manages requestId generation, context extraction,
     * IO dispatcher switching (to prove context propagation), and error triggering.
     */
    private suspend fun <T> handleUnary(
        name: String,
        methodName: String,
        buildResponse: (requestId: String, error: Error?) -> T,
    ): T {
        val requestId = UUID.randomUUID().toString()
        val userId = AuthInterceptor.USER_ID_CTX_KEY.get() ?: "unknown"
        val tenantId = AuthInterceptor.TENANT_ID_CTX_KEY.get() ?: "unknown"

        return withFields("requestId" to requestId, "rpc.method" to methodName) {
            logger.info { "Received $methodName request for name=$name, userId=$userId, tenantId=$tenantId" }

            delay(10)

            // Switch to IO dispatcher and delay to prove context propagation
            // across both dispatcher switch AND suspension/resumption
            withContext(Dispatchers.IO) {
                logger.info { "Processing on IO dispatcher (before delay) - userId=$userId, tenantId=$tenantId" }
                delay(10)
                logger.info { "Processing on IO dispatcher (after delay) - userId=$userId, tenantId=$tenantId" }
            }

            val error = resolveError(name)
            logger.info { "Sending $methodName response for requestId=$requestId" }
            buildResponse(requestId, error)
        }
    }

    private fun resolveError(name: String): Error? = when {
        name.equals(ErrorTriggers.GREET_ERROR_NAME, ignoreCase = true) ->
            Error.newBuilder().setHttpCode(500).setReason("Simulated internal error for testing").build()
        name.equals(ErrorTriggers.FAREWELL_NOT_FOUND_NAME, ignoreCase = true) ->
            Error.newBuilder().setHttpCode(404).setReason("User not found").build()
        else -> null
    }
}
