package com.example.grpcobservability.service

import com.example.grpcobservability.context.respondWith
import com.example.grpcobservability.context.streamWith
import com.example.grpcobservability.context.withFields
import com.example.grpcobservability.interceptor.AuthInterceptor
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
            val requestId = UUID.randomUUID().toString()
            val userId = AuthInterceptor.USER_ID_CTX_KEY.get() ?: "unknown"
            val tenantId = AuthInterceptor.TENANT_ID_CTX_KEY.get() ?: "unknown"

            withFields("requestId" to requestId, "rpc.method" to "Greet") {
                logger.info { "Received greet request for name=${request.name}, userId=$userId, tenantId=$tenantId" }

                delay(10)

                // Switch to IO dispatcher and delay to prove context propagation
                // across both dispatcher switch AND suspension/resumption
                withContext(Dispatchers.IO) {
                    logger.info { "Processing on IO dispatcher (before delay) - userId=$userId, tenantId=$tenantId" }
                    delay(10)
                    logger.info { "Processing on IO dispatcher (after delay) - userId=$userId, tenantId=$tenantId" }
                }

                logger.info { "Sending greet response for requestId=$requestId" }

                GreetResponse.newBuilder()
                    .setMessage("Hello, ${request.name}!")
                    .setRequestId(requestId)
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
}
