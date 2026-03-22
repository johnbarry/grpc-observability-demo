package com.example.grpcobservability.config

import com.example.grpcobservability.interceptor.AuthInterceptor
import com.example.grpcobservability.interceptor.LoggingInterceptor
import com.example.grpcobservability.interceptor.OtelGrpcInterceptor
import io.grpc.ServerInterceptor
import io.opentelemetry.api.OpenTelemetry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.annotation.Order
import org.springframework.grpc.server.GlobalServerInterceptor

@Configuration
class GrpcConfig {

    @Bean
    @Order(1)
    @GlobalServerInterceptor
    fun otelGrpcInterceptor(openTelemetry: OpenTelemetry): ServerInterceptor {
        val tracer = openTelemetry.getTracer("grpc-observability-demo")
        val propagator = openTelemetry.propagators.textMapPropagator
        return OtelGrpcInterceptor(tracer, propagator)
    }

    @Bean
    @Order(2)
    @GlobalServerInterceptor
    fun authInterceptor(): ServerInterceptor {
        return AuthInterceptor()
    }

    @Bean
    @Order(3)
    @GlobalServerInterceptor
    fun loggingInterceptor(): ServerInterceptor {
        return LoggingInterceptor()
    }
}
