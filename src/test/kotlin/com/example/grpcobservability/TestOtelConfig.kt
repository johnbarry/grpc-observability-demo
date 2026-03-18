package com.example.grpcobservability

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.TracerProvider
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary

@TestConfiguration
class TestOtelConfig {

    @Bean
    fun inMemorySpanExporter(): InMemorySpanExporter = InMemorySpanExporter.create()

    @Bean
    fun sdkTracerProvider(spanExporter: InMemorySpanExporter): SdkTracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()

    @Bean
    @Primary
    fun openTelemetry(tracerProvider: SdkTracerProvider): OpenTelemetry =
        OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build()
}
