package com.example.grpcobservability

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class GrpcObservabilityDemoApplication

fun main(args: Array<String>) {
    runApplication<GrpcObservabilityDemoApplication>(*args)
}
