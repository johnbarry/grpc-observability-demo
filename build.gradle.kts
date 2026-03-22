import com.google.protobuf.gradle.id

plugins {
    id("org.springframework.boot") version "4.0.3"
    id("io.spring.dependency-management") version "1.1.7"
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.spring") version "2.1.0"
    id("com.google.protobuf") version "0.9.6"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.grpc:spring-grpc-dependencies:1.0.2")
        mavenBom("io.opentelemetry:opentelemetry-bom:1.46.0")
        mavenBom("io.opentelemetry.instrumentation:opentelemetry-instrumentation-bom:2.12.0")
    }
}

configurations.all {
    exclude(group = "ch.qos.logback")
    exclude(group = "org.apache.logging.log4j", module = "log4j-to-slf4j")
}

dependencies {
    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-log4j2")
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // Spring gRPC
    implementation("org.springframework.grpc:spring-grpc-spring-boot-starter")

    // Protobuf / gRPC
    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-stub")
    implementation("com.google.protobuf:protobuf-java")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    // Kotlin
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.1")

    // OpenTelemetry
    implementation("io.opentelemetry:opentelemetry-api")
    implementation("io.opentelemetry:opentelemetry-sdk")
    implementation("io.opentelemetry.instrumentation:opentelemetry-spring-boot-starter")

    // Micrometer tracing bridge — connects Spring Observation API to OTel
    implementation("io.micrometer:micrometer-tracing-bridge-otel")

    // Logging
    implementation("io.github.oshai:kotlin-logging-jvm:7.0.3")
    implementation("co.elastic.logging:log4j2-ecs-layout:1.7.0")

    // Test
    testImplementation("org.springframework.boot:spring-boot-starter-test") {
        exclude(group = "ch.qos.logback")
    }
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.1")
    testImplementation("io.opentelemetry:opentelemetry-sdk-testing")
    testImplementation("io.grpc:grpc-testing")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${dependencyManagement.importedProperties["protobuf-java.version"]}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${dependencyManagement.importedProperties["grpc.version"]}"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                id("grpc")
            }
        }
    }
}

kotlin {
    compilerOptions {
        freeCompilerArgs.addAll("-Xjsr305=strict")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// Ensure generated proto sources are visible to Kotlin compiler
sourceSets {
    main {
        kotlin {
            srcDir("build/generated/source/proto/main/java")
            srcDir("build/generated/source/proto/main/grpc")
        }
    }
}
