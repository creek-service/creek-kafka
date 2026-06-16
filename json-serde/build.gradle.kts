/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    `java-library`
    id("org.creekservice.schema.json")
}

val creekVersion : String by extra
val kafkaVersion : String by extra
val confluentVersion : String by extra
val jacksonVersion : String by extra
val testContainersVersion : String by extra

dependencies {
    api(project(":serde"))
    api(project(":schema-store"))

    implementation("org.creekservice:creek-base-type:$creekVersion")
    implementation("org.creekservice:creek-observability-logging:$creekVersion")

    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")

    implementation("org.creekservice:creek-json-schema-validator:$creekVersion")

    implementation("io.confluent:kafka-schema-registry-client:$confluentVersion")
    implementation("io.confluent:kafka-json-schema-provider:$confluentVersion")

    // Explicit modules that are transitive runtime deps of the above libraries.
    // JPMS will not resolve explicit (named) modules into the module graph unless
    // they are explicitly required. These are needed by automatic modules above
    // (e.g. kafka-json-schema-provider needs org.json, commons-validator needs commons-beanutils,
    //  kafka-json-schema-provider → json-sKema needs kotlin.stdlib).
    implementation("org.json:json:20260522")
    implementation("commons-beanutils:commons-beanutils:1.11.0")
    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("com.github.luben:zstd-jni:1.5.7-11")

    jsonSchemaGenerator("org.creekservice:creek-json-schema-generator:$creekVersion")

    testImplementation(project(":client-extension"))
    testImplementation(project(":serde-test"))
    testImplementation(project(":test-service-json"))
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:$testContainersVersion")
    testImplementation("org.creekservice:creek-observability-logging-fixtures:$creekVersion")

    constraints {
        implementation("org.scala-lang:scala-library:3.8.3") {
            because("lower versions have security vulnerabilities")
        }
        implementation("commons-validator:commons-validator:1.10.1") {
            because("Moves commons-beanutils:commons-beanutils past version suffering from CVE-2025-48734 / GHSA-wxr5-93ph-8wr9")
        }
    }
}

// Patch Kafka Testcontainers jar into main test containers module to avoid split packages:
// Needed until https://github.com/testcontainers/testcontainers-java/issues/11716 is resolved.
modularity.patchModule("testcontainers", "testcontainers-kafka-$testContainersVersion.jar")

creek.schema.json {
    typeScanning.packageWhiteList("org.creekservice.api.kafka.serde.json", "org.creekservice.internal.kafka.serde.json")
    subTypeScanning.packageWhiteList("org.creekservice.api.kafka.serde.json", "org.creekservice.internal.kafka.serde.json")
}
