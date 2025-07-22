/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

    implementation("org.creekservice:creek-base-type:$creekVersion")
    implementation("org.creekservice:creek-observability-logging:$creekVersion")

    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")

    implementation("net.jimblackler.jsonschemafriend:core:0.12.5")

    implementation("io.confluent:kafka-schema-registry-client:$confluentVersion")
    implementation("io.confluent:kafka-json-schema-provider:$confluentVersion")
    constraints {
        implementation("org.scala-lang:scala-library:2.13.16") {
            because("lower versions have security vulnerabilities")
        }
    }

    jsonSchemaGenerator("org.creekservice:creek-json-schema-generator:$creekVersion")

    testImplementation(project(":client-extension"))
    testImplementation(project(":serde-test"))
    testImplementation(project(":test-service-json"))
    testImplementation("com.fasterxml.jackson.core:jackson-annotations:$jacksonVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testContainersVersion")
    testImplementation("org.testcontainers:kafka:$testContainersVersion")
    testImplementation("org.creekservice:creek-observability-logging-fixtures:$creekVersion")
}

// Patch Kafka Testcontainers jar into main test containers module to avoid split packages:
modularity.patchModule("testcontainers", "kafka-$testContainersVersion.jar")

creek.schema.json {
    typeScanning.packageWhiteList("org.creekservice.api.kafka.serde.json", "org.creekservice.internal.kafka.serde.json")
    subTypeScanning.packageWhiteList("org.creekservice.api.kafka.serde.json", "org.creekservice.internal.kafka.serde.json")
}
