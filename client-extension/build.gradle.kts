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
}

val creekVersion : String by extra
val kafkaVersion : String by extra
val testContainersVersion : String by extra
val confluentVersion : String by extra

dependencies {
    api(project(":metadata"))
    api(project(":serde"))
    api("org.creekservice:creek-service-extension:$creekVersion")
    api("org.apache.kafka:kafka-clients:$kafkaVersion")

    implementation("org.creekservice:creek-observability-logging:$creekVersion")
    implementation("org.creekservice:creek-base-type:$creekVersion")

    testImplementation("org.creekservice:creek-service-context:$creekVersion")
    testImplementation("org.creekservice:creek-observability-logging-fixtures:$creekVersion")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:$testContainersVersion")
    testImplementation("org.testcontainers:testcontainers-kafka:$testContainersVersion")
}

// Patch Kafka Testcontainers jar into main test containers module to avoid split packages:
// Needed until https://github.com/testcontainers/testcontainers-java/issues/11716 is resolved.
modularity.patchModule("testcontainers", "testcontainers-kafka-$testContainersVersion.jar")

val generateProperties by tasks.registering {
    val outputDir = layout.buildDirectory.dir("generated/resources/main")
    outputs.dir(outputDir)
    inputs.property("confluentVersion", confluentVersion)

    doLast {
        val propsFile = outputDir.get().file("creek-kafka-clients-extension.properties").asFile
        propsFile.parentFile.mkdirs()
        propsFile.writeText("confluentVersion=$confluentVersion\n")
    }
}

sourceSets.main {
    resources.srcDir(generateProperties)
}