/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

dependencies {
    api(project(":metadata"))
    api(project(":serde"))
    api("org.creekservice:creek-service-extension:$creekVersion")
    api("org.apache.kafka:kafka-clients:$kafkaVersion")

    implementation("org.creekservice:creek-observability-logging:$creekVersion")
    implementation("org.creekservice:creek-base-type:$creekVersion")

    testImplementation("org.creekservice:creek-service-context:$creekVersion")
    testImplementation("org.creekservice:creek-observability-logging-fixtures:$creekVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testContainersVersion")
    testImplementation("org.testcontainers:kafka:$testContainersVersion")
}

// Patch Kafka Testcontainers jar into main test containers module to avoid split packages:
modularity.patchModule("testcontainers", "kafka-$testContainersVersion.jar")