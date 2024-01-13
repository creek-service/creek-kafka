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
val testContainersVersion : String by extra
val kafkaVersion : String by extra
val slf4jVersion : String by extra

dependencies {
    api(project(":metadata"))
    api("org.creekservice:creek-system-test-extension:$creekVersion")

    implementation(project(":streams-extension"))
    implementation("org.creekservice:creek-base-type:$creekVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("org.slf4j:slf4j-api:$slf4jVersion")

    testImplementation(project(":test-service-native"))
    testImplementation("org.creekservice:creek-system-test-executor:$creekVersion")
    testImplementation("org.creekservice:creek-system-test-test-util:$creekVersion")
    testImplementation("org.testcontainers:testcontainers:$testContainersVersion")
    testImplementation("org.apache.kafka:kafka-clients:$kafkaVersion")
}

tasks.test {
    dependsOn(":test-service-native:buildAppImage")
}
