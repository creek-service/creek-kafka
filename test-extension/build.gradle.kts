/*
 * Copyright 2022 Creek Contributors (https://github.com/creek-service)
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

val creekSystemTestVersion : String by extra
val creekBaseVersion : String by extra
val testContainersVersion : String by extra
val kafkaVersion : String by extra

dependencies {
    api(project(":metadata"))
    api("org.creekservice:creek-system-test-extension:$creekSystemTestVersion")

    implementation(project(":streams-extension"))
    implementation("org.creekservice:creek-base-type:$creekBaseVersion")

    testImplementation(project(":test-service"))
    testImplementation("org.creekservice:creek-system-test-test-util:$creekSystemTestVersion")
    testImplementation("org.testcontainers:testcontainers:$testContainersVersion")
    testImplementation("org.apache.kafka:kafka-clients:$kafkaVersion")
}

tasks.test {
    dependsOn(":test-service:buildAppImage")
}
