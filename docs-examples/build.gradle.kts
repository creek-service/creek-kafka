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
    java
    id("org.creekservice.system.test") version "0.4.3"

// begin-snippet: module-plugin
    id("org.javamodularity.moduleplugin") version "2.0.0"
// end-snippet
}

repositories {
    mavenCentral()
}

// begin-snippet: deps
dependencies {
// end-snippet
    implementation("log4j:log4j:1.2.17")
// begin-snippet: meta
    implementation("org.creekservice:creek-kafka-metadata:0.4.3")
// end-snippet
    implementation("org.creekservice:creek-service-context:0.4.3")
// begin-snippet: client-ext
    implementation("org.creekservice:creek-kafka-client-extension:0.4.3")
// end-snippet
// begin-snippet: streams-ext
    implementation("org.creekservice:creek-kafka-streams-extension:0.4.3")
// end-snippet
// begin-snippet: test-ext
    systemTestExtension("org.creekservice:creek-kafka-test-extension:0.4.3")
// end-snippet
// begin-snippet: streams-test
    testImplementation("org.creekservice:creek-kafka-streams-test:0.4.3")
// end-snippet
    testImplementation("org.hamcrest:hamcrest-core:3.0")
    testImplementation(platform("org.junit:junit-bom:6.0.2"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// begin-snippet: resolution-strategy
configurations.all {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.apache.kafka") {
            useVersion("2.8.2")
        }
    }
}
// end-snippet

// begin-snippet: patch-module
// Patch Kafka Streams test jar into main Kafka Streams module to avoid split packages:
modularity.patchModule("kafka.streams", "kafka-streams-test-utils-2.8.2.jar")
// end-snippet

tasks.test {
    useJUnitPlatform()
    setForkEvery(5)
    maxParallelForks = Runtime.getRuntime().availableProcessors()
    testLogging {
        showStandardStreams = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showCauses = true
        showExceptions = true
        showStackTraces = true
    }
}

tasks.javadoc { enabled = false }

defaultTasks("build")
