/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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
    jacoco
    `creek-common-convention` apply false
    `creek-module-convention` apply false
    `creek-coverage-convention`
    `creek-publishing-convention` apply false
    `creek-sonatype-publishing-convention`
    id("pl.allegro.tech.build.axion-release") version "1.17.0" // https://plugins.gradle.org/plugin/pl.allegro.tech.build.axion-release
    id("com.bmuschko.docker-remote-api") version "9.4.0" apply false
    id("org.creekservice.schema.json") version "0.4.2-SNAPSHOT" apply false
}

project.version = scmVersion.version

allprojects {
    tasks.jar {
        onlyIf { sourceSets.main.get().allSource.files.isNotEmpty() }
    }
}

subprojects {
    project.version = project.parent?.version!!

    apply(plugin = "creek-common-convention")
    apply(plugin = "creek-module-convention")

    val shouldPublish = !name.startsWith("test-") || name == "test-extension"
    if (shouldPublish) {
        apply(plugin = "creek-publishing-convention")
        apply(plugin = "jacoco")
    } else {
        tasks.javadoc { onlyIf { false } }
    }

    repositories {
        maven {
            url = uri("https://jitpack.io")
            mavenContent {
                includeGroup("net.jimblackler.jsonschemafriend")
            }
        }

        maven {
            url = uri("https://packages.confluent.io/maven/")
            mavenContent {
                includeGroup("io.confluent")
            }
        }
    }

    extra.apply {
        set("creekVersion", "0.4.2-SNAPSHOT")
        set("spotBugsVersion", "4.8.3")         // https://mvnrepository.com/artifact/com.github.spotbugs/spotbugs-annotations
        set("jacksonVersion", "2.17.0")         // https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-annotations
        set("slf4jVersion", "2.0.12")            // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
        set("log4jVersion", "2.23.1")           // https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
        set("guavaVersion", "33.1.0-jre")         // https://mvnrepository.com/artifact/com.google.guava/guava
        set("junitVersion", "5.10.2")            // https://mvnrepository.com/artifact/org.junit.jupiter/junit-jupiter-api
        set("junitPioneerVersion", "2.2.0")     // https://mvnrepository.com/artifact/org.junit-pioneer/junit-pioneer
        set("mockitoVersion", "5.11.0")          // https://mvnrepository.com/artifact/org.mockito/mockito-junit-jupiter
        // Update kafka_version in `.github/workflows/build.yml` when updating this version
        set("kafkaVersion", "3.6.1")            // https://mvnrepository.com/artifact/org.apache.kafka
        set("confluentVersion", "7.6.0")        // https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client
        set("testContainersVersion", "1.19.7")  // https://mvnrepository.com/artifact/org.testcontainers/testcontainers
    }

    val kafkaVersionOverride = System.getenv("CREEK_KAFKA_VERSION")
    if (kafkaVersionOverride != null && kafkaVersionOverride.isNotEmpty()) {
        extra.apply {
            set("kafkaVersion", kafkaVersionOverride)
        }
    }

    configurations.all {
        resolutionStrategy.eachDependency {
            if (requested.group == "org.apache.kafka") {
                // Force use of apache Kafka libs, not Confluent's own:
                val kafkaVersion : String by extra
                useVersion(kafkaVersion)
            }
        }
    }

    val creekVersion : String by extra
    val guavaVersion : String by extra
    val log4jVersion : String by extra
    val jacksonVersion : String by extra
    val junitVersion: String by extra
    val junitPioneerVersion: String by extra
    val mockitoVersion: String by extra

    dependencies {
        implementation(platform("com.fasterxml.jackson:jackson-bom:$jacksonVersion"))

        testImplementation("org.creekservice:creek-test-util:$creekVersion")
        testImplementation("org.creekservice:creek-test-hamcrest:$creekVersion")
        testImplementation("org.creekservice:creek-test-conformity:$creekVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
        testImplementation("org.junit-pioneer:junit-pioneer:$junitPioneerVersion")
        testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion")
        testImplementation("com.google.guava:guava-testlib:$guavaVersion")
        testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:$log4jVersion")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    }
}

defaultTasks("format", "static", "check")
