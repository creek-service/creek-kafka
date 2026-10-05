/*
 * Copyright 2023-2026 Creek Contributors (https://github.com/creek-service)
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
    id("pl.allegro.tech.build.axion-release") version "1.21.4" // https://plugins.gradle.org/plugin/pl.allegro.tech.build.axion-release
    id("com.bmuschko.docker-remote-api") version "10.0.0" apply false
    id("org.creekservice.schema.json") version "0.4.5-SNAPSHOT" apply false
}

scmVersion {
    versionCreator("simple")
}

project.version = scmVersion.version
println("creekVersion: ${project.version}")

allprojects {
    tasks.jar {
        onlyIf { sourceSets.main.get().allSource.files.isNotEmpty() }
    }
}

subprojects {
    project.version = project.parent?.version!!

    pluginManager.apply("creek-common-convention")
    pluginManager.apply("creek-module-convention")

    val shouldPublish = !name.startsWith("test-") || name == "test-extension"
    if (shouldPublish) {
        pluginManager.apply("creek-publishing-convention")
        pluginManager.apply("jacoco")
    } else {
        tasks.javadoc { onlyIf { false } }
    }

    repositories {
        maven {
            url = uri("https://packages.confluent.io/maven/")
            mavenContent {
                includeGroup("io.confluent")
            }
        }
    }

    extra.apply { set("creekVersion", project.version) }

    val kafkaVersionOverride = System.getenv("CREEK_KAFKA_VERSION")
    if (kafkaVersionOverride != null && kafkaVersionOverride.isNotEmpty()) {
        extra.apply {
            set("kafkaVersion", kafkaVersionOverride)
        }
    }

    configurations.all {
        resolutionStrategy.eachDependency {
            if (requested.group == "org.apache.kafka") {
                // Force use of Apache Kafka libs, not Confluent's own:
                val kafkaVersion = property("kafkaVersion") as String
                useVersion(kafkaVersion)
            }
        }
    }

    val creekVersion = property("creekVersion") as String
    val junitVersion = property("junitVersion") as String
    val confluentVersion = property("confluentVersion") as String

    dependencies {
        constraints {
            implementation("org.apache.commons:commons-compress:1.28.0") {
                because("earlier versions have a security vulnerabilities")
            }
            implementation("at.yawk.lz4:lz4-java:1.11.1") {
                because("earlier versions have a security vulnerability (GHSA-xx22-p4ch-683r), pulled in transitively via kafka-clients")
            }
            implementation("tools.jackson.core:jackson-core:3.2.3") {
                because("earlier versions have security vulnerabilities (GHSA-7hhh-6rmp-j9qf), pulled in transitively via creek-json-schema-validator")
            }
            implementation("tools.jackson.core:jackson-databind:3.2.3") {
                because("earlier versions have security vulnerabilities (GHSA-cxp5-3px4-pw24, GHSA-wv8q-qhhj-9h54), pulled in transitively via creek-json-schema-validator")
            }
        }

        implementation(platform("com.fasterxml.jackson:jackson-bom:${property("jacksonVersion")}"))

        testImplementation("org.creekservice:creek-test-util:$creekVersion")
        testImplementation("org.creekservice:creek-test-hamcrest:$creekVersion")
        testImplementation("org.creekservice:creek-test-conformity:$creekVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
        testImplementation("org.junit-pioneer:junit-pioneer:${property("junitPioneerVersion")}")
        testImplementation("org.mockito:mockito-junit-jupiter:${property("mockitoVersion")}")
        testImplementation("com.google.guava:guava-testlib:${property("guavaVersion")}")
        testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:${property("log4jVersion")}")
        testImplementation("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    }

    tasks.withType<Test>().configureEach {
        systemProperty("confluentVersion", confluentVersion)
    }
}

defaultTasks("format", "static", "check")
