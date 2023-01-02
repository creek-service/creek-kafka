plugins {
    java
    jacoco
    `creek-common-convention` apply false
    `creek-module-convention` apply false
    `creek-coverage-convention`
    `creek-publishing-convention` apply false
    `creek-sonatype-publishing-convention`
    id("pl.allegro.tech.build.axion-release") version "1.14.3" // https://plugins.gradle.org/plugin/pl.allegro.tech.build.axion-release
}

project.version = scmVersion.version

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

    extra.apply {
        set("creekVersion", "0.2.1-SNAPSHOT")
        set("spotBugsVersion", "4.7.3")         // https://mvnrepository.com/artifact/com.github.spotbugs/spotbugs-annotations
        set("jacksonVersion", "2.14.1")         // https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-annotations
        set("slf4jVersion", "2.0.6")            // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
        set("log4jVersion", "2.19.0")           // https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
        set("guavaVersion", "31.1-jre")         // https://mvnrepository.com/artifact/com.google.guava/guava
        set("junitVersion", "5.9.1")            // https://mvnrepository.com/artifact/org.junit.jupiter/junit-jupiter-api
        set("junitPioneerVersion", "1.9.1")     // https://mvnrepository.com/artifact/org.junit-pioneer/junit-pioneer
        set("mockitoVersion", "4.11.0")          // https://mvnrepository.com/artifact/org.mockito/mockito-junit-jupiter
        set("hamcrestVersion", "2.2")           // https://mvnrepository.com/artifact/org.hamcrest/hamcrest-core
        set("kafkaVersion", System.getenv("CREEK_KAFKA_VERSION") ?: "3.3.0") // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
        set("testContainersVersion", "1.17.6")  // https://mvnrepository.com/artifact/org.testcontainers/testcontainers
    }

    val creekVersion : String by extra
    val guavaVersion : String by extra
    val log4jVersion : String by extra
    val junitVersion: String by extra
    val junitPioneerVersion: String by extra
    val mockitoVersion: String by extra
    val hamcrestVersion : String by extra

    dependencies {
        testImplementation("org.creekservice:creek-test-util:$creekVersion")
        testImplementation("org.creekservice:creek-test-hamcrest:$creekVersion")
        testImplementation("org.creekservice:creek-test-conformity:$creekVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-params:$junitVersion")
        testImplementation("org.junit-pioneer:junit-pioneer:$junitPioneerVersion")
        testImplementation("org.mockito:mockito-junit-jupiter:$mockitoVersion")
        testImplementation("org.hamcrest:hamcrest-core:$hamcrestVersion")
        testImplementation("com.google.guava:guava-testlib:$guavaVersion")
        testImplementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
        testImplementation("org.apache.logging.log4j:log4j-slf4j2-impl:$log4jVersion")
        testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    }
}

defaultTasks("format", "static", "check")
