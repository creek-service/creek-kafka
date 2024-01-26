[![javadoc](https://javadoc.io/badge2/org.creekservice/creek-kafka-json-serde/javadoc.svg)](https://javadoc.io/doc/org.creekservice/creek-kafka-json-serde)

# Creek Kafka JSON Serde

Module containing a Creek Kafka serde implementation that can be plugged in to Creek to provide JSON serde support.

For more information, see the [docs site](https://www.creekservice.org/creek-kafka/#json-schema-format).

This module has dependencies not stored in maven central. 
To use the module add Confluent and JitPack repositories to your build scripts.

For example, in Gradle `build.gradle.kts`:

```kotlin
repositories {
    maven {
        url = uri("https://jitpack.io")
        // Optionally limit the scope artefacts:
        mavenContent {
            includeGroup("net.jimblackler.jsonschemafriend")
        }
    }

    maven {
        url = uri("https://packages.confluent.io/maven/")
        // Optionally limit the scope artefacts:
        mavenContent {
            includeGroup("io.confluent")
        }
    }
}
```