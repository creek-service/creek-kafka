[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Coverage Status](https://coveralls.io/repos/github/creek-service/creek-kafka/badge.svg?branch=main)](https://coveralls.io/github/creek-service/creek-kafka?branch=main)
[![build](https://github.com/creek-service/creek-kafka/actions/workflows/build.yml/badge.svg)](https://github.com/creek-service/creek-kafka/actions/workflows/build.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.creekservice/creek-kafka-streams-extension.svg)](https://central.sonatype.dev/search?q=creek-kafka-*)
[![CodeQL](https://github.com/creek-service/creek-kafka/actions/workflows/codeql.yml/badge.svg)](https://github.com/creek-service/creek-kafka/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/creek-service/creek-kafka/badge)](https://api.securityscorecards.dev/projects/github.com/creek-service/creek-kafka)
[![OpenSSF Best Practices](https://bestpractices.coreinfrastructure.org/projects/6899/badge)](https://bestpractices.coreinfrastructure.org/projects/6899)

# Creek Kafka

Kafka integration for Creek.

An example of how to write Kafka Streams based microservices using this Creek extension can be found in the
aptly named [basic Kafka Streams tutorial][1].

See [CreekService.org](https://www.creekservice.org/creek-kafka) for more info on using this extension. 

## Modules defined in this repo:

### Published:
* **[metadata](metadata)** [[JavaDocs](https://javadoc.io/doc/org.creekservice/creek-kafka-metadata)]: defines types to allow your services to indicate they consume and produce to Kafka.
* **[client-extension](client-extension)** [[JavaDocs](https://javadoc.io/doc/org.creekservice/creek-kafka-client-extension)]: defines a Creek extension to help with writing Kafka client based microservices.
* **[streams-extension](streams-extension)** [[JavaDocs](https://javadoc.io/doc/org.creekservice/creek-kafka-streams-extension)]: defines a Creek extension to help with writing Kafka Streams based microservices.
* **[serde](serde)** [[JavaDocs](https://javadoc.io/doc/org.creekservice/creek-kafka-serde)]: defines the base types used to define Kafka Serde (serializers and deserializers),
  and a `kafka` serialization format which uses the standard Kafka client serde.
* **[serde-test](serde-test)** [[JavaDocs](https://javadoc.io/doc/org.creekservice/creek-kafka-serde-test)]: Test utils for testing serde 
* **[json-serde](json-serde)** [[JavaDocs](https://javadoc.io/doc/org.creekservice/creek-kafka-json-serde)]: provides JSON serde implementations. 
* **[streams-test](streams-test)** [[JavaDocs](https://javadoc.io/doc/org.creekservice/creek-kafka-streams-test)]: Helpers for writing tests for Kafka streams based microservices.
* **[test-extension](test-extension)** [[JavaDocs](https://javadoc.io/doc/org.creekservice/creek-kafka-test-extension)]: Creek system-test extension to allow system testing of Kafka based microservices.

### Internal / Non-published:
* **[test-java-eight](test-java-eight)**: functional tests *without* Java 9's modularity.
* **[test-java-nine](test-java-nine)**: functional tests *with* Java 9's modularity.
* **[test-serde](test-serde)**: test-only serde extension implementation.
* **[test-serde-java-eight](test-serde-java-eight)**: test-only serde extension implementation that only registers itself
  using the Java 8 `META-INF/services` method.
* **[test-service-native](test-service-native)**: test-only microservice implementation using inbuilt/native Kafka serde.

### Docs:
* **[docs](docs)**: doc site accessible on https://www.creekservice.org/creek-kafka.
* **[docs-examples](docs-examples)**: example code injected into the docs.

## Kafka client version compatability

The libraries themselves are compiled with the latest versions of the Kafka clients and Kafka streams libraries.
However, they are compatible with older versions, as set out in the table below.
The `tested version` column details the exact version of Kafka libraries testing covers.

| Kafka version | Tested version | Notes                                           |
|---------------|----------------|-------------------------------------------------|
| < 2.8         |                | Not compatible due to API changes in Streams    |
| 2.8.+         | 2.8.2          | Supported & tested                              |
| 3.0.+         | 3.0.2          | Supported & tested                              |
| 3.1.+         | 3.1.2          | Supported & tested                              |
| 3.2.+         | 3.2.3          | Supported & tested                              |
| 3.3.+         | 3.3.2          | Supported & tested                              |
| 3.4.+         | 3.4.1          | Supported & tested                              |
| 3.5.+         | 3.5.2          | Supported & tested                              |
| 3.6.+         | 3.6.1          | Supported & tested                              |
| > 3.6         |                | Not currently tested / released. Should work... |

In Gradle, it is possible to force the use of an older Kafka client if you wish using a resolution strategy:

```kotlin
configurations.all {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.apache.kafka") {
            useVersion("2.8.2")
        }
    }
}
```

[1]: https://www.creekservice.org/basic-kafka-streams-demo/