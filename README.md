[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Coverage Status](https://coveralls.io/repos/github/creek-service/creek-kafka/badge.svg?branch=main)](https://coveralls.io/github/creek-service/creek-kafka?branch=main)
[![build](https://github.com/creek-service/creek-kafka/actions/workflows/build.yml/badge.svg)](https://github.com/creek-service/creek-kafka/actions/workflows/build.yml)
[![CodeQL](https://github.com/creek-service/creek-kafka/actions/workflows/codeql.yml/badge.svg)](https://github.com/creek-service/creek-kafka/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/creek-service/creek-kafka/badge)](https://api.securityscorecards.dev/projects/github.com/creek-service/creek-kafka)

# Creek Kafka

Kafka integration for Creek.

An example of how to write Kafka Streams based microservices using this Creek extension can be found in the
aptly named [Example Kafka Streams Aggregate][1].

See [CreekService.org](https://www.creekservice.org) for info on Creek Service.

## Modules defined in this repo:

* **[metadata](metadata)**: defines types to allow your services to indicate they consume and produce to Kafka.
* **[serde](serde)**: defines the base types used to define Kafka Serde (serializers and deserializers), 
                      and a `kafka` serialization format which uses the standard Kafka client serde.
* **[client-extension](client-extension)**: defines a Creek extension to help with writing Kafka client based microservices.
* **[streams-extension](streams-extension)**: defines a Creek extension to help with writing Kafka Streams based microservices.
* **[streams-test](streams-test)**: Helpers for writing tests for Kafka streams based microservices.
* **[test-extension](test-extension)**: Creek system-test extension to allow system testing of Kafka based microservices.
* **[test-java-eight](test-java-eight)**: functional tests *without* Java 9's modularity.
* **[test-java-nine](test-java-nine)**: functional tests *with* Java 9's modularity.
* **[test-serde](test-serde)**: test-only serde extension implementation.

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
| > 3.2         |                | Not currently tested / released. Should work... |

In Gradle, it is possible to force the use of an older Kafka client if you which using a resolution strategy:

```kotlin
configurations.all {
    resolutionStrategy.eachDependency {
        if (requested.group == "org.apache.kafka") {
            useVersion("2.8.2")
        }
    }
}
```

[1]: https://github.com/creek-service/example-kafka-streams-aggregate