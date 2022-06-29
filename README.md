[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Coverage Status](https://coveralls.io/repos/github/creek-service/creek-kafka/badge.svg?branch=main)](https://coveralls.io/github/creek-service/creek-kafka?branch=main)
[![build](https://github.com/creek-service/creek-kafka/actions/workflows/gradle.yml/badge.svg)](https://github.com/creek-service/creek-kafka/actions/workflows/gradle.yml)
[![CodeQL](https://github.com/creek-service/creek-kafka/actions/workflows/codeql.yml/badge.svg)](https://github.com/creek-service/creek-kafka/actions/workflows/codeql.yml)

# Creek Kafka

Kafka integration for Creek.

An example of how to write Kafka Streams based microservices using this Creek extension can be found in the
aptly named [Example Kafka Streams Aggregate][1].

## Modules defined in this repo:

* **[common](common)**: common code shared between extension implementations
* **[metadata](metadata)**: defines types to allow your services to indicate they consume and produce to Kafka.
* **[serde](serde)**: defines the base types used to define Kafka Serde (serializers and deserializers), 
                      and a `kafka` serialization format which uses the standard Kafka client serde.
* **[streams-extension](streams-extension)**: defines a Creek extension to help with writing Kafka Streams based microservices.
* **[streams-test](streams-test)**: Helpers for writing tests for Kafka streams based microservices.
* **[test-extension](test-extension)**: Creek system-test extension to allow system testing of Kafka based microservices.
* **[test-java-eight](test-java-eight)**: functional tests *without* Java 9's modularity.
* **[test-java-nine](test-java-nine)**: functional tests *with* Java 9's modularity.
* **[test-serde](test-serde)**: test-only serde extension implementation.

[1]: https://github.com/creek-service/example-kafka-streams-aggregate