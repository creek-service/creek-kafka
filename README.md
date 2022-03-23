[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![build](https://github.com/creek-service/creek-kafka/actions/workflows/gradle.yml/badge.svg)](https://github.com/creek-service/creek-kafka/actions/workflows/gradle.yml)
[![Coverage Status](https://coveralls.io/repos/github/creek-service/creek-kafka/badge.svg?branch=main)](https://coveralls.io/github/creek-service/creek-kafka?branch=main)

# Creek Kafka

Kafka integration for Creek

* **[metadata](metadata)**: defines types for defining kafka resources in aggregate and service descriptors.
* **[serde](serde)**: defines the base types used to define Kafka Serde (serializers and deserializers), 
                      and a `kafka` serialization format which uses the standard Kafka client serde.
* **[streams-extension](streams-extension)**: defines extension to Creek's `CreekContext` to help with writing Kafka based micro-services.
* **[test-java-eight](test-java-eight)**: functional tests *without* Java 9's modularity.
* **[test-java-nine](test-java-nine)**: functional tests *with* Java 9's modularity.