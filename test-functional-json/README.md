# JSON functional tests

Containerized system tests for the Kafka test extension with JSON-schema-backed
topics. Kept separate from the native fixture tests because the system-test
executor prepares resources from every discovered service descriptor, including
services outside the selected suite.

Run with `./gradlew :test-functional-json:test` (requires Docker).
