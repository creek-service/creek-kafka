# Plan: Fix docs and docs-examples divergence from codebase

**Status:** proposed

## Context

Recent codebase changes have caused significant divergence between the docs site (`./docs`), examples (`./docs-examples`), and the actual codebase. This plan captures all identified issues.

---

## Issue 1: `streams-test` module removed — docs still reference it

**Status**: done.

**Severity: HIGH — code examples won't compile**

Module `streams-test` deleted from `settings.gradle.kts`. Two public classes removed:
- `TestKafkaStreamsExtensionOptions`
- `TestTopics`

Replacements exist:
- `KafkaStreamsExtensionOptions.testBuilder()` in `streams-extension` replaces `TestKafkaStreamsExtensionOptions.defaults()`
- `TestTopics` functionality is trivial (wraps `TopologyTestDriver.createInputTopic/createOutputTopic`); can be inlined or kept as local helper
   A local version of `TestTopics` is in `docs-examples/src/test/java/com/acme/examples/streams/TestTopics.java`

### Affected locations

**docs/_docs/home.md:**
- Lines 257-271: "Streams Test" section describes `creek-kafka-streams-test.jar`
- Line 262: `{% include_snippet streams-test from ../docs-examples/build.gradle.kts %}` — pulls in stale dep
- Lines 270-271: References `TestKafkaStreamsExtensionOptions` and `TestTopics`
- Line 672: Code example uses `TestKafkaStreamsExtensionOptions.defaults()`
- Lines 755-756: Javadoc links to `creek-kafka-streams-test` artifact

**docs-examples/build.gradle.kts:**
- Lines 47-49: Declares `creek-kafka-streams-test:0.4.4` dependency

**docs-examples/src/test/java/com/acme/examples/streams/TopologyBuilderTest.java:**
- Lines 25-26: Imports `TestKafkaStreamsExtensionOptions` and `TestTopics` from removed module
- Line 54: `TestKafkaStreamsExtensionOptions.defaults()`
- Lines 68-69: `TestTopics.inputTopic(...)` / `TestTopics.outputTopic(...)`

**docs-examples/src/test/java/module-info.test:**
- Lines 2, 5: References `creek.kafka.streams.test` module

**docs-examples/src/test/java/com/acme/examples/streams/TestTopics.java:**
- Line 1: Wrong package declaration (`io.github.creek.service.basic.kafka.streams.demo.service.kafka.streams` instead of `com.acme.examples.streams`)

---

## Issue 2: New `schema-store` module undocumented

**Severity: MEDIUM**

New module `schema-store` (commit `70a8750`) contains types moved from `json-serde`:
- `SchemaStoreEndpoints` — now at `org.creekservice.api.kafka.serde.schema.store.endpoint`
- `MockEndpointsLoader` — now at `org.creekservice.api.kafka.serde.schema.store.endpoint`
- `SystemEnvSchemaRegistryEndpointLoader`

No mention in docs anywhere. Users following custom schema client setup instructions will have wrong imports.

### Affected locations

**docs/_docs/home.md:**
- Lines 662-687: Custom schema client test example references `SchemaStoreEndpoints.Loader`, `MockEndpointsLoader` without noting correct module/package

---

## Issue 3: New `KafkaSystemTestSerdeProvider` SPI undocumented

**Severity: MEDIUM**

Commit `87dc5fc` added `KafkaSystemTestSerdeProvider` interface at:
- `serde/src/main/java/org/creekservice/api/kafka/serde/provider/KafkaSystemTestSerdeProvider.java`

New SPI for pluggable system test serialization. Companion to `KafkaSerdeProvider`. Custom serde format implementors now need to implement both interfaces.

No mention in docs. The "Custom formats" section only covers `KafkaSerdeProvider`.

---

## Issue 4: Stale Kafka version in docs-examples

**Severity: LOW**

**docs-examples/build.gradle.kts:**
- Lines 60-63: Resolution strategy pins Kafka to `2.8.2`; main codebase uses `4.3.0`
- Line 69: Module patch references `kafka-streams-test-utils-2.8.2.jar`

**docs/_docs/home.md:**
- Kafka compatibility table lists supported versions up to 3.4.x — may be stale

---

## Issue 5: Stale Creek version in docs-examples

**Severity: LOW**

All Creek deps in `docs-examples/build.gradle.kts` pinned to `0.4.4`. Main repo on `0.4.5-SNAPSHOT`. Once `0.4.5` releases (with `streams-test` removal), these examples will fail to compile even with version update — must fix Issue 1 first.

---

## Issue 6: JSON serde test example has stale package references

**Severity: LOW**

**docs/_docs/home.md lines 662-687:**
- Code example references `JsonSchemaStoreClient.Factory.class`, `MockSchemaRegistryClient`, `SchemaStoreEndpoints.Loader.class`, `MockEndpointsLoader`
- These types still exist but `SchemaStoreEndpoints` and `MockEndpointsLoader` moved to `schema-store` module with new package `org.creekservice.api.kafka.serde.schema.store.endpoint`
- Import context in code block is implicitly wrong

## Issue 7: KafkaSerdeProvider interface — docs-examples OK

**Severity: NONE**

Interface unchanged. CustomSerdeFormatProvider in docs-examples still matches current API.