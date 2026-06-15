# Plan: Fix Issue 1 — Remove `streams-test` references

**Status:** done

## Context

`streams-test` module removed from `settings.gradle.kts`. Docs and docs-examples still reference it. Code examples won't compile once `0.4.5` releases.

Replacements:
- `TestKafkaStreamsExtensionOptions.defaults()` → `KafkaStreamsExtensionOptions.testBuilder().build()` (already in `streams-extension`)
- `TestTopics` → local helper in `docs-examples` (already exists but has wrong package)

---

## Changes

### 1. `docs-examples/build.gradle.kts`

Remove `streams-test` dependency (lines 47-49):
```
// begin-snippet: streams-test
    testImplementation("org.creekservice:creek-kafka-streams-test:0.4.4")
// end-snippet
```

### 2. `docs-examples/src/test/java/com/acme/examples/streams/TestTopics.java`

Fix package declaration from `io.github.creek.service.basic.kafka.streams.demo.service.kafka.streams` to `com.acme.examples.streams`.

### 3. `docs-examples/src/test/java/com/acme/examples/streams/TopologyBuilderTest.java`

Add missing import:
```java
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
```

File already uses `KafkaStreamsExtensionOptions.testBuilder()` and local `TestTopics` — no other code changes needed.

### 4. `docs-examples/src/test/java/module-info.test`

Remove `creek.kafka.streams.test` from both `--add-modules` and `--add-reads`:

Before:
```
--add-modules
  org.hamcrest,creek.test.util,creek.kafka.streams.test

--add-reads
  custom.serde.format=org.hamcrest,creek.test.util,creek.kafka.streams.test
```

After:
```
--add-modules
  org.hamcrest,creek.test.util

--add-reads
  custom.serde.format=org.hamcrest,creek.test.util
```

### 5. `docs/_docs/home.md`

**Lines 257-262** — Remove `streams-test` module description. 
  Replace with guidance showing `KafkaStreamsExtensionOptions.testBuilder()`, with link to source code in Github, 
  note that test topic helpers can be created locally, add an include_snippet for the TestTopics class. 
  Remove `{% include_snippet streams-test ... %}`.

**Line 663** — Replace `TestKafkaStreamsExtensionOptions.defaults()` with `KafkaStreamsExtensionOptions.testBuilder().build()` in custom schema client test example.

**Lines 746-747** — Remove stale javadoc links:
```
[testKsExtOpt]: https://javadoc.io/doc/org.creekservice/creek-kafka-streams-test/...
[testTopics]: https://javadoc.io/doc/org.creekservice/creek-kafka-streams-test/...
```

---

## Verification

1. `cd docs-examples && ./gradlew build` — compiles
2. `cd docs-examples && ./gradlew test` — tests pass
3. `grep -r 'streams-test' docs/ docs-examples/` — zero hits
4. `grep -r 'TestKafkaStreamsExtensionOptions' docs/ docs-examples/` — zero hits
