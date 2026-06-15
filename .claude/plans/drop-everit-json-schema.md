# Plan: Drop `com.github.erosb:everit-json-schema` Dependency

## Background

The `json-serde` module transitively depends on `com.github.erosb:everit-json-schema:1.14.6`
via `io.confluent:kafka-json-schema-provider`. The goal is to eliminate this dependency.

---

## Analysis Findings

### Where everit comes from

`everit-json-schema` is pulled in as a **transitive dependency** of `io.confluent:kafka-json-schema-provider`:

```
io.confluent:kafka-json-schema-provider:8.2.0
  └── com.github.erosb:everit-json-schema:1.14.6
```

The `jsonschemafriend` library (Creek's own validation layer) does **NOT** depend on everit.

### How everit is used at runtime

The only runtime code path that triggers the everit library is in `CompatabilityChecker`:

```
CompatabilityChecker.checkCompatability()
  → JsonSchema.isBackwardCompatible()      (Confluent class)
    → JsonSchema.rawSchema()               (lazily initialises org.everit.json.schema.Schema)
      → SchemaDiff.compare(everit.Schema, everit.Schema)
```

Specifically in `CompatabilityChecker.java`:
- Line 81: `new JsonSchema(producerSchema.asJsonText()).isBackwardCompatible(newProducer)`  — forwards compat check
- Line 88: `new JsonSchema(existingConsumer.asJsonText()).isBackwardCompatible(newProducer)` — forwards compat check
- Line 107: `new JsonSchema(consumerSchema.asJsonText()).isBackwardCompatible(...)`          — backwards compat check
- Line 114: `newConsumer.isBackwardCompatible(new JsonSchema(existingProducer.asJsonText()))` — backwards compat check

All other uses of `JsonSchema` (schema registration/retrieval in `DefaultJsonSchemaRegistryClient`)
call only `canonicalString()` (Jackson-based) and `instanceof` checks — **no everit involvement**.

### Is everit validation redundant?

Yes and no:

- Creek's `SchemaFriendValidator` (uses `jsonschemafriend`) validates **data** against a schema at
  serialization/deserialization time. This is Creek's own validation layer — independent of everit.
- Everit is used by Confluent's `SchemaDiff` for **schema-to-schema compatibility** checking.
  This is a different concern: comparing two schema versions for backwards/forwards compatibility.

These two concerns are separate. The validation layer is NOT redundant, but they use different
libraries for different purposes. The question is only whether we can replace the Confluent
compatibility checker with one that avoids everit.

### Can we simply exclude everit?

No, not without code changes. `SchemaDiff.compare()` takes `org.everit.json.schema.Schema` objects
as parameters and is deeply coupled to everit types. Excluding the jar would cause
`NoClassDefFoundError` at runtime whenever compatibility checking runs.

Neither `kafka-json-schema-provider` nor `everit-json-schema` declare JPMS module-info, so the
module system won't flag the absence at startup — the failure would be a runtime crash.

### Confluent json-sKema alternative?

Confluent 8.2.0 also depends on `com.github.erosb:json-sKema` and `JsonSchema` has a separate
`rawSchemaV` (json-sKema) field. However, Confluent's `SchemaDiff` class **only** exposes
`compare(everit.Schema, everit.Schema)` overloads — there is no json-sKema-based diff path
in the current Confluent version.

---

## Proposed Approach

To drop everit, replace the `CompatabilityChecker`'s use of Confluent's
`JsonSchema.isBackwardCompatible()` with a custom implementation that works directly on
Jackson `JsonNode` objects (the schema's parsed JSON representation).

The schemas in this project are well-defined JSON Schemas derived from Java classes via the
`creek-json-schema-generator`. They follow a predictable structure (object schemas with typed
properties), making a targeted reimplementation feasible.

### Implementation plan

#### Step 1 – Verify scope with a test exclusion

Add an exclusion in `json-serde/build.gradle.kts` and run tests to confirm only
`CompatabilityChecker` breaks, and that no other runtime path uses everit.

```kotlin
implementation("io.confluent:kafka-json-schema-provider:$confluentVersion") {
    exclude(group = "com.github.erosb", module = "everit-json-schema")
}
```

Run: `./gradlew :json-serde:test`

Expected: tests that exercise `CompatabilityChecker` fail with `NoClassDefFoundError`; all
others pass.

#### Step 2 – Implement `JsonNodeSchemaDiff`

Create a new class `JsonNodeSchemaDiff` (or similar) in the `compatability` package that:
- Accepts two JSON schemas as `JsonNode` (or `ProducerSchema`/`ConsumerSchema`)
- Performs the same forwards/backwards compatibility analysis that `SchemaDiff.compare()` does
- Returns a `List<String>` of incompatibility messages (same contract as `isBackwardCompatible`)

The logic should cover the cases that Creek's producer/consumer schemas actually exercise:
- Object schemas: property additions/removals, required field changes, type changes
- Basic type constraints (string patterns, number bounds, array items)
- `additionalProperties` changes (Creek uses closed producer schemas / open consumer schemas)

Confluent's SchemaDiff source (Apache-licensed) can be used as reference for the expected
semantics without needing to copy the everit-specific code:
https://github.com/confluentinc/schema-registry/tree/master/json-schema-provider/src/main/java/io/confluent/kafka/schemaregistry/json/diff

#### Step 3 – Replace `CompatabilityChecker` logic

Replace the two calls to `isBackwardCompatible` in `CompatabilityChecker` with calls to
`JsonNodeSchemaDiff`. The schema text is already available as `asJsonText()` on the schema
objects; parse it with Jackson into `JsonNode` before diffing.

Remove the `import io.confluent.kafka.schemaregistry.json.JsonSchema` from `CompatabilityChecker`.

#### Step 4 – Add the Gradle exclusion

Once tests pass, add the explicit exclusion in `json-serde/build.gradle.kts` so the jar is
never included in any downstream artifact:

```kotlin
implementation("io.confluent:kafka-json-schema-provider:$confluentVersion") {
    exclude(group = "com.github.erosb", module = "everit-json-schema")
}
```

Also remove from `module-info.java` any `requires` for everit if one exists (currently none;
the module-info only `requires kafka.json.schema.provider`).

#### Step 5 – Add regression / compatibility tests

Ensure the existing `SchemaEvolutionTest` (and any other integration tests) cover the
compatibility checking paths. Add targeted unit tests for `JsonNodeSchemaDiff` covering:
- Backwards-incompatible change: removing a required field from producer schema
- Forwards-incompatible change: adding a required field to a new producer schema
- Compatible change: adding an optional field
- Type change (incompatible)
- `additionalProperties` changes

---

## Files to change

| File | Change |
|------|--------|
| `json-serde/build.gradle.kts` | Exclude `everit-json-schema` from `kafka-json-schema-provider` transitive deps |
| `json-serde/src/main/java/.../compatability/CompatabilityChecker.java` | Replace `isBackwardCompatible` calls with `JsonNodeSchemaDiff` |
| `json-serde/src/main/java/.../compatability/JsonNodeSchemaDiff.java` | New class implementing everit-free schema diff |
| `json-serde/src/test/java/.../compatability/CompatabilityCheckerTest.java` | Update tests if needed |
| `json-serde/src/test/java/.../compatability/JsonNodeSchemaDiffTest.java` | New unit tests for the diff logic |

No changes needed to:
- `DefaultJsonSchemaRegistryClient` — does not use everit
- `SchemaFriendValidator` — uses jsonschemafriend, not everit
- `module-info.java` — no everit require declared
- `JsonSerdeExtensionOptions` — uses `JsonSchemaProvider` (Confluent), not everit directly

---

## Risks

- **Compatibility logic gap**: Reimplementing schema diff is non-trivial. An incomplete
  implementation could allow incompatible schema changes to slip through undetected.
  Mitigation: Port the logic from Confluent's Apache-licensed source and add thorough tests.

- **Confluent internal coupling**: `CompatabilityChecker` notes that "the default JSON Schema
  compatibility checks in the Confluent Schema Registry are broken", meaning we already diverge
  from the default. This works in our favour — we're in control of this checker.

- **Future Confluent versions**: If a future version of Confluent drops everit natively (moving to
  json-sKema), this workaround becomes unnecessary. Monitor Confluent's release notes.

---

## Alternative (lower effort, higher risk): Conditional exclusion + test coverage

If full reimplementation is out of scope, a lower-risk incremental option is:
1. Exclude everit
2. Keep `CompatabilityChecker` but wrap `isBackwardCompatible` calls in a try-catch for
   `NoClassDefFoundError`, logging a warning and skipping the check
3. Accept the compatibility check is now a no-op until a proper replacement is written

This is not recommended for production but could be an interim measure.
