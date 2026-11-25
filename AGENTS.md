
## Updating kafkaVersion

When updating the `kafkaVersion` in `gradle.properties`, make sure to also 

1. update the `kafka_version` matrix in `.github/workflows/build.yml` to keep the highest version in the matrix consistent with `kafkaVersion`.
2. update the docs site:
   1. `docs/_docs/home.md` "Kafka client compatibility" section has a table of supported Kafka versions, which should be updated to include the new version.
   2. `docs-examples/build.gradle.kts` has a resolution strategy that pins the Kafka version: update this to the new version.