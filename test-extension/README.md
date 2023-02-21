[![javadoc](https://javadoc.io/badge2/org.creekservice/creek-kafka-test-extension/javadoc.svg)](https://javadoc.io/doc/org.creekservice/creek-kafka-test-extension)
# Creek Kafka System Test Extension

A Creek system test extension for system testing Kafka based microservices.

## Configuring the extension

The test extension can be added to any module that runs system tests. 
How this is done will depend on the build plugin being used to run system tests.
For the [gradle plugin][gradle-system-test-plugin] the text extension can be added using the `systemTestExtension`
dependency configuration:

##### Groovy: adding the Kafka test extension to a build
```groovy
dependencies {
    systemTestExtension 'org.creekservice:creek-kafka-test-extension:0.2.0'
}
```

##### Kotlin: adding the Kafka test extension to a build
```kotlin
dependencies {
    systemTestExtension("org.creekservice:creek-kafka-test-extension:0.2.0")
}
```

## Test model

The extension registers the following model subtypes to support system testing of Kafka based microservices:

### Option model extensions

The behaviour of the Kafka test extension can be controlled via the `creek/kafka-option@1` option type.  
This option type defines the following:

| Property name     | Property type            | Description                                                                                                                                                                                                                                                                         |
|-------------------|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| outputOrdering    | Enum (`NONE`, `BY_KEY`)  | (Optional) Controls the ordering requirements for the expected output records on the same topic. Valid values are:<br>`None`: records can be in any order.<br>`BY_KEY`: record expectations that share the same key must be received in the order defined.<br>**Default**: `BY_KEY` |
| verifierTimeout   | Duration (String/Number) | (Optional) Overrides the global verifier timeout. Can be set to number of seconds, e.g. `60` or a string that can be parsed by Java `Duration` type, e.g. `PT2M`.                                                                                                                   |
| extraTimeout      | Duration (String/Number) | (Optional) Sets the time the tests will wait for extra, unexpected, records to be produced. Can be set to number of seconds, e.g. `60` or a string that can be parsed by Java `Duration` type, e.g. `PT2M`. **Default**: 1 second.                                                  |
| kafkaDockerImage  | String                   | (Optional) Override the default docker image used for the Kafka server in the tests. **Default**: `confluentinc/cp-kafka:7.3.1`.                                                                                                                                                    |
| notes             | String                   | (Optional) A notes field. Ignored by the system tests. Can be used to document intent.                                                                                                                                                                                              |

For example, the following defines a suite that turns off ordering requirements for expectation records:

##### **`no-ordering-suite.yml`**
```yaml
---
name: Test suite with expectation ordering disabled
options:
  - !creek/kafka-options@1
    outputOrdering: NONE
    notes: ordering turned off because blah blah.
services:
  - some-service
tests:
  - name: test 1
    inputs:
      - some_input
    expectations:
      - unordered_output
```

### Input model extensions

The Kafka test extension registers a `creek/kafka-topic@1` input model extension. 
This can be used to define seed and input records to be produced to Kafka.
It supports the following properties:

| Property Name | Property Type           | Description                                                                                                                                                            |
|---------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic         | String                  | (Optional) A default topic name that will be used for any `records` that do not define their own. If not set, any records without a topic set will result in an error. |
| cluster       | String                  | (Optional) A default cluster name that will be used for any `records` that do not define their own. If not set, any records will default to the default cluster name.  |
| notes         | String                  | (Optional) A notes field. Ignored by the system tests. Can be used to document intent.                                                                                 |
| records       | Array of `TopicRecord`s | (Required) The records to produce to Kafka.                                                                                                                            |

Each `TopicRecord` supports the following properties:

| Property Name | Property Type | Description                                                                                                                                                             |
|---------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic         | String        | (Optional) The topic to produce the record to. If not set, the file level default `topic` will be used. Neither being set will result in an error.                      |
| cluster       | String        | (Optional) The cluster to produce the record to. If not set, the file level default `cluster` will be used. If neither are set the `default` cluster will be used.      |
| key           | Any           | (Optional) The key of the record to produce. The type can be any type supported by the topic's key serde. If not set, the produced record will have a `null` key.       |
| value         | Any           | (Optional) The value of the record to produce. The type can be any type supported by the topic's value serde. If not set, the produced record will have a `null` value. |
| notes         | String        | (Optional) An optional notes field. Ignored by the system tests. Can be used to document intent.                                                                        |

For example, the following defines an input that will produce two records to an `input` topic on the `default` cluster:

##### **`inputs/produce_input.yml`**
```yaml
---
!creek/kafka-topic@1
topic: input
records:
  - key: 1
    value: foo
  - notes: this record has no value set. The record produced to kafka will therefore have a null value.
    key: 2    
```

### Expectation model extensions

The Kafka test extension registers a `creek/kafka-topic@1` expectation model extension.
This can be used to define the records services are expected to produce to Kafka.
It supports the following properties:

| Property Name | Property Type           | Description                                                                                                                                                            |
|---------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic         | String                  | (Optional) A default topic name that will be used for any `records` that do not define their own. If not set, any records without a topic set will result in an error. |
| cluster       | String                  | (Optional) A default cluster name that will be used for any `records` that do not define their own. If not set, any records will default to the default cluster name.  |
| notes         | String                  | (Optional) A notes field. Ignored by the system tests. Can be used to document intent.                                                                                 |
| records       | Array of `TopicRecord`s | (Required) The records to produce to Kafka.                                                                                                                            |

Each `TopicRecord` supports the following properties:

| Property Name | Property Type | Description                                                                                                                                                          |
|---------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic         | String        | (Optional) The topic to consume the record from. If not set, the file level default `topic` will be used. Neither being set will result in an error.                 |
| cluster       | String        | (Optional) The cluster to consume the record from. If not set, the file level default `cluster` will be used. If neither are set the `default` cluster will be used. |
| key           | Any           | (Optional) The expected key of the record. If not set, the consumed record's key will be ignored.                                                                    |
| value         | Any           | (Optional) The expected value of the record. If not set, the consumed record's value will be ignored.                                                                |
| notes         | String        | (Optional) An optional notes field. Ignored by the system tests. Can be used to document intent.                                                                     |

For example, the following defines an expectation that two records will be produced to the `output` topic on the `primary` cluster:

##### **`inputs/produce_input.yml`**
```yaml
---
!creek/kafka-topic@1
topic: input
cluster: primary
records:
  - notes: this record expectation does not define any value, meaning the value is ignored, i.e. it can hold any value.
    key: 1    
  - notes: this record expectation explicitly requires the value to be null
    key: 2
    value: ~
```

[gradle-system-test-plugin]: https://github.com/creek-service/creek-system-test-gradle-plugin
