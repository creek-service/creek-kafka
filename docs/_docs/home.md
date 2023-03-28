---
title: Creek Kafka
permalink: /
layout: single
header:
  overlay_color: "#000"
  overlay_filter: "0.5"
  overlay_image: /assets/images/background-1.png
excerpt: A Creek extension to take the heavy lifting out of building and testing Kafka Streams and Kafka Client based microservices.
toc: true
classes: extra_wide
snippet_comment_prefix: "//"
---

The Kafka extensions to Creek make writing Kafka Client and Kafka Streams based microservices easy.

The source code is available on GitHub: [<i class="fab fa-fw fa-github"/>&nbsp; View on GitHub][onGitHub]{: .btn .btn--success}

An example of how to write Kafka Streams based microservices using this Creek extension can be found in the
aptly named [Basic Kafka Streams Tutorial][basicDemo].

## Kafka client compatability

The extentions themselves are compiled with the latest versions of the Kafka clients and Kafka streams libraries.
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
| 3.4.+         | 3.4.0          | Supported & tested                              |
| > 3.4         |                | Not currently tested / released. Should work... |

In Gradle, it is possible to force the use of an older Kafka client, if you wish, using a resolution strategy.
In Kotlin, this looks like:

{% highlight kotlin %}
{% include_snippet resolution-strategy from ../docs-examples/build.gradle.kts %}
{% endhighlight %}

## Metadata types

The `creek-kafka-metadata.jar` contains metadata types that can be used in aggregate and service descriptor to define
Kafka resources the component uses or exposes.

To use the metadata, simply the `creek-kafka-metadata.jar` as a dependency to the `api` or `services` module.

{% highlight kotlin %}
{% include_snippet deps from ../docs-examples/build.gradle.kts %}
{% include_snippet meta from ../docs-examples/build.gradle.kts %}
}
{% endhighlight %}

### Input types

Input topics are topics a service/aggregate consumes from:

* `OwnedKafkaTopicInput`: an input topic that is conceptually owned by the component.
* `KafkaTopicInput`: an input topic that is not conceptually owned by the component, i.e. it is an owned output topic of another service.

### Internal types

Internal topics are things like changelog topics for internal state stores and repartition topics:

* **`KafkaTopicInternal`**: used to represent an internal topic that is implicitly created, e.g. a changelog or repartition topic.
* **`CreatableKafkaTopicInternal`**: used to represent an internal topic that is not implicitly created and hence should be created during deployment.

### Output types

Output topics are topics that a service/aggregate produce to:

* `OwnedKafkaTopicOutput`: an output topic that is conceptually owned by the component.
* `KafkaTopicOutput`: an output topic that is not conceptually owned by the component, i.e. it is an owned input topic of another service.

## Extensions

### Kafka Clients Extension

Provides an extension to Creek to allow it to work with Kafka resources using standard Kafka Clients, e.g. 
producers, consumers and the admin client.

By default, if the `creek-kafka-extension.jar` is on the class or module path, Creek will load the
extension and use it to handle any topic resources.

To use the extension, simply the `creek-kafka-extension.jar` as a dependency to a service.

{% highlight kotlin %}
{% include_snippet deps from ../docs-examples/build.gradle.kts %}
{% include_snippet client-ext from ../docs-examples/build.gradle.kts %}
}
{% endhighlight %}

#### Basic usage

{% highlight java %}
{% include_snippet service-main from ../docs-examples/src/main/java/com/acme/examples/clients/ServiceMain.java %}
}
{% endhighlight %}

#### Configuration

##### Extension options

**Note:** It's common to use [System environment variables](#system-environment-variables) to configure
Kafka for settings such as `boostrap.servers` and authentication information.
{: .notice--warning}

The extension can be configured by passing an instance of [`KafkaClientsExtensionOptions`][clientOptions] when creating
the Creek context. For example,

{% highlight java %}
{% include_snippet extension-options from ../docs-examples/src/main/java/com/acme/examples/clients/ServiceMain.java %}
}
{% endhighlight %}

See [`KafkaClientsExtensionOptions`][clientOptions] for more info.

##### System environment variables

An alternative to using `KafkaClientsExtensionOptions` to configure Kafka client properties is to use environment
variables. By default, any environment variable prefixed with `KAFKA_` will be passed to the Kafka clients.

It is common to pass `bootstrap.servers` and authentication information to the service in this way, so that different
values can be passed in different environments. For example, `bootstrap.servers` cam be passed by setting a
`KAFKA_BOOTSTRAP_SERVERS` environment variable. This is how the [system tests][systemTest] pass the Kafka bootstrap
to your service.

See [`SystemEnvPropertyOverrides`][envPropOverrides] for more info, including multi-cluster support.

This behaviour is customizable. See [`KafkaClientsExtensionOptions.Builder.withKafkaPropertiesOverrides`][clientPropOverrides] for more info.

### Kafka Streams extension

Provides an extension to Creek to allow it to work with Kafka Streams and Kafka resources.
The extension extends the functionality provided by the [Kafka client extension](#kafka-clients-extension),
adding Streams specific functionality.

By default, if the `creek-kafka-streams-extension.jar` is on the class or module path, Creek will load the
extension and use it to handle any topic resources and provide functionality for Kafka Streams based apps.

To use the extension, simply the `creek-kafka-streams-extension.jar` as a dependency to a service.

{% highlight kotlin %}
{% include_snippet deps from ../docs-examples/build.gradle.kts %}
{% include_snippet streams-ext from ../docs-examples/build.gradle.kts %}
}
{% endhighlight %}

#### Basic usage

{% highlight java %}
{% include_snippet service-main from ../docs-examples/src/main/java/com/acme/examples/streams/ServiceMain.java %}
}

{% include_snippet topology-builder from ../docs-examples/src/main/java/com/acme/examples/streams/TopologyBuilder.java %}
{% endhighlight %}

#### Configuration

##### Extension options

**Note:** It's common to use [System environment variables](#system-environment-variables-1) to configure
Kafka for settings such as `boostrap.servers` and authentication information.
{: .notice--warning}

The extension can be configured by passing an instance of [`KafkaStreamsExtensionOptions`][streamsOptions] when creating
the Creek context. For example,

{% highlight java %}
{% include_snippet extension-options from ../docs-examples/src/main/java/com/acme/examples/streams/ServiceMain.java %}
{% endhighlight %}

See [`KafkaStreamsExtensionOptions`][streamsOptions] for more info.

##### System environment variables

An alternative to using `KafkaStreamsExtensionOptions` to configure Kafka client properties is to use environment
variables. By default, any environment variable prefixed with `KAFKA_` will be passed to the Kafka clients.

It is common to pass `bootstrap.servers` and authentication information to the service in this way, so that different
values can be passed in different environments. For example, `bootstrap.servers` cam be passed by setting a
`KAFKA_BOOTSTRAP_SERVERS` environment variable. This is how the [system tests][systemTest] pass the Kafka bootstrap
to your service.

See [`SystemEnvPropertyOverrides`][envPropOverrides] for more info, including multi-cluster support.

This behaviour is customizable. See [`KafkaStreamsExtensionOptions.Builder.withKafkaPropertiesOverrides`][streamPropOverrides] for more info.

#### Unit testing topologies

**ProTip:** Creek recommends focusing on [system-tests][systemTest] for testing the combined business functionality
of a service, or services. Of course, unit testing still has its place.
{: .notice--info}

The `creek-kafka-streams-test.jar` contains test helpers, that can be added as a test dependency, to help with 
unit testing Kafka Streams topologies.

{% highlight kotlin %}
{% include_snippet deps from ../docs-examples/build.gradle.kts %}
{% include_snippet streams-test from ../docs-examples/build.gradle.kts %}
}
{% endhighlight %}

##### Writing topology unit tests

This module provides classes to make it easier to write unit tests for Kafka Streams' topologies:

* [`TestKafkaStreamsExtensionOptions`][testKsExtOpt] can be used to configure the [streams extension](#kafka-streams-extension) for unit testing.
* [`TestTopics`][testTopics] can be used to create input and output topics when using [`TopologyTestDriver`][ksTest].
  
For example:

{% highlight java %}
{% include_snippet topology-builder-test from ../docs-examples/src/test/java/com/acme/examples/streams/TopologyBuilderTest.java %}
{% endhighlight %}

##### Using with JPMS

The Java platform module system will complain about split packages because both the _Kafka Streams_ and _Kafka Streams Test_ jars expose
classes in the same packages:

```shell
error: module some.module reads package org.apache.kafka.streams from both kafka.streams and kafka.streams.test.utils
```

This is tracked by Apache Kafka in issue [KAFKA-7491](https://issues.apache.org/jira/browse/KAFKA-7491). 

Until the issue is fixed, or if using an older version of Kafka, any module using both jars under JPMS will need to 
patch the streams test jar into the Kafka streams jar.

The module patching can be achieved using the `org.javamodularity.moduleplugin` plugin:

{% highlight kotlin %}
plugins {
{% include_snippet module-plugin from ../docs-examples/build.gradle.kts %}
}

{% include_snippet patch-module from ../docs-examples/build.gradle.kts %}
{% endhighlight %}

## System test extension

The `creek-kafka-test-extension.jar` is a [Creek system test][systemTest] extension, allowing system tests that 
seed Kafka clusters with test data, produce inputs to Kafka topics, and assert expectations of records in Kafka topics.

### Configuring the extension

The test extension can be added to any module that runs system tests.
How this is done will depend on the build plugin being used to run system tests.
For the [gradle plugin][gradle-system-test-plugin] the text extension can be added using the `systemTestExtension`
dependency configuration:

{% highlight kotlin %}
{% include_snippet deps from ../docs-examples/build.gradle.kts %}
{% include_snippet test-ext from ../docs-examples/build.gradle.kts %}
}
{% endhighlight %}

**ProTip:** The `systemTestExtension` dependency configuration is added by the `org.creekservice.system.test` 
Gradle plugin.
{: .notice--info}

### Test model

The extension registers the following model subtypes to support system testing of Kafka based microservices:

#### Option model extensions

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

[todo]: http://read-these-example-yaml-files-from-disk

#### Input model extensions

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

#### Expectation model extensions

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

## Kafka serialization formats

The `creek-kafka-serde.jar` provides the base types used to define and register a serde provider for Creek Kafka.

Serialization formats are pluggable. allowing users to plug in their own custom serialization formats, should they want. 
Each format a component uses must have exactly one matching [`KafkaSerdeProvider`][serdeProvider] implementation available
at runtime, on the class-path or module-path.

Currently supported serialization formats:

| Serialization format     | Notes                                              |
|--------------------------|----------------------------------------------------|
| [`kafka`](#kafka-format) | Serialization using the Kafka clients serializers. |
| [`json`](#json-format)   | Schema validated JSON serialization                |

...or extend Creek with a [custom format](#custom-formats). 

### `kafka` format

Creek Kafka comes with an in-built `kafka` serialization format, which supports the standard set of Kafka serializers.
The serializer handles the following types:

| Java type    | Serde class               |
|--------------|---------------------------|
| `UUID`       | `Serdes.UUIDSerde`        |
| `long`       | `Serdes.LongSerde`        |
| `Long`       | `Serdes.LongSerde`        |
| `int`        | `Serdes.IntegerSerde`     |
| `Integer`    | `Serdes.IntegerSerde`     |
| `short`      | `Serdes.ShortSerde`       |
| `Short`      | `Serdes.ShortSerde`       |
| `float`      | `Serdes.FloatSerde`       |
| `Float`      | `Serdes.FloatSerde`       |
| `double`     | `Serdes.DoubleSerde`      |
| `Double`     | `Serdes.DoubleSerde`      |
| `String`     | `Serdes.StringSerde`      |
| `ByteBuffer` | `Serdes.ByteBufferSerde`  |
| `Bytes`      | `Serdes.BytesSerde`       |
| `byte[]`     | `Serdes.ByteArraySerde`   |
| `Void`       | `Serdes.VoidSerde`        |


This format can be used by having [`KafkaTopicDescriptor.PartDescriptor.format()`][topicPartFormatMethod] method return
`kafka` as the serialization format.

For an example of using the `kafka` format in resource descriptors see the [`TopicDescriptors`][topicDescriptors] class,
or the [basic Kafka Streams tutorial][basicDemo].

### `json` format

A schema-validated JSON format is currently [in development](https://github.com/creek-service/creek-kafka/issues/25).

### Custom formats

Creek Kafka has a pluggable serialization format, allowing new formats to be added easily.
New formats should implement the `KafkaSerdeProvider` interface:

{% highlight java %}
{% include_snippet kafka-serde-provider from ../serde/src/main/java/org/creekservice/api/kafka/serde/provider/KafkaSerdeProvider.java %}
{% endhighlight %}

The provider will be used to create `Serde` for any topic keys or values that use the provider's `format()`.
The provider can query the `part`, passed to `create`, to obtain the class of the key or value part.

#### Registering custom formats

Serialization formats are discovered from the class-path and module-path using the standard Java [`ServiceLoader`][serviceLoader].

How to make a serialization format discoverable by Creek will depend on whether it is on the JVM's class or module path.

**ProTip:** We suggest registering component descriptors for use on both the class-path _and_ module-path, 
ensuring they work today and tomorrow.
{: .notice--info}

##### Formats on the module path

If the format resides in a module it needs to be declared in the module's descriptor, i.e. in the `module-info.java` file,
as a provider of the `KafkaSerdeProvider` type:

{% highlight java %}
{% include_snippet module-name from ../docs-examples/src/main/java/module-info.java %}
{% include_snippet serde-deps from ../docs-examples/src/main/java/module-info.java %}
{% include_snippet serde-reg from ../docs-examples/src/main/java/module-info.java %}
}
{% endhighlight %}

##### Formats on the class path

If the format does not reside in a module, or the jar is on the class-path, it is registered by placing a 
_provider-configuration_ file in the `META-INF/services` resource directory.

This is a plain text file named `org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider`. Add the fully-qualified
name of the type implementing the `KafkaSerdeProvider` type to this file:

```
c{% include_snippet all from ../docs-examples/src/main/resources/org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider %}
```

#### Testing format registration

The `creek-kafka-serde-test` jar contains a test utility that will test a serialization format is registered correctly:

{% highlight java %}
{% include_snippet serde-tester from ../test-serde-java-eight/src/test/java/org/creekservice/test/api/kafka/serde/eight/test/KafkaSerdeProviderTesterTest.java %}
{% endhighlight %}

[onGitHub]: https://github.com/creek-service/creek-kafka
[basicDemo]: https://www.creekservice.org/basic-kafka-streams-demo/
[clientOptions]: https://javadoc.io/doc/org.creekservice/creek-kafka-client-extension/latest/creek.kafka.clients.extension/org/creekservice/api/kafka/extension/KafkaClientsExtensionOptions.html
[streamsOptions]: https://javadoc.io/doc/org.creekservice/creek-kafka-streams-extension/latest/creek.kafka.streams.extension/org/creekservice/api/kafka/streams/extension/KafkaStreamsExtensionOptions.html
[envPropOverrides]: https://javadoc.io/doc/org.creekservice/creek-kafka-client-extension/latest/creek.kafka.clients.extension/org/creekservice/api/kafka/extension/config/SystemEnvPropertyOverrides.html
[clientPropOverrides]: https://javadoc.io/doc/org.creekservice/creek-kafka-client-extension/latest/creek.kafka.clients.extension/org/creekservice/api/kafka/extension/KafkaClientsExtensionOptions.Builder.html#withKafkaPropertiesOverrides(org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides)
[streamPropOverrides]: https://javadoc.io/doc/org.creekservice/creek-kafka-streams-extension/latest/creek.kafka.streams.extension/org/creekservice/api/kafka/streams/extension/KafkaStreamsExtensionOptions.Builder.html#withKafkaPropertiesOverrides(org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides)
[serdeProvider]: https://javadoc.io/doc/org.creekservice/creek-kafka-serde/latest/creek.kafka.serde/org/creekservice/api/kafka/serde/provider/KafkaSerdeProvider.html
[serdes]: https://javadoc.io/doc/org.apache.kafka/kafka-clients/latest/org/apache/kafka/common/serialization/Serdes.html
[topicPartFormatMethod]: https://javadoc.io/static/org.creekservice/creek-kafka-metadata/0.4.0/creek.kafka.metadata/org/creekservice/api/kafka/metadata/KafkaTopicDescriptor.PartDescriptor.html#format()
[topicDescriptors]: https://github.com/creek-service/creek-kafka/blob/bb79516b4fba0fbda6d17cb9b82a3a6773913fe5/test-service/src/main/java/org/creekservice/internal/kafka/test/service/TopicDescriptors.java
[testKsExtOpt]: https://javadoc.io/doc/org.creekservice/creek-kafka-streams-test/latest/creek.kafka.streams.test/org/creekservice/api/kafka/streams/test/TestKafkaStreamsExtensionOptions.html
[testTopics]: https://javadoc.io/doc/org.creekservice/creek-kafka-streams-test/latest/creek.kafka.streams.test/org/creekservice/api/kafka/streams/test/TestTopics.html
[ksTest]: https://kafka.apache.org/documentation/streams/developer-guide/testing.html
[systemTest]: https://github.com/creek-service/creek-system-test
[gradle-system-test-plugin]: https://github.com/creek-service/creek-system-test-gradle-plugin
[serviceLoader]: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/ServiceLoader.html
[todo]: http://update links above once doccs migrated to creekservice.org