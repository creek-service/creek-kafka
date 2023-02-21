[![javadoc](https://javadoc.io/badge2/org.creekservice/creek-kafka-streams-test/javadoc.svg)](https://javadoc.io/doc/org.creekservice/creek-kafka-streams-test)
# Creek Kafka Streams Test

Provides helpers for testing Kafka Streams based code.

## Writing topology unit tests

This module provides classes to make it easier to write unit tests for Kafka Streams' topologies:

* [`TestKafkaStreamsExtensionOptions`](src/main/java/org/creekservice/api/kafka/streams/test/TestKafkaStreamsExtensionOptions.java)
  Can be used to configure the [streams extension](../streams-extension) for unit tests. 
* [`TestTopics`](src/main/java/org/creekservice/api/kafka/streams/test/TestTopics.java)
  Can be used to create input and output topics when using `TopologyTestDriver`.

For example:

```java
class TopologyBuilderTest {
    private static CreekContext ctx;

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<Long, String> outputTopic;

    @BeforeAll
    public static void classSetup() {
        ctx = CreekServices.builder(new ExampleServiceDescriptor())
                .with(TestKafkaStreamsExtensionOptions.defaults())
                .build();
    }

    @BeforeEach
    public void setUp() {
        final KafkaStreamsExtension ext = ctx.extension(KafkaStreamsExtension.class);

        final Topology topology = new TopologyBuilder(ext).build();

        testDriver = new TopologyTestDriver(topology, ext.properties());

        inputTopic = inputTopic(InputTopic, ctx, testDriver);
        outputTopic = outputTopic(OutputTopic, ctx, testDriver);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldSwitchKeyAndValue() {
        // When:
        inputTopic.pipeInput("a", 1L);

        // Then:
        assertThat(outputTopic.readKeyValuesToList(), contains(pair(1L, "a")));
    }
}
```

## Using with Java platform's module system

The JPMS will complain about split packages because both the Kafka streams and streams test jars expose 
classes in the same packages:

```shell
error: module some.module reads package org.apache.kafka.streams from both kafka.streams and kafka.streams.test.utils
```

Therefore, to use both Kafka streams and the streams test jars under JPMS, it is necessary to patch 
the streams test jar into the Kafka streams jar. 

The module patching can be achieved using the `org.javamodularity.moduleplugin` plugin:

##### Groovy: Patching the Streams' test jar into the main jar
```groovy
plugins {
  id 'org.javamodularity.moduleplugin' version '1.8.12'
}

modularity.patchModule('kafka.streams', "kafka-streams-test-utils-${kafkaVersion}.jar")
```

##### Kotlin: Patching the Streams' test jar into the main jar
```kotlin
plugins {
  id("org.javamodularity.moduleplugin") version "1.8.12"
}

modularity.patchModule("kafka.streams", "kafka-streams-test-utils-$kafkaVersion.jar")
```
