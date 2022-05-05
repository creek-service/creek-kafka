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