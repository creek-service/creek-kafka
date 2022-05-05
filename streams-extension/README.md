# Creek Kafka Streams Extension

Provides an extension to Creek to allow it to work with Kafka Streams and Kafka resources.

By default, if the `creek-kafka-streams-extension.jar` is on the class or module path, Creek will load the
extension and use it to handle any topic resources.

## Configuration

The extension can be configured by passing an instance of [`KafkaStreamsExtensionOptions`][1] when creating
the Creek context. For example,

```java
public class ServiceMain {
    public static void main(String... args) {
        CreekContext ctx = CreekServices.builder(new MyServiceDescriptor())
                .with(
                        KafkaStreamsExtensionOptions.builder()
                                .withKafkaProperty("someProp", "someValue")
                                .withMetricsPublishing(
                                        KafkaMetricsPublisherOptions.builder()
                                                .withPublishingPeriod(Duration.ofMinutes(5))
                                )
                                .build()
                )
                .build();

        final KafkaStreamsExtension ext = ctx.extension(KafkaStreamsExtension.class);
        final Topology topology = new TopologyBuilder(ext).build();
        ext.execute(topology);
    }
}
```

See [`KafkaStreamsExtensionOptions`][1] for more info.

## Building your topology

The extension provides easy access to topic serde, making writing your topologies more simple:

```java
public final class TopologyBuilder {

    private final KafkaStreamsExtension ext;
    private final Name name = Name.root();

    public TopologyBuilder(final KafkaStreamsExtension ext) {
        this.ext = requireNonNull(ext, "ext");
    }

    public Topology build() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KafkaTopic<Long, String> input = ext.topic(YourServiceDescriptor.InputTopic);
        final KafkaTopic<String, Long> output = ext.topic(YourServiceDescriptor.OutputTopic);

        builder.stream(
                        input.name(),
                        Consumed.with(input.keySerde(), input.valueSerde())
                                .withName(name.name("ingest-" + input.name())))
                .flatTransform(doStuff(), name.named("work-magic"))
                .to(
                        output.name(),
                        Produced.with(output.keySerde(), output.valueSerde())
                                .withName(name.name("egress-" + output.name())));

        return builder.build(ext.properties());
    }
}
```

[1]: src/main/java/org/creekservice/api/kafka/streams/extension/KafkaStreamsExtensionOptions.java