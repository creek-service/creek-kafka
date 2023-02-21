[![javadoc](https://javadoc.io/badge2/org.creekservice/creek-kafka-streams-extension/javadoc.svg)](https://javadoc.io/doc/org.creekservice/creek-kafka-streams-extension)
# Creek Kafka Streams Extension

Provides an extension to Creek to allow it to work with Kafka Streams and Kafka resources.

By default, if the `creek-kafka-streams-extension.jar` is on the class or module path, Creek will load the
extension and use it to handle any topic resources and provide functionality for Kafka Streams based apps.

## Configuration

### Extension options

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

### System environment variables

An alternative to using `KafkaStreamsExtensionOptions` to configure Kafka client properties is to use environment 
variables. By default, any environment variable prefixed with `KAFKA_` will be passed to the Kafka clients.

It is common to pass `bootstrap.servers` and authentication information to the service in this way, so that different
values can be passed in different environments. For example, `bootstrap.servers` cam be passed by setting a
`KAFKA_BOOTSTRAP_SERVERS` environment variable. This is how the [system tests][systemTest] pass the Kafka bootstrap
to your service.

See [`SystemEnvPropertyOverrides`][2] for more info, including multi-cluster support.

This behaviour is customizable. See [`KafkaStreamsExtensionOptions`][1]`.withKafkaPropertiesOverrides` for more info.

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
[2]: ../client-extension/src/main/java/org/creekservice/api/kafka/extension/config/SystemEnvPropertyOverrides.java
[systemTest]: https://github.com/creek-service/creek-system-test
[todo]: update links above once doccs migrated to creekservice.org