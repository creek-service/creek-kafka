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
    }
}
```

See [`KafkaStreamsExtensionOptions`][1] for more info.

[1]: src/main/java/org/creek/api/kafka/streams/extension/KafkaStreamsExtensionOptions.java