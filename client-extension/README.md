# Creek Kafka Clients Extension

Provides an extension to Creek to allow it to work with Kafka resources.

By default, if the `creek-kafka-extension.jar` is on the class or module path, Creek will load the
extension and use it to handle any topic resources.

## Configuration

### Extension options

The extension can be configured by passing an instance of [`KafkaClientsExtensionOptions`][1] when creating
the Creek context. For example,

```java
public class ServiceMain {
    public static void main(String... args) {
        CreekContext ctx = CreekServices.builder(new MyServiceDescriptor())
                .with(
                        KafkaClientsExtensionOptions.builder()
                                .withKafkaProperty("someProp", "someValue")
                                .build()
                )
                .build();

        new ServiceMain(ctx.extension(KafkaClientsExtension.class)).run();
    }

    private final KafkaTopic<Long, String> inputTopic;
    private final KafkaTopic<Long, String> outputTopic;

    private ServiceMain(KafkaClientsExtension ext) {
        this.inputTopic = ext.topic(MyServiceDescriptor.InputTopic);
        this.outputTopic = ext.topic(MyServiceDescriptor.OutputTopic);
    }

    private void run() {
        try (Consumer<byte[], byte[]> consumer = ext.consumer();
             Producer<byte[], byte[]> producer = ext.producer()) {
            
            consumer.subscribe(List.of(inputTopic.name()));
            
            while (running()) {
                consumer.poll(Duration.ofSeconds(1))
                        .records(inputTopic.name())
                        .forEach(r -> processInput(r, producer));
            }
        }
    }

    private void processInput(final ConsumerRecord<byte[], byte[]> input, 
                              final Producer<byte[], byte[]> producer) {

        long key = inputTopic.deserializeKey(input.key());
        String value = inputTopic.deserializeValue(input.value());
        
        // ... do stuff with key & value.
        
        producer.send(new ProducerRecord(outputTopic.name(), 
                outputTopic.serializeKey(key), 
                outputTopic.serializeValue(value) 
                ));
    }
}
```

See [`KafkaClientsExtensionOptions`][1] for more info.

### System environment variables

An alternative to using `KafkaClientsExtensionOptions` to configure Kafka client properties is to use environment 
variables. By default, any environment variable prefixed with `KAFKA_` will be passed to the Kafka clients.

It is common to pass `bootstrap.servers` and authentication information to the service in this way, so that different
values can be passed in different environments. For example, `bootstrap.servers` cam be passed by setting a
`KAFKA_BOOTSTRAP_SERVERS` environment variable.

See [`SystemEnvPropertyOverrides`][2] for more info, including multi-cluster support.

This behaviour is customizable. See [`KafkaClientsExtensionOptions`][1]`.withKafkaPropertiesOverrides` for more info.

[1]: src/main/java/org/creekservice/api/kafka/extension/KafkaClientsExtensionOptions.java
[2]: src/main/java/org/creekservice/api/kafka/extension/config/SystemEnvPropertyOverrides.java