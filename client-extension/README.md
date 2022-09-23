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

        final KafkaClientsExtension ext = ctx.extension(KafkaClientsExtension.class);
        
        try (Consumer<Long, String> consumer = ext.topic(MyServiceDescriptor.InputTopic).consumer();
             Producer<Long, String> producer = ext.topic(MyServiceDescriptor.OutputTopic).producer()) {
            
            consumer.subscribe(List.of(MyServiceDescriptor.InputTopic.name()));
            
            while (running()) {
                Iterator<ConsumerRecord<Long, String>> records = consumer.poll(Duration.ofSeconds(1))
                        .records(MyServiceDescriptor.InputTopic.name())
                        .iterator();

                records.forEachRemaining(r -> 
                    producer.send(new ProducerRecord(MyServiceDescriptor.OutputTopic.name(), r.key(), r.value())));
            }
        }
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