# Creek Kafka Serde

Module containing the types needed to implement serde for the Creek Kafka extensions.

Serialization formats are pluggable. Each format a component uses must be matched by exactly one implementation
of [`KafkaSerdeProvider`][1] available at runtime that handles the format. 

This module includes a `kafka` format, which supports the standard Kafka set of serializers, 
for example `Long`, `UUID`, `String` and others. See `org.apache.kafka.common.serialization.Serdes` for more information.
This format can be used by having [`KafkaTopicDescriptor`][2]'s return `"kafka"` as the serialization format one of
more of their message parts.

[1]: src/main/java/org/creekservice/api/kafka/serde/provider/KafkaSerdeProvider.java
[2]: ../metadata/src/main/java/org/creekservice/api/kafka/metadata/KafkaTopicDescriptor.java
