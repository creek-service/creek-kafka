# Creek Kafka Serde

Module containing the types needed to implement serde for the Creek Kafka extensions.

Serialization formats are pluggable. Each format a component uses must be matched by exactly one implementation
of [`KafkaSerdeProvider`][1] available at runtime that handles the format. 

[1]: src/main/java/org/creek/api/kafka/serde/provider/KafkaSerdeProvider.java