# Creek Kafka Metadata

Provides metadata types that define Kafka topics:

* **[KafkaTopic](src/main/java/org/creek/api/kafka/metadata/KafkaTopic.java)**:
  defines metadata about a Kafka topic.
* **[CreatableKafkaTopic](src/main/java/org/creek/api/kafka/metadata/CreatableKafkaTopic.java)**: 
  extends `KafkaTopic` with additional information required to create the topic.