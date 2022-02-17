# Creek Kafka Metadata

Provides metadata types that can be exposed from an Aggregate's or Service's descriptor:

## Inputs

Input topics are topics a service/aggregate consumes from:

* `OwnedKafkaTopicInput`: an input topic that is conceptually owned by the component.
* `KafkaTopicInput`: an input topic that is not conceptually owned by the component, i.e. it is an owned output topic of another service.

## Internals

Internal topics are things like changelog topics for internal state stores and repartition topics:

* **`KafkaTopicInternal`**: used to represent an internal topic that is implicitly created, e.g. a changelog or repartition topic.
* **`CreatableKafkaTopicInternal`**: used to represent an internal topic that is not implicitly created and hence should be created during deployment. 

## Outputs

Output topics are topics that a service/aggregate produce to:

* `OwnedKafkaTopicOutput`: an output topic that is conceptually owned by the component.
* `KafkaTopicOutput`: an output topic that is not conceptually owned by the component, i.e. it is an owned input topic of another service.
