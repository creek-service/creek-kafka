/**
 * Dependency-light module containing creek service model extensions to allow services to define
 * dependencies on Kafka resources.
 */
module creek.kafka.metadata {
    requires transitive creek.platform.metadata;

    exports org.creekservice.api.kafka.metadata;
}
