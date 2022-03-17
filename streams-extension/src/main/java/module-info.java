module creek.kafka.streams.extension {
    requires transitive creek.kafka.metadata;
    requires transitive creek.base.annotation;
    requires transitive creek.service.extension;
    requires transitive kafka.streams;
    requires transitive kafka.clients;
    requires creek.observability.logging;
    requires creek.base.type;

    exports org.creek.api.kafka.streams.extension;
    exports org.creek.api.kafka.streams.observation;
    exports org.creek.api.kafka.streams.resource;
    exports org.creek.api.kafka.streams.util;
}
