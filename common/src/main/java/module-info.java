module creek.kafka.common {
    requires transitive creek.kafka.metadata;
    requires transitive creek.kafka.serde;
    requires transitive kafka.clients;
    requires creek.base.type;

    exports org.creek.api.kafka.common.resource;
    exports org.creek.internal.kafka.common.resource to
            creek.kafka.streams.extension;
}
