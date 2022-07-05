module creek.kafka.common {
    requires transitive creek.kafka.metadata;
    requires transitive creek.kafka.serde;
    requires transitive kafka.clients;
    requires creek.base.type;

    exports org.creekservice.api.kafka.common.config;
    exports org.creekservice.api.kafka.common.resource;
    exports org.creekservice.internal.kafka.common.resource to
            creek.kafka.streams.extension,
            creek.kafka.streams.test.extension;
    exports org.creekservice.internal.kafka.common.config to
            creek.kafka.streams.extension,
            creek.kafka.streams.test.extension;
}
