module creek.kafka.streams.test {
    requires transitive creek.kafka.metadata;
    requires transitive creek.service.context;
    requires transitive kafka.streams;
    requires transitive creek.kafka.streams.extension;
    requires creek.test.util;

    exports org.creekservice.api.kafka.streams.test;
}
