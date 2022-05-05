
import org.creekservice.api.service.extension.CreekExtensionBuilder;

module creek.kafka.streams.extension {
    requires transitive creek.kafka.metadata;
    requires transitive creek.kafka.common;
    requires transitive creek.kafka.serde;
    requires transitive creek.base.annotation;
    requires transitive creek.service.extension;
    requires transitive kafka.streams;
    requires transitive kafka.clients;
    requires creek.observability.logging;
    requires creek.base.type;
    requires com.github.spotbugs.annotations;

    exports org.creekservice.api.kafka.streams.extension;
    exports org.creekservice.api.kafka.streams.observation;
    exports org.creekservice.api.kafka.streams.util;

    provides CreekExtensionBuilder with
            org.creekservice.internal.kafka.streams.extension.KafkaStreamsExtensionBuilder;
}
