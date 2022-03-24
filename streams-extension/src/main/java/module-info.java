
import org.creek.api.service.extension.CreekExtensionBuilder;

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

    exports org.creek.api.kafka.streams.extension;
    exports org.creek.api.kafka.streams.observation;
    exports org.creek.api.kafka.streams.util;

    provides CreekExtensionBuilder with
            org.creek.internal.kafka.streams.extension.KafkaStreamsExtensionBuilder;
}
