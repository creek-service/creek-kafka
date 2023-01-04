import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionProvider;
import org.creekservice.api.service.extension.CreekExtensionProvider;

/** A service extension that provides functionality to work with Kafka Streams apps. */
module creek.kafka.streams.extension {
    requires transitive creek.kafka.clients.extension;
    requires transitive kafka.streams;
    requires creek.observability.logging;
    requires creek.observability.lifecycle;
    requires creek.base.type;

    exports org.creekservice.api.kafka.streams.extension;
    exports org.creekservice.api.kafka.streams.extension.exception;
    exports org.creekservice.api.kafka.streams.extension.observation;
    exports org.creekservice.api.kafka.streams.extension.util;

    provides CreekExtensionProvider with
            KafkaStreamsExtensionProvider;
}
