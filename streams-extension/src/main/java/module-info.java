
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionProvider;
import org.creekservice.api.service.extension.CreekExtensionProvider;

module creek.kafka.streams.extension {
    requires transitive creek.kafka.clients.extension;
    requires transitive kafka.streams;
    requires creek.observability.logging;
    requires creek.observability.lifecycle;

    exports org.creekservice.api.kafka.streams.extension;
    exports org.creekservice.api.kafka.streams.extension.exception;
    exports org.creekservice.api.kafka.streams.extension.observation;
    exports org.creekservice.api.kafka.streams.extension.util;

    provides CreekExtensionProvider with
            KafkaStreamsExtensionProvider;
}
