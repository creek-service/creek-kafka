
import org.creekservice.api.system.test.extension.CreekTestExtension;
import org.creekservice.internal.kafka.streams.test.extension.KafkaStreamsTestExtension;

module creek.kafka.streams.test.extension {
    requires transitive creek.system.test.extension;
    requires creek.kafka.metadata;

    provides CreekTestExtension with
            KafkaStreamsTestExtension;
}
