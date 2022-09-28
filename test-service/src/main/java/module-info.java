
import org.creekservice.api.platform.metadata.ComponentDescriptor;

module creek.kafka.test.service {
    requires transitive creek.kafka.metadata;
    requires creek.service.context;
    requires creek.kafka.streams.extension;
    requires org.apache.logging.log4j;

    exports org.creekservice.api.kafka.test.service;

    provides ComponentDescriptor with
            org.creekservice.api.kafka.test.service.TestServiceDescriptor,
            org.creekservice.api.kafka.test.service.UpstreamAggregateDescriptor;
}
