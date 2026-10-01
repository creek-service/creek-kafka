/*
 * Copyright 2026-2026 Creek Contributors (https://github.com/creek-service)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.creekservice.internal.kafka.test.service.inbuilt.kafka.streams;

import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;

import java.util.Collection;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.test.service.inbuilt.PipelineServiceDescriptor;

public final class PipelineTopology {
    private PipelineTopology() {}

    public static Topology build(
            final KafkaClientsExtension ext, final PipelineServiceDescriptor service) {
        final KafkaTopic<String, String> from = topic(ext, service.inputs(), "input");
        final KafkaTopic<String, String> to = topic(ext, service.outputs(), "output");
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(from.name(), Consumed.with(from.keySerde(), from.valueSerde()))
                .mapValues(value -> value + "-" + service.stage())
                .to(to.name(), Produced.with(to.keySerde(), to.valueSerde()));
        return builder.build(ext.properties(DEFAULT_CLUSTER_NAME));
    }

    private static KafkaTopic<String, String> topic(
            final KafkaClientsExtension ext, final Collection<?> resources, final String role) {
        if (resources.size() != 1) {
            throw new IllegalArgumentException("Pipeline service must have exactly one " + role);
        }
        final Object resource = resources.iterator().next();
        if (!(resource instanceof KafkaTopicDescriptor<?, ?> descriptor)
                || descriptor.key().type() != String.class
                || descriptor.value().type() != String.class) {
            throw new IllegalArgumentException(
                    "Pipeline service has an invalid " + role + " topic");
        }
        @SuppressWarnings("unchecked")
        final KafkaTopicDescriptor<String, String> topic =
                (KafkaTopicDescriptor<String, String>) descriptor;
        return ext.topic(topic);
    }
}
