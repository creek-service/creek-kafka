/*
 * Copyright 2023-2025 Creek Contributors (https://github.com/creek-service)
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

package com.acme.examples.streams;

import com.acme.examples.service.MyServiceDescriptor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.streams.extension.util.Name;

import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;


// begin-snippet: topology-builder
public final class TopologyBuilder {

    private final KafkaStreamsExtension ext;
    private final Name name = Name.root();

    public TopologyBuilder(final KafkaStreamsExtension ext) {
        this.ext = ext;
    }

    public Topology build() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Retrieve type-safe topic metadata and serde:
        final KafkaTopic<Long, String> input = ext.topic(MyServiceDescriptor.InputTopic);
        final KafkaTopic<Long, String> output = ext.topic(MyServiceDescriptor.OutputTopic);

        builder.stream(
                        input.name(),
                        // Type-safe access to topic serde:
                        Consumed.with(input.keySerde(), input.valueSerde())
                                .withName(name.name("ingest-" + input.name())))
                // ... perform business logic
                .to(
                        output.name(),
                        // Type-safe access to topic serde:
                        Produced.with(output.keySerde(), output.valueSerde())
                                .withName(name.name("egress-" + output.name())));

        // Grab the cluster properties from Creek to build and return the Topology:
        return builder.build(ext.properties(DEFAULT_CLUSTER_NAME));
    }
}
// end-snippet