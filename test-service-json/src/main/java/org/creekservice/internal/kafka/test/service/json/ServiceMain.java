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
package org.creekservice.internal.kafka.test.service.json;

import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.test.service.json.JsonServiceDescriptor;
import org.creekservice.api.kafka.test.service.json.model.OutputValue;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;

public final class ServiceMain {
    private ServiceMain() {}

    public static void main(final String... args) {
        try (CreekContext context = CreekServices.context(new JsonServiceDescriptor())) {
            final KafkaStreamsExtension ext = context.extension(KafkaStreamsExtension.class);
            final KafkaTopic<String, OutputValue> input = ext.topic(JsonServiceDescriptor.INPUT);
            final KafkaTopic<String, OutputValue> output = ext.topic(JsonServiceDescriptor.OUTPUT);
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream(input.name(), Consumed.with(input.keySerde(), input.valueSerde()))
                    .mapValues(
                            value ->
                                    new OutputValue(
                                            value.getKey().orElseThrow(), value.getValue() + 1))
                    .to(output.name(), Produced.with(output.keySerde(), output.valueSerde()));
            ext.execute(builder.build(ext.properties(DEFAULT_CLUSTER_NAME)));
        }
    }
}
