/*
 * Copyright 2022 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.test.service.kafka.streams;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.api.kafka.test.service.TestServiceDescriptor.InputTopic;
import static org.creekservice.api.kafka.test.service.TestServiceDescriptor.OutputTopic;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.creekservice.api.kafka.common.resource.KafkaTopic;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.streams.extension.util.Name;

public final class TopologyBuilder {

    private final KafkaStreamsExtension ext;
    private final Name name = Name.root();

    public TopologyBuilder(final KafkaStreamsExtension ext) {
        this.ext = requireNonNull(ext, "ext");
    }

    public Topology build() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KafkaTopic<String, Long> inputTopic = ext.topic(InputTopic);
        final KafkaTopic<Long, String> outputTopic = ext.topic(OutputTopic);

        builder.stream(
                        inputTopic.name(),
                        Consumed.with(inputTopic.keySerde(), inputTopic.valueSerde())
                                .withName(name.name("ingest-" + inputTopic.name())))
                .peek((k, v) -> System.out.println("received " + k + "-> " + v))
                .transform(switchKeyAndValue(), name.named("switch"))
                .peek((k, v) -> System.out.println("producing " + k + "-> " + v))
                .to(
                        outputTopic.name(),
                        Produced.with(outputTopic.keySerde(), outputTopic.valueSerde())
                                .withName(name.name("egress-" + outputTopic.name())));

        return builder.build(ext.properties(DEFAULT_CLUSTER_NAME));
    }

    private TransformerSupplier<String, Long, KeyValue<Long, String>> switchKeyAndValue() {
        return () ->
                new Transformer<>() {
                    @Override
                    public void init(final ProcessorContext context) {}

                    @Override
                    public KeyValue<Long, String> transform(final String key, final Long value) {
                        return new KeyValue<>(value, key);
                    }

                    @Override
                    public void close() {}
                };
    }
}
