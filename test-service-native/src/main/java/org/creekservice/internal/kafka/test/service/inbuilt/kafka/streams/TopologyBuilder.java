/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.api.kafka.test.service.inbuilt.NativeServiceDescriptor.InputTopic;
import static org.creekservice.api.kafka.test.service.inbuilt.NativeServiceDescriptor.OutputTopic;

import java.util.Objects;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.streams.extension.util.Name;

public final class TopologyBuilder {

    private static final String PRODUCE_BAD_KEY = "produce-bad-key";

    private final KafkaClientsExtension ext;
    private final Name name = Name.root();

    public TopologyBuilder(final KafkaClientsExtension ext) {
        this.ext = requireNonNull(ext, "ext");
    }

    public Topology build() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KafkaTopic<String, Long> inputTopic = ext.topic(InputTopic);

        builder.stream(
                        inputTopic.name(),
                        Consumed.with(inputTopic.keySerde(), inputTopic.valueSerde())
                                .withName(name.name("ingest-" + inputTopic.name())))
                .peek((k, v) -> System.out.println("received " + k + "-> " + v))
                .split(name.named("branch-"))
                .branch(
                        this::shouldProduceBadOutput,
                        Branched.withConsumer(this::handleBadOutput).withName("bad-output"))
                .defaultBranch(
                        Branched.withConsumer(this::handleGoodOutput).withName("good-output"));

        return builder.build(ext.properties(DEFAULT_CLUSTER_NAME));
    }

    private void handleGoodOutput(final KStream<String, Long> branch) {
        final KafkaTopic<Long, String> outputTopic = ext.topic(OutputTopic);

        branch.map((k, v) -> new KeyValue<>(v, k), name.named("fliparoo"))
                .peek((k, v) -> System.out.println("producing good output: " + k + "-> " + v))
                .to(
                        outputTopic.name(),
                        Produced.with(outputTopic.keySerde(), outputTopic.valueSerde())
                                .withName(name.name("good-egress-" + outputTopic.name())));
    }

    private void handleBadOutput(final KStream<String, Long> branch) {
        final KafkaTopic<Long, String> outputTopic = ext.topic(OutputTopic);

        branch.map(this::generateBadOutput, name.named("generate-bad-output"))
                .peek((k, v) -> System.out.println("producing bad output: " + k + "-> " + v))
                .to(
                        outputTopic.name(),
                        // Note: serde deliberately wrong way round:
                        Produced.with(outputTopic.valueSerde(), outputTopic.keySerde())
                                .withName(name.name("bad-egress-" + outputTopic.name())));
    }

    private KeyValue<String, Long> generateBadOutput(final String key, final Long value) {
        // Don't flip key & value, causing a string to be serialized as the key,
        // where a long is expected. Note, doing the same for the value won't
        // cause a deserialization as String constructor accepts any byte sequence.
        return new KeyValue<>(key, null);
    }

    private boolean shouldProduceBadOutput(final String key, final Long value) {
        return Objects.equals(key, PRODUCE_BAD_KEY);
    }
}
