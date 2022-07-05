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

package org.creekservice.api.kafka.streams.test;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.creekservice.api.kafka.common.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.service.context.CreekContext;

/**
 * Test helper methods for creating input and output topics when using Creek and {@link
 * TopologyTestDriver}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * private CreekContext ctx;
 * private TopologyTestDriver testDriver;
 * private TestInputTopic<String, Long> inputTopic;
 * private TestOutputTopic<Long, String> outputTopic;
 *
 * @BeforeEach
 * public void setUp() {
 *   ...
 *
 *   testDriver = new TopologyTestDriver(topology, ext.properties());
 *
 *   inputTopic = inputTopic(InputTopic, ctx, testDriver);
 *   outputTopic = outputTopic(OutputTopic, ctx, testDriver);
 * }
 * }</pre>
 */
public final class TestTopics {

    private TestTopics() {}

    public static <K, V> TestInputTopic<K, V> inputTopic(
            final KafkaTopicDescriptor<K, V> topic,
            final CreekContext ctx,
            final TopologyTestDriver testDriver) {
        final TopicSerde<K, V> serde = topicSerde(topic, ctx);
        return testDriver.createInputTopic(
                topic.name(), serde.keySerde.serializer(), serde.valueSerde.serializer());
    }

    public static <K, V> TestInputTopic<K, V> inputTopic(
            final KafkaTopicDescriptor<K, V> topic,
            final CreekContext ctx,
            final TopologyTestDriver testDriver,
            final Instant startTimestamp,
            final Duration autoAdvance) {
        final TopicSerde<K, V> serde = topicSerde(topic, ctx);
        return testDriver.createInputTopic(
                topic.name(),
                serde.keySerde.serializer(),
                serde.valueSerde.serializer(),
                startTimestamp,
                autoAdvance);
    }

    public static <K, V> TestOutputTopic<K, V> outputTopic(
            final KafkaTopicDescriptor<K, V> topic,
            final CreekContext ctx,
            final TopologyTestDriver testDriver) {
        final TopicSerde<K, V> serde = topicSerde(topic, ctx);
        return testDriver.createOutputTopic(
                topic.name(), serde.keySerde.deserializer(), serde.valueSerde.deserializer());
    }

    private static <K, V> TopicSerde<K, V> topicSerde(
            final KafkaTopicDescriptor<K, V> def, final CreekContext ctx) {
        final KafkaStreamsExtension ext = ctx.extension(KafkaStreamsExtension.class);
        final KafkaTopic<K, V> topic = ext.topic(def);
        final TopicSerde<K, V> serde = new TopicSerde<>(topic.keySerde(), topic.valueSerde());

        final Map<String, Object> props = new HashMap<>();
        final Properties properties = ext.properties(def.cluster());
        properties.stringPropertyNames().forEach(k -> props.put(k, properties.getProperty(k)));

        serde.configure(props);
        return serde;
    }

    private static final class TopicSerde<K, V> {
        final Serde<K> keySerde;
        final Serde<V> valueSerde;

        private TopicSerde(final Serde<K> keySerde, final Serde<V> valueSerde) {
            this.keySerde = requireNonNull(keySerde, "keySerde");
            this.valueSerde = requireNonNull(valueSerde, "valueSerde");
        }

        void configure(final Map<String, ?> config) {
            keySerde.configure(config, true);
            valueSerde.configure(config, false);
        }
    }
}
