/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.service.context.CreekContext;

/**
 * Test helper methods for creating input and output topics when using Creek and the {@link
 * TopologyTestDriver}.
 *
 * <p>Example usage:
 *
 * <pre>
 * private CreekContext ctx;
 * private TopologyTestDriver testDriver;
 * private TestInputTopi&#60;String, Long&#62; inputTopic;
 * private TestOutputTopic&#60;Long, String&#62; outputTopic;
 *
 * &#64;BeforeEach
 * public void setUp() {
 *   ...
 *
 *   testDriver = new TopologyTestDriver(topology, ext.properties());
 *
 *   inputTopic = inputTopic(InputTopic, ctx, testDriver);
 *   outputTopic = outputTopic(OutputTopic, ctx, testDriver);
 * }
 * </pre>
 *
 * @deprecated create a version in each demo repo.
 */
@SuppressWarnings("resource")
@Deprecated(forRemoval = true, since = "0.4.5")
public final class TestTopics {

    private TestTopics() {}

    /**
     * Create a test input topic
     *
     * @param topicDescriptor the topic descriptor
     * @param ctx the creek context
     * @param testDriver the Streams topology test driver
     * @param <K> the topic key type
     * @param <V> the topic value type
     * @return the test input topic
     * @see TopologyTestDriver#createInputTopic(String,
     *     org.apache.kafka.common.serialization.Serializer,
     *     org.apache.kafka.common.serialization.Serializer)
     */
    public static <K, V> TestInputTopic<K, V> inputTopic(
            final KafkaTopicDescriptor<K, V> topicDescriptor,
            final CreekContext ctx,
            final TopologyTestDriver testDriver) {
        final KafkaTopic<K, V> topic =
                ctx.extension(KafkaClientsExtension.class).topic(topicDescriptor);
        return testDriver.createInputTopic(
                topicDescriptor.name(),
                topic.keySerde().serializer(),
                topic.valueSerde().serializer());
    }

    /**
     * Create a test input topic
     *
     * @param topicDescriptor the topic descriptor
     * @param ctx the creek context
     * @param testDriver the Streams topology test driver
     * @param startTimestamp Start timestamp for auto-generated record time
     * @param autoAdvance autoAdvance duration for auto-generated record time
     * @param <K> the topic key type
     * @param <V> the topic value type
     * @return the test input topic
     * @see TopologyTestDriver#createInputTopic(String,
     *     org.apache.kafka.common.serialization.Serializer,
     *     org.apache.kafka.common.serialization.Serializer, Instant, Duration)
     */
    public static <K, V> TestInputTopic<K, V> inputTopic(
            final KafkaTopicDescriptor<K, V> topicDescriptor,
            final CreekContext ctx,
            final TopologyTestDriver testDriver,
            final Instant startTimestamp,
            final Duration autoAdvance) {
        final KafkaTopic<K, V> topic =
                ctx.extension(KafkaClientsExtension.class).topic(topicDescriptor);
        return testDriver.createInputTopic(
                topicDescriptor.name(),
                topic.keySerde().serializer(),
                topic.valueSerde().serializer(),
                startTimestamp,
                autoAdvance);
    }

    /**
     * Create a test output topic
     *
     * @param topicDescriptor the topic descriptor
     * @param ctx the creek context
     * @param testDriver the Streams topology test driver
     * @param <K> the topic key type
     * @param <V> the topic value type
     * @return the test output topic
     * @see TopologyTestDriver#createOutputTopic(String,
     *     org.apache.kafka.common.serialization.Deserializer,
     *     org.apache.kafka.common.serialization.Deserializer)
     */
    public static <K, V> TestOutputTopic<K, V> outputTopic(
            final KafkaTopicDescriptor<K, V> topicDescriptor,
            final CreekContext ctx,
            final TopologyTestDriver testDriver) {
        final KafkaTopic<K, V> topic =
                ctx.extension(KafkaClientsExtension.class).topic(topicDescriptor);
        return testDriver.createOutputTopic(
                topicDescriptor.name(),
                topic.keySerde().deserializer(),
                topic.valueSerde().deserializer());
    }
}
