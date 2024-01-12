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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.net.URI;
import java.util.List;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.creekservice.internal.kafka.streams.test.extension.util.TopicConfigBuilder;
import org.creekservice.internal.kafka.streams.test.extension.util.TopicDescriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RecordCoercerTest {

    private static final URI LOCATION = URI.create("record:///location");
    private static final String CLUSTER = "cluster-a";
    private static final String TOPIC = "topic-b";
    private static final KafkaTopicDescriptor<?, ?> DESCRIPTOR =
            TopicDescriptors.inputTopic(
                    CLUSTER,
                    TOPIC,
                    Long.class,
                    BigDecimal.class,
                    TopicConfigBuilder.withPartitions(1));

    private RecordCoercer coercer;

    @BeforeEach
    void setUp() {
        coercer = new RecordCoercer();
    }

    @Test
    void shouldHandleKeyAndValueBeingOfCorrectType() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(
                        LOCATION, CLUSTER, TOPIC, Optional3.of(99L), Optional3.of(BigDecimal.TEN));

        // When:
        final List<TopicRecord> result = coercer.coerce(List.of(topicRecord), DESCRIPTOR);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).location(), is(LOCATION));
        assertThat(result.get(0).clusterName(), is(CLUSTER));
        assertThat(result.get(0).topicName(), is(TOPIC));
        assertThat(result.get(0).key(), is(Optional3.of(99L)));
        assertThat(result.get(0).value(), is(Optional3.of(BigDecimal.TEN)));
    }

    @Test
    void shouldCoerceKey() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(
                        LOCATION, CLUSTER, TOPIC, Optional3.of(99), Optional3.of(BigDecimal.TEN));

        // When:
        final List<TopicRecord> result = coercer.coerce(List.of(topicRecord), DESCRIPTOR);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).key(), is(Optional3.of(99L)));
    }

    @Test
    void shouldCoerceValue() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(LOCATION, CLUSTER, TOPIC, Optional3.of(99L), Optional3.of(10));

        // When:
        final List<TopicRecord> result = coercer.coerce(List.of(topicRecord), DESCRIPTOR);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).value(), is(Optional3.of(BigDecimal.TEN)));
    }

    @Test
    void shouldHandleKeyAndValueNotProvided() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(
                        LOCATION, CLUSTER, TOPIC, Optional3.notProvided(), Optional3.notProvided());

        // When:
        final List<TopicRecord> result = coercer.coerce(List.of(topicRecord), DESCRIPTOR);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).key(), is(Optional3.notProvided()));
        assertThat(result.get(0).value(), is(Optional3.notProvided()));
    }

    @Test
    void shouldHandleKeyAndValueExplicitlyNull() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(
                        LOCATION,
                        CLUSTER,
                        TOPIC,
                        Optional3.explicitlyNull(),
                        Optional3.explicitlyNull());

        // When:
        final List<TopicRecord> result = coercer.coerce(List.of(topicRecord), DESCRIPTOR);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).key(), is(Optional3.explicitlyNull()));
        assertThat(result.get(0).value(), is(Optional3.explicitlyNull()));
    }

    @Test
    void shouldThrowIfKeyCanNotBeCoerced() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(
                        LOCATION,
                        CLUSTER,
                        TOPIC,
                        Optional3.of("incompatible"),
                        Optional3.notProvided());

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> coercer.coerce(List.of(topicRecord), DESCRIPTOR));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to coerce expected record. "
                                + "topic: topic-b, location: record:///location"));
        assertThat(
                e.getCause().getMessage(),
                is(
                        "The record's key is not compatible with the topic's key type. key:"
                                + " incompatible, key_type: java.lang.String, topic_key_type:"
                                + " java.lang.Long"));
    }

    @Test
    void shouldThrowIfValueCanNotBeCoerced() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(
                        LOCATION,
                        CLUSTER,
                        TOPIC,
                        Optional3.notProvided(),
                        Optional3.of("incompatible"));

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> coercer.coerce(List.of(topicRecord), DESCRIPTOR));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to coerce expected record. "
                                + "topic: topic-b, location: record:///location"));
        assertThat(
                e.getCause().getMessage(),
                is(
                        "The record's value is not compatible with the topic's value type. value:"
                                + " incompatible, value_type: java.lang.String, topic_value_type:"
                                + " java.math.BigDecimal"));
    }
}
