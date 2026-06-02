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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RecordNormaliserTest {

    private static final URI LOCATION = URI.create("record:///location");
    private static final String CLUSTER = "cluster-a";
    private static final String TOPIC = "topic-b";

    @Mock private TestKafkaTopic testTopic;
    @Mock private KafkaTopicDescriptor<?, ?> topicDescriptor;
    @Mock private KafkaTopicDescriptor.PartDescriptor<?> keyPart;
    @Mock private KafkaTopicDescriptor.PartDescriptor<?> valuePart;

    private RecordNormaliser coercer;

    @BeforeEach
    void setUp() {
        coercer = new RecordNormaliser();

        when(testTopic.name()).thenReturn(TOPIC);
        when(testTopic.normaliseKey(any())).thenAnswer(inv -> inv.getArgument(0));
        when(testTopic.normaliseValue(any())).thenAnswer(inv -> inv.getArgument(0));

        doReturn(topicDescriptor).when(testTopic).descriptor();
        doReturn(keyPart).when(topicDescriptor).key();
        doReturn(valuePart).when(topicDescriptor).value();
        doReturn(Long.class).when(keyPart).type();
        doReturn(Long.class).when(valuePart).type();
    }

    @Test
    void shouldHandleKeyAndValueBeingOfCorrectType() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(LOCATION, CLUSTER, TOPIC, Optional3.of(99L), Optional3.of(10L));

        // When:
        final List<TopicRecord> result = coercer.normalise(List.of(topicRecord), testTopic);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).location(), is(LOCATION));
        assertThat(result.get(0).clusterName(), is(CLUSTER));
        assertThat(result.get(0).topicName(), is(TOPIC));
        assertThat(result.get(0).key(), is(Optional3.of(99L)));
        assertThat(result.get(0).value(), is(Optional3.of(10L)));
    }

    @Test
    void shouldNormaliseKey() {
        // Given:
        when(testTopic.normaliseKey(99)).thenReturn(99L);

        final TopicRecord topicRecord =
                new TopicRecord(LOCATION, CLUSTER, TOPIC, Optional3.of(99), Optional3.of(10L));

        // When:
        final List<TopicRecord> result = coercer.normalise(List.of(topicRecord), testTopic);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).key(), is(Optional3.of(99L)));
    }

    @Test
    void shouldNormaliseValue() {
        // Given:
        when(testTopic.normaliseValue(10)).thenReturn(10L);

        final TopicRecord topicRecord =
                new TopicRecord(LOCATION, CLUSTER, TOPIC, Optional3.of(99L), Optional3.of(10));

        // When:
        final List<TopicRecord> result = coercer.normalise(List.of(topicRecord), testTopic);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).value(), is(Optional3.of(10L)));
    }

    @Test
    void shouldHandleKeyAndValueNotProvided() {
        // Given:
        final TopicRecord topicRecord =
                new TopicRecord(
                        LOCATION, CLUSTER, TOPIC, Optional3.notProvided(), Optional3.notProvided());

        // When:
        final List<TopicRecord> result = coercer.normalise(List.of(topicRecord), testTopic);

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
        final List<TopicRecord> result = coercer.normalise(List.of(topicRecord), testTopic);

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0).key(), is(Optional3.explicitlyNull()));
        assertThat(result.get(0).value(), is(Optional3.explicitlyNull()));
    }

    @Test
    void shouldThrowIfKeyCanNotBeNormalised() {
        // Given:
        when(testTopic.normaliseKey("incompatible")).thenThrow(new RuntimeException("it no work"));

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
                        () -> coercer.normalise(List.of(topicRecord), testTopic));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "The record's key is not compatible with the topic's key type."
                                + " key: incompatible, key_type: java.lang.String,"
                                + " topic_key_type: java.lang.Long"));
        assertThat(e.getCause().getMessage(), is("it no work"));
    }

    @Test
    void shouldThrowIfValueCanNotBeNormalised() {
        // Given:
        when(testTopic.normaliseValue("incompatible"))
                .thenThrow(new RuntimeException("it no work"));

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
                        () -> coercer.normalise(List.of(topicRecord), testTopic));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "The record's value is not compatible with the topic's value type."
                                + " value: incompatible, value_type: java.lang.String,"
                                + " topic_value_type: java.lang.Long"));
        assertThat(e.getCause().getMessage(), is("it no work"));
    }
}
