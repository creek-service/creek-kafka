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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicConsumersTest {

    private static final String TOPIC_A = "topic-a";
    private static final TopicPartition TP_A_1 = new TopicPartition(TOPIC_A, 1);
    private static final TopicPartition TP_A_0 = new TopicPartition(TOPIC_A, 0);
    private static final String TOPIC_B = "topic-b";
    private static final TopicPartition TP_B_0 = new TopicPartition(TOPIC_B, 0);

    @Mock private KafkaTopic<?, ?> topicA;
    @Mock private KafkaTopic<?, ?> topicB;
    @Mock private Consumer<byte[], byte[]> kafkaConsumer;
    @Mock private TopicConsumers.TopicConsumerFactory consumerFactory;

    private TopicConsumers consumers;

    @BeforeEach
    void setUp() {
        final PartitionInfo piA0 = pi(0, TOPIC_A);
        final PartitionInfo piA1 = pi(1, TOPIC_A);
        when(kafkaConsumer.partitionsFor(TOPIC_A)).thenReturn(List.of(piA0, piA1));
        final PartitionInfo piB0 = pi(0, TOPIC_B);
        when(kafkaConsumer.partitionsFor(TOPIC_B)).thenReturn(List.of(piB0));

        when(kafkaConsumer.endOffsets(any())).thenAnswer(TopicConsumersTest::endOffsets);

        consumers =
                new TopicConsumers(
                        topics(TOPIC_A, topicA, TOPIC_B, topicB), kafkaConsumer, consumerFactory);
    }

    @Test
    void shouldGetPartitionsByTopicName() {
        verify(kafkaConsumer).partitionsFor(TOPIC_A);
        verify(kafkaConsumer).partitionsFor(TOPIC_B);
    }

    @Test
    void shouldThrowOnUnknownTopic() {
        // Given:
        when(kafkaConsumer.partitionsFor(any())).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                new TopicConsumers(
                                        topics(TOPIC_A, topicA), kafkaConsumer, consumerFactory));

        // Then:
        assertThat(e.getMessage(), is("Unknown topic: " + TOPIC_A));
    }

    @Test
    void shouldGetEndOffsetsForAllTopics() {
        verify(kafkaConsumer).endOffsets(List.of(TP_A_0, TP_A_1, TP_B_0));
    }

    @Test
    void shouldGetTopicConsumerAndSeek() {
        // Given:
        final TopicConsumer expected = mock(TopicConsumer.class);
        when(consumerFactory.create(any(), any())).thenReturn(expected);

        // When:
        final TopicConsumer topicConsumer = consumers.get(TOPIC_A);

        // Then:
        assertThat(topicConsumer, is(expected));
        verify(consumerFactory).create(topicA, kafkaConsumer);
        verify(topicConsumer).assignAndSeek(Map.of(TP_A_0, 10L, TP_A_1, 11L));
    }

    private static Map<TopicPartition, Long> endOffsets(final InvocationOnMock inv) {
        final Collection<TopicPartition> partitions = inv.getArgument(0);
        return partitions.stream()
                .collect(
                        Collectors.toUnmodifiableMap(
                                Function.identity(), TopicConsumersTest::endOffset));
    }

    private static long endOffset(final TopicPartition partition) {
        return (partition.topic().equals(TOPIC_A) ? 1 : 2) * (partition.partition() + 10L);
    }

    private static PartitionInfo pi(final Integer partition, final String topic) {
        final PartitionInfo mock = mock(PartitionInfo.class);
        when(mock.topic()).thenReturn(topic);
        when(mock.partition()).thenReturn(partition);
        return mock;
    }

    private static Map<String, KafkaTopic<?, ?>> topics(final Object... elements) {
        assertThat("length is odd", elements.length & 1, is(0));

        // For consistent ordering in the test:
        final LinkedHashMap<String, KafkaTopic<?, ?>> topics = new LinkedHashMap<>();
        for (int i = 0; i < elements.length; i += 2) {
            topics.put((String) elements[i], (KafkaTopic<?, ?>) elements[i + 1]);
        }
        return topics;
    }
}
