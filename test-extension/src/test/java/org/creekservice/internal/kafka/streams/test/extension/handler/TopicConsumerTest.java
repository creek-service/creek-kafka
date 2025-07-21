/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicConsumerTest {

    private static final Instant START = Instant.now();
    private static final byte[] BINARY_KEY = "key".getBytes(UTF_8);
    private static final byte[] BINARY_VALUE = "value".getBytes(UTF_8);

    @Mock private KafkaTopic<?, ?> topic;
    @Mock private Consumer<byte[], byte[]> kafkaConsumer;
    @Mock private Clock clock;
    @Mock private TopicPartition tp0;
    @Mock private TopicPartition tp1;

    private ConsumerRecord<byte[], byte[]> cr =
            new ConsumerRecord<>("t", 1, 2, BINARY_KEY, BINARY_VALUE);
    private TopicConsumer topicConsumer;

    @BeforeEach
    void setUp() {
        topicConsumer = new TopicConsumer(topic, kafkaConsumer, clock);

        when(clock.instant()).thenReturn(START);
        when(topic.deserializeKey(any())).thenAnswer(inv -> bytesToString(inv.getArgument(0)));
        when(topic.deserializeValue(any())).thenAnswer(inv -> bytesToString(inv.getArgument(0)));

        when(kafkaConsumer.poll(Duration.ofSeconds(1)))
                .thenReturn(new ConsumerRecords<>(Map.of(tp0, List.of(cr))));
    }

    @Test
    void shouldAssignAndSeek() {
        // Given:
        final Map<TopicPartition, Long> startOffsets =
                Map.of(
                        tp0, 1L,
                        tp1, 9865L);

        // When:
        topicConsumer.assignAndSeek(startOffsets);

        // Then:
        verify(kafkaConsumer).assign(Set.of(tp0, tp1));
        verify(kafkaConsumer).seek(tp0, 1L);
        verify(kafkaConsumer).seek(tp1, 9865L);
    }

    @Test
    void shouldThrowIfAssignThrows() {
        // Given:
        final KafkaException cause = new KafkaException("boom");
        doThrow(cause).when(kafkaConsumer).assign(any());

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> topicConsumer.assignAndSeek(Map.of(tp0, 1L)));

        // Then:
        assertThat(e.getMessage(), is("Failed to assign topic partitions"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowIfSeekThrows() {
        // Given:
        final KafkaException cause = new KafkaException("boom");
        doThrow(cause).when(kafkaConsumer).seek(any(), anyLong());

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> topicConsumer.assignAndSeek(Map.of(tp0, 1L)));

        // Then:
        assertThat(e.getMessage(), is("Failed to seek topic partition to starting offset"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldDeserialize() {
        // When:
        final List<ConsumedRecord> result = topicConsumer.consume(1, START.plusSeconds(10));

        // Then:
        assertThat(result, hasSize(greaterThan(0)));
        assertThat(result.get(0).key(), is(Optional.of("key")));
        assertThat(result.get(0).value(), is(Optional.of("value")));
    }

    @Test
    void shouldDeserializeNulls() {
        // Given:
        cr = new ConsumerRecord<>("t", 0, 0, null, null);
        when(kafkaConsumer.poll(Duration.ofSeconds(1)))
                .thenReturn(new ConsumerRecords<>(Map.of(tp0, List.of(cr))));

        // When:
        final List<ConsumedRecord> result = topicConsumer.consume(1, START.plusSeconds(10));

        // Then:
        assertThat(result, hasSize(greaterThan(0)));
        assertThat(result.get(0).key(), is(Optional.empty()));
        assertThat(result.get(0).value(), is(Optional.empty()));
    }

    @Test
    void shouldPollUntilMinRecordsConsumed() {
        // When:
        final List<ConsumedRecord> result = topicConsumer.consume(3, START.plusSeconds(10));

        // Then:
        assertThat(result, hasSize(3));
    }

    @Test
    void shouldPollUntilTimeOutExceeded() {
        // Given:
        when(clock.instant())
                .thenReturn(START, START.plusSeconds(10).minusNanos(1), START.plusSeconds(10));

        // When:
        final List<ConsumedRecord> result = topicConsumer.consume(3, START.plusSeconds(10));

        // Then:
        assertThat(result, hasSize(2));
    }

    @SuppressFBWarnings()
    @Test
    void shouldThrowIfPollThrows() {
        // Given:
        final KafkaException cause = new KafkaException("boom");
        doThrow(cause).when(kafkaConsumer).poll(any(Duration.class));

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> topicConsumer.consume(1L, START.plusMillis(1)));

        // Then:
        assertThat(e.getMessage(), is("Failed to consume records from Kafka"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowIfKeyDeserializationThrows() {
        // Given:
        final KafkaException cause = new KafkaException("boom");
        doThrow(cause).when(topic).deserializeKey(any());

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> topicConsumer.consume(1L, START.plusMillis(1)));

        // Then:
        assertThat(
                e.getMessage(),
                is("Failed to deserialize record key. topic: t, partition: 1, offset: 2"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowIfValueDeserializationThrows() {
        // Given:
        final KafkaException cause = new KafkaException("boom");
        doThrow(cause).when(topic).deserializeValue(any());

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> topicConsumer.consume(1L, START.plusMillis(1)));

        // Then:
        assertThat(
                e.getMessage(),
                is("Failed to deserialize record value. topic: t, partition: 1, offset: 2"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldNotPollIfTimeOutExceeded() {
        // When:
        topicConsumer.consume(3, START);

        // Then:
        verify(kafkaConsumer, never()).poll(any(Duration.class));
    }

    private static String bytesToString(final byte[] bytes) {
        return bytes == null ? null : new String(bytes, UTF_8);
    }
}
