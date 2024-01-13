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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.system.test.extension.test.model.InputHandler.InputOptions;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicInput;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.creekservice.internal.kafka.streams.test.extension.yaml.TypeCoercer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicInputHandlerTest {

    private static final byte[] SERIALIZED_KEY_A = "key-a".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERIALIZED_KEY_B = "key-b".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERIALIZED_VALUE_A = "value-a".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERIALIZED_VALUE_B = "value-b".getBytes(StandardCharsets.UTF_8);

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ClientsExtension clientsExt;

    @Mock private TypeCoercer coercer;
    @Mock private TopicInput input;
    @Mock private InputOptions options;
    @Mock private Producer<byte[], byte[]> producerA;
    @Mock private Producer<byte[], byte[]> producerB;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private KafkaTopic<Integer, String> topicA;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private KafkaTopic<Integer, String> topicB;

    @Mock private TopicValidator topicValidator;

    private TopicInputHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TopicInputHandler(clientsExt, coercer, topicValidator);

        final TopicRecord record0 =
                new TopicRecord(
                        URI.create("record0:///location"),
                        "cluster-a",
                        "topic-a",
                        Optional3.of(87),
                        Optional3.of(0));
        final TopicRecord record1 =
                new TopicRecord(
                        URI.create("record1:///location"),
                        "cluster-b",
                        "topic-b",
                        Optional3.of(123L),
                        Optional3.of("1"));

        when(input.records()).thenReturn(List.of(record0, record1));

        when(clientsExt.producer("cluster-a")).thenReturn(producerA);
        when(clientsExt.producer("cluster-b")).thenReturn(producerB);

        doReturn(topicA).when(clientsExt).topic(any(), eq("topic-a"));
        doReturn(topicB).when(clientsExt).topic(any(), eq("topic-b"));

        when(topicA.name()).thenReturn("topic-a");
        when(topicB.name()).thenReturn("topic-b");

        when(topicA.serializeKey(any())).thenReturn(SERIALIZED_KEY_A);
        when(topicB.serializeKey(any())).thenReturn(SERIALIZED_KEY_B);

        when(topicA.serializeValue(any())).thenReturn(SERIALIZED_VALUE_A);
        when(topicB.serializeValue(any())).thenReturn(SERIALIZED_VALUE_B);

        when(topicA.descriptor().key().type()).thenReturn(Integer.class);
        when(topicB.descriptor().key().type()).thenReturn(Integer.class);

        when(topicA.descriptor().value().type()).thenReturn(String.class);
        when(topicB.descriptor().value().type()).thenReturn(String.class);

        when(coercer.coerce(any(), any()))
                .thenAnswer(
                        inv -> {
                            final Class<?> arg1 = inv.getArgument(1);
                            if (arg1.equals(Integer.class)) {
                                return inv.<Number>getArgument(0).intValue() + 1;
                            }

                            return "coerced-" + inv.getArgument(0);
                        });
    }

    @Test
    void shouldDoNothingIfNoRecords() {
        // Given:
        when(input.records()).thenReturn(List.of());

        // When:
        handler.process(input, options);
        handler.flush();

        // Then:
        verifyNoInteractions(clientsExt);
    }

    @Test
    void shouldGetTopic() {
        // When:
        handler.process(input, options);

        // Then:
        verify(clientsExt).topic("cluster-a", "topic-a");
        verify(clientsExt).topic("cluster-b", "topic-b");
    }

    @Test
    void shouldGetProducer() {
        // When:
        handler.process(input, options);

        // Then:
        verify(clientsExt).producer("cluster-a");
        verify(clientsExt).producer("cluster-b");
    }

    @Test
    void shouldCoerceKeys() {
        // When:
        handler.process(input, options);

        // Then:
        verify(coercer).coerce(87, Integer.class);
        verify(coercer).coerce(123L, Integer.class);
    }

    @Test
    void shouldCoerceValues() {
        // When:
        handler.process(input, options);

        // Then:
        verify(coercer).coerce(0, String.class);
        verify(coercer).coerce("1", String.class);
    }

    @Test
    void shouldSerializeKeys() {
        // When:
        handler.process(input, options);

        // Then:
        verify(topicA).serializeKey(88);
        verify(topicB).serializeKey(124);
    }

    @Test
    void shouldSerializeValues() {
        // When:
        handler.process(input, options);

        // Then:
        verify(topicA).serializeValue("coerced-0");
        verify(topicB).serializeValue("coerced-1");
    }

    @Test
    void shouldSendByCluster() {
        // When:
        handler.process(input, options);

        // Then:
        verify(producerA)
                .send(new ProducerRecord<>("topic-a", SERIALIZED_KEY_A, SERIALIZED_VALUE_A));
        verify(producerB)
                .send(new ProducerRecord<>("topic-b", SERIALIZED_KEY_B, SERIALIZED_VALUE_B));
    }

    @Test
    void shouldShareByCluster() {
        // Given:
        final TopicRecord record0 =
                new TopicRecord(
                        URI.create("record0:///location"),
                        "cluster-a",
                        "topic-a",
                        Optional3.of(87),
                        Optional3.of(0));
        final TopicRecord record1 =
                new TopicRecord(
                        URI.create("record1:///location"),
                        "cluster-a",
                        "topic-b",
                        Optional3.of(123L),
                        Optional3.of("1"));
        when(input.records()).thenReturn(List.of(record0, record1));

        // When:
        handler.process(input, options);

        // Then:
        verify(producerA)
                .send(new ProducerRecord<>("topic-a", SERIALIZED_KEY_A, SERIALIZED_VALUE_A));
        verify(producerA)
                .send(new ProducerRecord<>("topic-b", SERIALIZED_KEY_B, SERIALIZED_VALUE_B));
    }

    @Test
    void shouldFlushByCluster() {
        // Given:
        handler.process(input, options);

        // When:
        handler.flush();

        // Then:
        verify(producerA).flush();
        verify(producerB).flush();
    }

    @Test
    void shouldFlushOncePerCluster() {
        // Given:
        final TopicRecord record0 =
                new TopicRecord(
                        URI.create("record0:///location"),
                        "cluster-a",
                        "topic-a",
                        Optional3.of(87),
                        Optional3.of(0));
        final TopicRecord record1 =
                new TopicRecord(
                        URI.create("record1:///location"),
                        "cluster-a",
                        "topic-b",
                        Optional3.of(123L),
                        Optional3.of("1"));
        when(input.records()).thenReturn(List.of(record0, record1));

        // Given:
        handler.process(input, options);

        // When:
        handler.flush();

        // Then:
        verify(producerA).flush();
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldFlushOnlyOnce() {
        // Given:
        handler.process(input, options);
        handler.flush();
        clearInvocations(producerA);

        // When:
        handler.flush();

        // Then:
        verifyNoInteractions(producerA);
    }

    @Test
    void shouldHandleRecordWithoutKey() {
        // Given:
        final TopicRecord record0 =
                new TopicRecord(
                        URI.create("record0:///location"),
                        "cluster-a",
                        "topic-a",
                        Optional3.notProvided(),
                        Optional3.of(0));
        final TopicRecord record1 =
                new TopicRecord(
                        URI.create("record0:///location"),
                        "cluster-b",
                        "topic-b",
                        Optional3.explicitlyNull(),
                        Optional3.of("1"));
        when(input.records()).thenReturn(List.of(record0, record1));

        // When:
        handler.process(input, options);

        // Then:
        verify(producerA).send(new ProducerRecord<>("topic-a", null, SERIALIZED_VALUE_A));
        verify(producerB).send(new ProducerRecord<>("topic-b", null, SERIALIZED_VALUE_B));
    }

    @Test
    void shouldHandleRecordWithoutValue() {
        // Given:
        final TopicRecord record0 =
                new TopicRecord(
                        URI.create("record0:///location"),
                        "cluster-a",
                        "topic-a",
                        Optional3.of(87),
                        Optional3.notProvided());
        final TopicRecord record1 =
                new TopicRecord(
                        URI.create("record1:///location"),
                        "cluster-b",
                        "topic-b",
                        Optional3.of(123L),
                        Optional3.explicitlyNull());
        when(input.records()).thenReturn(List.of(record0, record1));

        // When:
        handler.process(input, options);

        // Then:
        verify(producerA).send(new ProducerRecord<>("topic-a", SERIALIZED_KEY_A, null));
        verify(producerB).send(new ProducerRecord<>("topic-b", SERIALIZED_KEY_B, null));
    }

    @Test
    void shouldThrowOnUnknownTopic() {
        // Given:
        final RuntimeException cause = new RuntimeException("boom");
        when(clientsExt.topic(any(), any())).thenThrow(cause);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.process(input, options));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "The input record's cluster or topic is not known. cluster: cluster-a,"
                                + " topic: topic-a, location: record0:///location"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowIfKeyCoercionFails() {
        // Given:
        final RuntimeException cause = new RuntimeException("boom");
        doThrow(cause).when(coercer).coerce(eq(123L), any());

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.process(input, options));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "The record's key is not compatible with the topic's key type. key: 123,"
                            + " key_type: java.lang.Long, topic_key_type: java.lang.Integer, topic:"
                            + " topic-b, location: record1:///location"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowIfValueCoercionFails() {
        // Given:
        final RuntimeException cause = new RuntimeException("boom");
        doThrow(cause).when(coercer).coerce(any(), eq(String.class));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.process(input, options));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "The record's value is not compatible with the topic's value type. value:"
                            + " 0, value_type: java.lang.Integer, topic_value_type:"
                            + " java.lang.String, topic: topic-a, location: record0:///location"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowIfKeySerializationFails() {
        // Given:
        final RuntimeException cause = new RuntimeException("boom");
        doThrow(cause).when(topicB).serializeKey(any());

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.process(input, options));

        // Then:
        assertThat(
                e.getMessage(),
                is("Failed to serialize the record's key: 124, location: record1:///location"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowIfValueSerializationFails() {
        // Given:
        final RuntimeException cause = new RuntimeException("boom");
        doThrow(cause).when(topicB).serializeValue(any());

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.process(input, options));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to serialize the record's value: coerced-1, location:"
                                + " record1:///location"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldThrowIfFlushThrows() {
        // Given:
        handler.process(input, options);

        final RuntimeException cause = new RuntimeException("boom");
        doThrow(cause).when(producerA).flush();

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> handler.flush());

        // Then:
        assertThat(e, is(cause));
    }
}
