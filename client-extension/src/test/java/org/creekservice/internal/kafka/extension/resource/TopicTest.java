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

package org.creekservice.internal.kafka.extension.resource;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicTest {

    @Mock private KafkaTopicDescriptor<Long, String> descriptor;
    @Mock private Serde<Long> keySerde;
    @Mock private Serde<String> valueSerde;
    private final Map<String, Object> clientProperties =
            Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:8080");
    private Topic<Long, String> topic;

    @BeforeEach
    void setUp() {
        topic = new Topic<>(descriptor, keySerde, valueSerde, clientProperties);
    }

    @Test
    void shouldReturnName() {
        // Given:
        when(descriptor.name()).thenReturn("bob");

        // Then:
        assertThat(topic.name(), is("bob"));
    }

    @Test
    void shouldReturnKeySerde() {
        assertThat(topic.keySerde(), is(keySerde));
    }

    @Test
    void shouldReturnValueSerde() {
        assertThat(topic.valueSerde(), is(valueSerde));
    }

    @Test
    void shouldReturnDescriptor() {
        assertThat(topic.descriptor(), is(descriptor));
    }

    @Test
    void shouldReturnConfiguredKafkaProducer() {
        // Given:
        when(keySerde.serializer()).thenReturn(new LongSerializer());
        when(valueSerde.serializer()).thenReturn(new StringSerializer());

        // When:
        final Producer<Long, String> producer = topic.producer();

        // Then:
        assertThat(producer, is(instanceOf(KafkaProducer.class)));
    }

    @Test
    void shouldReturnConfiguredKafkaConsumer() {
        // Given:
        when(keySerde.deserializer()).thenReturn(new LongDeserializer());
        when(valueSerde.deserializer()).thenReturn(new StringDeserializer());

        // When:
        final Consumer<Long, String> consumer = topic.consumer();

        // Then:
        assertThat(consumer, is(instanceOf(KafkaConsumer.class)));
    }
}
