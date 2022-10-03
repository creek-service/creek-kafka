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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.system.test.extension.test.model.InputHandler;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicInput;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.creekservice.internal.kafka.streams.test.extension.yaml.TypeCoercer;

public final class TopicInputHandler implements InputHandler<TopicInput> {

    private final ClientsExtension clientsExt;
    private final TypeCoercer coercer;
    private final Set<Producer<byte[], byte[]>> toFlush = new HashSet<>();

    public TopicInputHandler(final ClientsExtension clientsExt) {
        this(clientsExt, new TypeCoercer());
    }

    @VisibleForTesting
    TopicInputHandler(final ClientsExtension clientsExt, final TypeCoercer typeCoercer) {
        this.clientsExt = requireNonNull(clientsExt, "clientsExt");
        this.coercer = requireNonNull(typeCoercer, "typeCoercer");
    }

    @Override
    public void process(final TopicInput input) {
        try {
            input.records().forEach(this::process);
        } catch (final Exception e) {
            throw new TopicInputException(
                    "Failed to process topic input. " + "location: " + input.location(), e);
        }
    }

    @Override
    public void flush() {
        toFlush.forEach(Producer::flush);
        toFlush.clear();
    }

    private void process(final TopicRecord record) {
        try {
            send(record, clientsExt.topic(record.clusterName(), record.topicName()));
        } catch (final Exception e) {
            throw new TopicInputException(
                    "Failed to send Kafka record. " + "location: " + record.location(), e);
        }
    }

    private <K, V> void send(final TopicRecord record, final KafkaTopic<K, V> topic) {
        final byte[] key =
                record.key()
                        .map(k -> coerceKey(k, topic))
                        .map(k -> serializeKey(k, topic))
                        .orElse(null, null);

        final byte[] value =
                record.value()
                        .map(v -> coerceValue(v, topic))
                        .map(v -> serializeValue(v, topic))
                        .orElse(null, null);

        final Producer<byte[], byte[]> producer = clientsExt.producer(record.clusterName());

        producer.send(new ProducerRecord<>(topic.name(), key, value));

        toFlush.add(producer);
    }

    private <K> K coerceKey(final Object key, final KafkaTopic<K, ?> topic) {
        try {
            return coercer.coerce(key, topic.descriptor().key().type());
        } catch (final Exception e) {
            throw new TopicInputException(
                    "The record's key is not compatible with the topic's key type."
                            + " key: "
                            + key
                            + ", topic_key_type: "
                            + topic.descriptor().key().type().getName()
                            + ", topic: "
                            + topic.name(),
                    e);
        }
    }

    private <V> V coerceValue(final Object value, final KafkaTopic<?, V> topic) {
        try {
            return coercer.coerce(value, topic.descriptor().value().type());
        } catch (final Exception e) {
            throw new TopicInputException(
                    "The record's value is not compatible with the topic's value type."
                            + " value: "
                            + value
                            + ", topic_value_type: "
                            + topic.descriptor().value().type().getName()
                            + ", topic: "
                            + topic.name(),
                    e);
        }
    }

    private <K> byte[] serializeKey(final K key, final KafkaTopic<K, ?> topic) {
        try {
            return topic.serializeKey(key);
        } catch (final Exception e) {
            throw new TopicInputException("Failed to serialize the record's key: " + key, e);
        }
    }

    private <V> byte[] serializeValue(final V value, final KafkaTopic<?, V> topic) {
        try {
            return topic.serializeValue(value);
        } catch (final Exception e) {
            throw new TopicInputException("Failed to serialize the record's value: " + value, e);
        }
    }

    private static final class TopicInputException extends RuntimeException {
        TopicInputException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }
}
