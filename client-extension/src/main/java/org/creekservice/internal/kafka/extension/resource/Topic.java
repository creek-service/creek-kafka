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

import static java.util.Objects.requireNonNull;

import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;

public final class Topic<K, V> implements KafkaTopic<K, V> {

    private final KafkaTopicDescriptor<K, V> descriptor;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Map<String, Object> clientProperties;

    public Topic(
            final KafkaTopicDescriptor<K, V> descriptor,
            final Serde<K> keySerde,
            final Serde<V> valueSerde,
            final Map<String, Object> clientProperties) {
        this.descriptor = requireNonNull(descriptor, "descriptor");
        this.keySerde = requireNonNull(keySerde, "keySerde");
        this.valueSerde = requireNonNull(valueSerde, "valueSerde");
        this.clientProperties = Map.copyOf(requireNonNull(clientProperties, "clientProperties"));
    }

    @Override
    public String name() {
        return descriptor.name();
    }

    @Override
    public Serde<K> keySerde() {
        return keySerde;
    }

    @Override
    public Serde<V> valueSerde() {
        return valueSerde;
    }

    @Override
    public Producer<K, V> producer() {
        return new KafkaProducer<>(
                clientProperties, keySerde.serializer(), valueSerde.serializer());
    }

    @Override
    public Consumer<K, V> consumer() {
        return new KafkaConsumer<>(
                clientProperties, keySerde.deserializer(), valueSerde.deserializer());
    }

    public KafkaTopicDescriptor<K, V> descriptor() {
        return descriptor;
    }
}
