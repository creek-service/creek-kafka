/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.extension.resource;

import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;

/**
 * A Kafka topic resource.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KafkaTopic<K, V> {

    /**
     * @return the name of the topic
     */
    default String name() {
        return descriptor().name();
    }

    /**
     * @return the value type
     */
    KafkaTopicDescriptor<K, V> descriptor();

    /**
     * @return the serde used to (de)serialize the keys stored in the topic
     */
    Serde<K> keySerde();

    /**
     * @return the serde used to (de)serialize the values stored in the topic
     */
    Serde<V> valueSerde();

    /**
     * Convenience method for serializing a key instance.
     *
     * @param key the key to serialize.
     * @return the binary serialized key.
     */
    default byte[] serializeKey(final K key) {
        return keySerde().serializer().serialize(name(), key);
    }

    /**
     * Convenience method for serializing a value instance.
     *
     * @param value the value to serialize.
     * @return the binary serialized value.
     */
    default byte[] serializeValue(final V value) {
        return valueSerde().serializer().serialize(name(), value);
    }

    /**
     * Convenience method for deserializing a key instance.
     *
     * @param key the binary key to deserialize.
     * @return the deserialized key.
     */
    default K deserializeKey(final byte[] key) {
        return keySerde().deserializer().deserialize(name(), key);
    }

    /**
     * Convenience method for deserializing a value instance.
     *
     * @param value the binary value to deserialize.
     * @return the deserialized value.
     */
    default V deserializeValue(final byte[] value) {
        return valueSerde().deserializer().deserialize(name(), value);
    }
}
