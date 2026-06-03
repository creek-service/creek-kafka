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

package org.creekservice.api.kafka.serde.provider;

import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;

/**
 * Provides serialization/deserialization for system tests.
 *
 * <p>Unlike {@link KafkaSerdeProvider}, which works with strongly-typed domain objects, this
 * provider works with untyped data (primitives, maps, lists) as parsed from YAML test files. This
 * avoids constructing domain objects during testing.
 *
 * <p>Loaded via {@link java.util.ServiceLoader}. Register in module-info.java or META-INF/services.
 */
public interface KafkaSystemTestSerdeProvider {

    /**
     * @return the serialization format this provider handles. Must match the format returned by the
     *     corresponding {@link KafkaSerdeProvider}.
     */
    SerializationFormat format();

    /**
     * Create the system test serde factory.
     *
     * @return the system test serde factory.
     */
    SystemTestSerde create();

    /**
     * Factory for system-test-specific serialization of untyped data.
     *
     * <p>All data is in "canonical" form: null, primitives (Integer, Long, Double, Float, Short,
     * String, Boolean, BigDecimal, UUID, byte[]), Map&lt;String, Object&gt;, or List&lt;Object&gt;.
     *
     * <p><b>Input path (producing to Kafka)</b>
     *
     * <p>YAML → Object.class (Integer/Map/etc) → testTopic.serializeKey(data) → bytes → Kafka
     *
     * <p><b>Expectation path (consuming from Kafka + comparing)</b>
     *
     * <p>Kafka bytes → testTopic.deserializeKey(bytes) → canonical form
     *
     * <p>Expected YAML → testTopic.normaliseKey(yamlData) → canonical form
     *
     * <p>Compare: Objects.equals(normalised expected, deserialized actual)
     */
    interface SystemTestSerde {

        /**
         * Serialize untyped YAML data to bytes.
         *
         * <p>The implementation should handle any necessary type normalisation internally. For
         * example, a native serde implementation might normalise Integer to Long before
         * serializing.
         *
         * @param data the untyped data (primitive, map, or list) from YAML.
         * @param part the topic part descriptor (contains format, type, key/value info).
         * @param topicName the Kafka topic name (needed by some serializers).
         * @return the serialized bytes.
         */
        byte[] serialize(Object data, PartDescriptor<?> part, String topicName);

        /**
         * Deserialize bytes to untyped canonical form.
         *
         * <p>Must return primitives/maps/lists, never domain-specific types.
         *
         * <p>Must match the types returned by {@link #normalise} for the expected data.
         *
         * @param data the bytes from Kafka.
         * @param part the topic part descriptor.
         * @param topicName the Kafka topic name.
         * @return the deserialized data in canonical form, or null.
         */
        Object deserialize(byte[] data, PartDescriptor<?> part, String topicName);

        /**
         * Normalise YAML-parsed data to match the types that {@link #deserialize} returns.
         *
         * <p>This ensures {@code Objects.equals(normalise(yamlData), deserialize(bytes))} works
         * correctly. For example, YAML may parse {@code 1} as {@code Integer}, but a Long topic's
         * deserialize returns {@code Long}. Normalise handles this.
         *
         * @param data the untyped data from YAML.
         * @param part the topic part descriptor.
         * @param topicName the Kafka topic name.
         * @return the normalised data.
         */
        default Object normalise(Object data, PartDescriptor<?> part, final String topicName) {
            return deserialize(serialize(data, part, topicName), part, topicName);
        }
    }
}
