/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.api.kafka.extension.resource.KafkaTopicInfo;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider.SystemTestSerde;

/**
 * A system-test-specific Kafka topic that works with untyped data.
 *
 * <p>Delegates serialization/deserialization to the appropriate {@link SystemTestSerde} for each
 * part (key/value), which may use different serialization formats.
 *
 * <p>Created once at startup per topic used in tests.
 */
final class TestKafkaTopic implements KafkaTopicInfo {

    private final KafkaTopicDescriptor<?, ?> descriptor;
    private final SystemTestSerde keySerde;
    private final SystemTestSerde valueSerde;

    TestKafkaTopic(
            final KafkaTopicDescriptor<?, ?> descriptor,
            final SystemTestSerde keySerde,
            final SystemTestSerde valueSerde) {
        this.descriptor = requireNonNull(descriptor, "descriptor");
        this.keySerde = requireNonNull(keySerde, "keySerde");
        this.valueSerde = requireNonNull(valueSerde, "valueSerde");
    }

    /**
     * @return the topic descriptor
     */
    public KafkaTopicDescriptor<?, ?> descriptor() {
        return descriptor;
    }

    /** Serialize an untyped key to bytes. */
    byte[] serializeKey(final Object key) {
        return keySerde.serialize(key, descriptor.key(), descriptor.name());
    }

    /** Serialize an untyped value to bytes. */
    byte[] serializeValue(final Object value) {
        return valueSerde.serialize(value, descriptor.value(), descriptor.name());
    }

    /** Deserialize key bytes to untyped canonical form. */
    Object deserializeKey(final byte[] key) {
        return keySerde.deserialize(key, descriptor.key(), descriptor.name());
    }

    /** Deserialize value bytes to untyped canonical form. */
    Object deserializeValue(final byte[] value) {
        return valueSerde.deserialize(value, descriptor.value(), descriptor.name());
    }

    /** Normalise a key parsed from YAML to match the type returned by {@link #deserializeKey}. */
    Object normaliseKey(final Object key) {
        return keySerde.normalise(key, descriptor.key(), descriptor.name());
    }

    /**
     * Normalise a value parsed from YAML to match the type returned by {@link #deserializeValue}.
     */
    Object normaliseValue(final Object value) {
        return valueSerde.normalise(value, descriptor.value(), descriptor.name());
    }
}
