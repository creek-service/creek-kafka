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

package org.creekservice.api.kafka.serde.provider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider.SystemTestSerde;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaSystemTestSerdeProviderTest {

    private static final String TOPIC = "test-topic";

    @Mock private PartDescriptor<Long> part;

    @BeforeEach
    void setUp() {
        when(part.type()).thenReturn(Long.class);
    }

    @Test
    void shouldDefaultNormaliseViaSerializeDeserializeRoundTrip() {
        // Given:
        final SystemTestSerde serde = new RoundTripSerde();

        // When:
        final Object result = serde.normalise(42, part, TOPIC);

        // Then: the default normalise serializes then deserializes
        assertThat(result, is(42L));
    }

    /**
     * A minimal SystemTestSerde that converts Integer to Long during serialization, allowing the
     * default normalise implementation to be tested.
     */
    private static final class RoundTripSerde implements SystemTestSerde {

        @Override
        public byte[] serialize(
                final Object data, final PartDescriptor<?> part, final String topicName) {
            // Simulate normalisation: convert Integer to Long
            final long value = ((Number) data).longValue();
            return longToBytes(value);
        }

        @Override
        public Object deserialize(
                final byte[] data, final PartDescriptor<?> part, final String topicName) {
            return bytesToLong(data);
        }

        private static byte[] longToBytes(final long value) {
            return new byte[] {
                (byte) (value >> 56),
                (byte) (value >> 48),
                (byte) (value >> 40),
                (byte) (value >> 32),
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value
            };
        }

        private static long bytesToLong(final byte[] bytes) {
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
            return value;
        }
    }
}
