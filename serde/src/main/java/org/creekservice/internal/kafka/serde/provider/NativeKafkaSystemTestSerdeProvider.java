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

package org.creekservice.internal.kafka.serde.provider;

import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.serde.NativeKafkaSerde;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider;

/** System test serde provider for Kafka's own in-built serde classes. */
public final class NativeKafkaSystemTestSerdeProvider implements KafkaSystemTestSerdeProvider {

    @Override
    public SerializationFormat format() {
        return NativeKafkaSerde.format();
    }

    @Override
    public SystemTestSerde create() {
        return new NativeSystemTestSerde();
    }

    @SuppressWarnings("resource")
    private static final class NativeSystemTestSerde implements SystemTestSerde {
        private final TypeNormaliser normaliser = new TypeNormaliser();

        @Override
        public byte[] serialize(
                final Object data, final PartDescriptor<?> part, final String topicName) {
            final Object normalised = normaliser.normalise(data, part.type());
            return serializeWith(part, normalised, topicName);
        }

        @Override
        public Object deserialize(
                final byte[] data, final PartDescriptor<?> part, final String topicName) {
            final Serde<?> serde = NativeKafkaSerdeProvider.serde(part.type());
            return serde.deserializer().deserialize(topicName, data);
        }

        @Override
        public Object normalise(
                final Object data, final PartDescriptor<?> part, final String topicName) {
            return normaliser.normalise(data, part.type());
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private static byte[] serializeWith(
                final PartDescriptor<?> part, final Object data, final String topicName) {
            final Serde serde = NativeKafkaSerdeProvider.serde(part.type());
            return serde.serializer().serialize(topicName, data);
        }
    }
}
