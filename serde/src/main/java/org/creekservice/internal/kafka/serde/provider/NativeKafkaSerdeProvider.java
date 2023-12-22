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

package org.creekservice.internal.kafka.serde.provider;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;

/** Serde provider for Kafka's own in-built serde classes. */
public final class NativeKafkaSerdeProvider implements KafkaSerdeProvider {

    /** The serialization format name for the native Kafka format. */
    public static final SerializationFormat FORMAT =
            SerializationFormat.serializationFormat("kafka");

    private static final Map<Class<?>, Supplier<Serde<?>>> SUPPLIERS =
            Map.ofEntries(
                    Map.entry(UUID.class, Serdes::UUID),
                    Map.entry(long.class, Serdes::Long),
                    Map.entry(Long.class, Serdes::Long),
                    Map.entry(int.class, Serdes::Integer),
                    Map.entry(Integer.class, Serdes::Integer),
                    Map.entry(short.class, Serdes::Short),
                    Map.entry(Short.class, Serdes::Short),
                    Map.entry(float.class, Serdes::Float),
                    Map.entry(Float.class, Serdes::Float),
                    Map.entry(double.class, Serdes::Double),
                    Map.entry(Double.class, Serdes::Double),
                    Map.entry(String.class, Serdes::String),
                    Map.entry(ByteBuffer.class, Serdes::ByteBuffer),
                    Map.entry(Bytes.class, Serdes::Bytes),
                    Map.entry(byte[].class, Serdes::ByteArray),
                    Map.entry(Void.class, Serdes::Void));

    @Override
    public SerializationFormat format() {
        return FORMAT;
    }

    @Override
    public SerdeProvider initialize(final String clusterName, final InitializeParams params) {
        return new SerdeProvider() {

            @SuppressWarnings("unchecked")
            @Override
            public <T> Serde<T> createSerde(final KafkaTopicDescriptor.PartDescriptor<T> part) {
                final Supplier<Serde<?>> supplier = SUPPLIERS.get(part.type());
                if (supplier == null) {
                    throw new UnsupportedTypeException(part.type());
                }

                return (Serde<T>) supplier.get();
            }
        };
    }

    /**
     * Check if this provider supports the supplied {@code type}.
     *
     * @param type the type to check
     * @return {@code true} if supported, {@code false} otherwise.
     */
    public static boolean supports(final Class<?> type) {
        return SUPPLIERS.containsKey(type);
    }

    private static final class UnsupportedTypeException extends IllegalArgumentException {
        <T> UnsupportedTypeException(final Class<T> type) {
            super("The supplied type is not supported by the kafka format: " + type.getName());
        }
    }
}
