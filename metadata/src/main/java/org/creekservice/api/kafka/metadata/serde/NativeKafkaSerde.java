/*
 * Copyright 2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.metadata.serde;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;
import org.creekservice.api.kafka.metadata.SerializationFormat;

/** Utility class to access details on the types the Native Kafka serde. */
public final class NativeKafkaSerde {

    private static final SerializationFormat FORMAT =
            SerializationFormat.serializationFormat("kafka");

    private static final Set<Class<?>> SUPPORTED =
            Set.of(
                    UUID.class,
                    long.class,
                    Long.class,
                    int.class,
                    Integer.class,
                    short.class,
                    Short.class,
                    float.class,
                    Float.class,
                    double.class,
                    Double.class,
                    String.class,
                    ByteBuffer.class,
                    byte[].class,
                    Void.class);

    private NativeKafkaSerde() {}

    /**
     * @return the native Kafka serialization format.
     */
    public static SerializationFormat format() {
        return FORMAT;
    }

    /**
     * Check if this provider supports the supplied {@code type}.
     *
     * @param type the type to check
     * @return {@code true} if supported, {@code false} otherwise.
     */
    public static boolean supports(final Class<?> type) {
        if (SUPPORTED.contains(type)) {
            return true;
        }

        // Tested by name to avoid adding a Kafka client dependency:
        return "org.apache.kafka.common.utils.Bytes".equals(type.getName());
    }
}
