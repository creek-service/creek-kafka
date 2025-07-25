/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.metadata;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

/**
 * Type-safe serialization format name.
 *
 * <p>Serialization formats are pluggable. Each format used requires a single implementation of
 * {@code KafkaSerdeProvider} on the class or module path.
 */
public final class SerializationFormat {

    private final String name;

    /**
     * Factory method
     *
     * @param name the format name.
     * @return the format.
     */
    public static SerializationFormat serializationFormat(final String name) {
        return new SerializationFormat(name);
    }

    private SerializationFormat(final String name) {
        this.name = requireNonNull(name, "name");

        if (name.isBlank()) {
            throw new IllegalArgumentException("name can not be blank");
        }

        if (!Objects.equals(name, name.trim())) {
            throw new IllegalArgumentException("name should be trimmed: '" + name + "'");
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SerializationFormat that = (SerializationFormat) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
