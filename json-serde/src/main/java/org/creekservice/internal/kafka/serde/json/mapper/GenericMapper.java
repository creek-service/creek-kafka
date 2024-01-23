/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.mapper;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.Map;

public final class GenericMapper<T> implements JsonReader<T>, JsonWriter<T> {

    private final Class<T> type;
    private final JsonMapper mapper;

    public GenericMapper(final Class<T> type, final JsonMapper mapper) {
        this.type = requireNonNull(type, "type");
        this.mapper = requireNonNull(mapper, "mapper");
    }

    @Override
    public Map<String, ?> convertToMap(final T model) {
        try {
            return mapper.convertValue(model, new TypeReference<>() {});
        } catch (final Exception e) {
            throw new JsonSerializationException(type, e);
        }
    }

    @Override
    public byte[] writeAsBytes(final Map<String, ?> properties) {
        try {
            return mapper.writeValueAsBytes(properties);
        } catch (final Exception e) {
            throw new JsonSerializationException(type, e);
        }
    }

    @Override
    public Map<String, Object> readValue(final byte[] data) {
        try {
            return mapper.readValue(data, new TypeReference<>() {});
        } catch (final Exception e) {
            throw new JsonDeserializationException(type, e);
        }
    }

    @Override
    public T convertFromMap(final Map<String, ?> properties) {
        try {
            return mapper.convertValue(properties, type);
        } catch (final Exception e) {
            throw new JsonDeserializationException(type, e);
        }
    }

    static class JsonSerializationException extends RuntimeException {
        JsonSerializationException(final Class<?> type, final Exception cause) {
            super("Error serializing type: " + type.getName(), cause);
        }
    }

    static class JsonDeserializationException extends RuntimeException {
        JsonDeserializationException(final Class<?> type, final Exception cause) {
            super("Error deserializing type: " + type.getName(), cause);
        }
    }
}
