/*
 * Copyright 2024 Creek Contributors (https://github.com/creek-service)
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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import java.util.Map;
import org.creekservice.api.base.annotation.VisibleForTesting;

public final class GenericMapperFactory {

    private final JsonMapper mapper;

    public GenericMapperFactory(final Map<Class<?>, String> subTypes) {
        this(subTypes, BaseJsonMapper.get());
    }

    @VisibleForTesting
    GenericMapperFactory(final Map<Class<?>, String> subTypes, final JsonMapper jsonMapper) {
        this.mapper = requireNonNull(jsonMapper, "jsonMapper");

        subTypes.entrySet().stream()
                .map(e -> new NamedType(e.getKey(), e.getValue()))
                .forEach(jsonMapper::registerSubtypes);
    }

    public <T> GenericMapper<T> create(final Class<T> type) {
        return new GenericMapper<>(type, mapper);
    }
}
