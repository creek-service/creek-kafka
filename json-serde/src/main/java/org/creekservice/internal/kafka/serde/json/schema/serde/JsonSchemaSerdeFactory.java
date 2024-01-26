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

package org.creekservice.internal.kafka.serde.json.schema.serde;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.creekservice.internal.kafka.serde.json.mapper.BaseJsonMapper;
import org.creekservice.internal.kafka.serde.json.mapper.GenericMapper;
import org.creekservice.internal.kafka.serde.json.schema.store.RegisteredSchema;
import org.creekservice.internal.kafka.serde.json.schema.validation.SchemaFriendValidator;
import org.creekservice.internal.kafka.serde.json.schema.validation.SchemaValidator;

public final class JsonSchemaSerdeFactory {

    private final JsonMapper jsonMapper;

    /**
     * @param subtypes subtypes explicitly registered by users via options.
     */
    public JsonSchemaSerdeFactory(final Map<Class<?>, String> subtypes) {
        this.jsonMapper = BaseJsonMapper.get();

        subtypes.entrySet().stream()
                .map(e -> new NamedType(e.getKey(), e.getValue()))
                .forEach(jsonMapper::registerSubtypes);
    }

    public <T> Serde<T> create(final RegisteredSchema<T> schema) {
        final SchemaValidator producerValidator = new SchemaFriendValidator(schema.schema());
        final SchemaValidator consumerValidator =
                new SchemaFriendValidator(schema.schema().toConsumerSchema());
        final GenericMapper<T> mapper = new GenericMapper<>(schema.type(), jsonMapper);
        return Serdes.serdeFrom(
                new JsonSchemaSerializer<>(producerValidator, mapper),
                new JsonSchemaDeserializer<>(consumerValidator, mapper));
    }
}
