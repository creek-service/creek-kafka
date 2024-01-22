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

package org.creekservice.internal.kafka.serde.json.schema.serde;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.internal.kafka.serde.json.mapper.JsonWriter;
import org.creekservice.internal.kafka.serde.json.schema.validation.SchemaValidator;

public final class JsonSchemaSerializer<T> implements Serializer<T> {

    private final JsonWriter<T> mapper;
    private final SchemaValidator validator;
    private Part part;

    public JsonSchemaSerializer(final SchemaValidator validator, final JsonWriter<T> mapper) {
        this.mapper = requireNonNull(mapper, "mapper");
        this.validator = requireNonNull(validator, "validator");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        this.part = isKey ? Part.key : Part.value;
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        final Map<String, ?> properties = mapper.convertToMap(data);
        validator.validate(properties, topic, part);
        return mapper.writeAsBytes(properties);
    }

    @Override
    public byte[] serialize(final String topic, final Headers headers, final T data) {
        return serialize(topic, data);
    }
}
