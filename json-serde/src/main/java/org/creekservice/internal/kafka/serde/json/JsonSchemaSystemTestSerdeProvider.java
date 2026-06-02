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

package org.creekservice.internal.kafka.serde.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.serde.JsonSchemaKafkaSerde;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider;

/** System test serde provider for the JSON schema serialization format. */
public final class JsonSchemaSystemTestSerdeProvider implements KafkaSystemTestSerdeProvider {

    @Override
    public SerializationFormat format() {
        return JsonSchemaKafkaSerde.format();
    }

    @Override
    public SystemTestSerde create() {
        return new JsonSystemTestSerde();
    }

    private static final class JsonSystemTestSerde implements SystemTestSerde {
        private final JsonMapper mapper =
                JsonMapper.builder()
                        .findAndAddModules()
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                        .enable(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION)
                        .build();

        @Override
        public byte[] serialize(
                final Object data, final PartDescriptor<?> part, final String topicName) {
            if (data == null) {
                return null;
            }
            try {
                return mapper.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new JsonSerializationException(
                        "Failed to serialize data as JSON: " + data, e);
            }
        }

        @Override
        public Object deserialize(
                final byte[] data, final PartDescriptor<?> part, final String topicName) {
            if (data == null) {
                return null;
            }
            try {
                return mapper.readValue(data, Object.class);
            } catch (final Exception e) {
                throw new JsonSerializationException("Failed to deserialize JSON bytes", e);
            }
        }
    }

    private static final class JsonSerializationException extends RuntimeException {
        JsonSerializationException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }
}
