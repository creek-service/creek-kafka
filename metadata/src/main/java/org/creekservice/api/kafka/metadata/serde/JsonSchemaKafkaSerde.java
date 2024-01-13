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

package org.creekservice.api.kafka.metadata.serde;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;

import org.creekservice.api.kafka.metadata.SerializationFormat;

/** Metadata about the JSON-schema Kafka Serde */
public final class JsonSchemaKafkaSerde {

    private static final SerializationFormat FORMAT = serializationFormat("json-schema");

    private JsonSchemaKafkaSerde() {}

    /**
     * @return the JSON Schema Kafka serialization format.
     */
    public static SerializationFormat format() {
        return FORMAT;
    }
}
