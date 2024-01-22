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

package org.creekservice.internal.kafka.serde.json.schema;

import io.confluent.kafka.schemaregistry.json.JsonSchema;

public final class SchemaConvertor {

    private SchemaConvertor() {}

    /**
     * Converts a producer-schema to a consumer-schema.
     *
     * <p>All objects in producer-schemas used a closed content model, i.e. {@code
     * additionalProperties} is set to {@code false}.
     *
     * @param producerSchema the producer schema to convert.
     * @return a matching consumer schema, i.e. one with an open content model.
     */
    public static JsonSchema toConsumerSchema(final JsonSchema producerSchema) {
        final String schemaText = producerSchema.canonicalString();
        return new JsonSchema(
                schemaText.replaceAll(
                        "\"additionalProperties\":\\s*false", "\"additionalProperties\": true"));
    }
}
