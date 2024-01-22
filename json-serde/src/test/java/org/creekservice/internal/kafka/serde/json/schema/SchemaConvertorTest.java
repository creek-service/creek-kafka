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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.jupiter.api.Test;

class SchemaConvertorTest {

    @Test
    void shouldConvertClosedToOpenObjectModels() {
        // Given:
        final String closedSchema =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"type\": \"object\",\n"
                        + "  \"additionalProperties\": false,\n"
                        + "  \"properties\": {\n"
                        + "    \"foo\": {\"type\": \"integer\"},\n"
                        + "    \"bar\": {\"type\": \"string\",\"format\": \"date\"},\n"
                        + "    \"zar\": {\"$ref\": \"#/definitions/Zar\"}\n"
                        + "  },\n"
                        + "  \"definitions\": {\n"
                        + "    \"Zar\": {\n"
                        + "      \"type\": \"object\",\n"
                        + "      \"additionalProperties\": false,\n"
                        + "      \"properties\": {\n"
                        + "        \"id\": {\"type\": \"string\"}\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";

        final JsonSchema producerSchema = new JsonSchema(closedSchema);

        // When:
        final JsonSchema consumerSchema = SchemaConvertor.toConsumerSchema(producerSchema);

        // Then:
        assertThat(
                consumerSchema.canonicalString(),
                is(
                        "{\""
                                + "$schema\":\"http://json-schema.org/draft-07/schema#\","
                                + "\"type\":\"object\","
                                + "\"additionalProperties\":true,"
                                + "\"properties\":{"
                                + "\"foo\":{\"type\":\"integer\"},"
                                + "\"bar\":{\"type\":\"string\",\"format\":\"date\"},"
                                + "\"zar\":{\"$ref\":\"#/definitions/Zar\"}},"
                                + "\"definitions\":{"
                                + "\"Zar\":{"
                                + "\"type\":\"object\","
                                + "\"additionalProperties\":true,"
                                + "\"properties\":{"
                                + "\"id\":{\"type\":\"string\"}"
                                + "}}}}"));
    }
}
