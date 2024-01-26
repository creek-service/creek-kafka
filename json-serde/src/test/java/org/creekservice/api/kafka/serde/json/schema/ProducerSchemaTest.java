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

package org.creekservice.api.kafka.serde.json.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema.OpenSchemaException;
import org.junit.jupiter.api.Test;

class ProducerSchemaTest {

    private static final String CLOSED_YAML =
            "---\n"
                    + "$schema: http://json-schema.org/draft-07/schema#\n"
                    + "type: object\n"
                    + "additionalProperties: false\n"
                    + "properties:\n"
                    + "  foo:\n"
                    + "    type: integer\n"
                    + "  bar:\n"
                    + "    type: string\n"
                    + "    format: date\n"
                    + "  zar:\n"
                    + "    $ref: '#/definitions/Zar'\n"
                    + "  additionalProperties: false\n"
                    + "definitions:\n"
                    + "  Zar:\n"
                    + "    type: object\n"
                    + "    additionalProperties: false\n"
                    + "    properties:\n"
                    + "      id:\n"
                    + "        type: string\n"
                    + "      additionalProperties: false\n";

    private static final String CLOSED_JSON =
            "{\n"
                    + "  \"$schema\" : \"http://json-schema.org/draft-07/schema#\",\n"
                    + "  \"type\" : \"object\",\n"
                    + "  \"additionalProperties\" : false,\n"
                    + "  \"properties\" : {\n"
                    + "    \"foo\" : {\n"
                    + "      \"type\" : \"integer\"\n"
                    + "    },\n"
                    + "    \"bar\" : {\n"
                    + "      \"type\" : \"string\",\n"
                    + "      \"format\" : \"date\"\n"
                    + "    },\n"
                    + "    \"zar\" : {\n"
                    + "      \"$ref\" : \"#/definitions/Zar\"\n"
                    + "    },\n"
                    + "    \"additionalProperties\" : false\n"
                    + "  },\n"
                    + "  \"definitions\" : {\n"
                    + "    \"Zar\" : {\n"
                    + "      \"type\" : \"object\",\n"
                    + "      \"additionalProperties\" : false,\n"
                    + "      \"properties\" : {\n"
                    + "        \"id\" : {\n"
                    + "          \"type\" : \"string\"\n"
                    + "        },\n"
                    + "        \"additionalProperties\" : false\n"
                    + "      }\n"
                    + "    }\n"
                    + "  }\n"
                    + "}";

    private static final String OPEN_YAML =
            "---\n"
                    + "$schema: http://json-schema.org/draft-07/schema#\n"
                    + "type: object\n"
                    + "additionalProperties: true\n"
                    + "properties:\n"
                    + "  foo:\n"
                    + "    type: integer\n"
                    + "  bar:\n"
                    + "    type: string\n"
                    + "    format: date\n"
                    + "  zar:\n"
                    + "    $ref: '#/definitions/Zar'\n"
                    + "  additionalProperties: false\n"
                    + "definitions:\n"
                    + "  Zar:\n"
                    + "    type: object\n"
                    + "    additionalProperties: true\n"
                    + "    properties:\n"
                    + "      id:\n"
                    + "        type: string\n"
                    + "      additionalProperties: false\n";

    private static final String OPEN_JSON =
            "{\n"
                    + "  \"$schema\" : \"http://json-schema.org/draft-07/schema#\",\n"
                    + "  \"type\" : \"object\",\n"
                    + "  \"additionalProperties\" : true,\n"
                    + "  \"properties\" : {\n"
                    + "    \"foo\" : {\n"
                    + "      \"type\" : \"integer\"\n"
                    + "    },\n"
                    + "    \"bar\" : {\n"
                    + "      \"type\" : \"string\",\n"
                    + "      \"format\" : \"date\"\n"
                    + "    },\n"
                    + "    \"zar\" : {\n"
                    + "      \"$ref\" : \"#/definitions/Zar\"\n"
                    + "    },\n"
                    + "    \"additionalProperties\" : false\n"
                    + "  },\n"
                    + "  \"definitions\" : {\n"
                    + "    \"Zar\" : {\n"
                    + "      \"type\" : \"object\",\n"
                    + "      \"additionalProperties\" : true,\n"
                    + "      \"properties\" : {\n"
                    + "        \"id\" : {\n"
                    + "          \"type\" : \"string\"\n"
                    + "        },\n"
                    + "        \"additionalProperties\" : false\n"
                    + "      }\n"
                    + "    }\n"
                    + "  }\n"
                    + "}";

    @Test
    void shouldThrowNPEs() {
        new NullPointerTester().testAllPublicStaticMethods(ProducerSchema.class);
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        ProducerSchema.fromYaml("--- true\n"),
                        ProducerSchema.fromYaml("--- true\n"),
                        ProducerSchema.fromJson("true"))
                .addEqualityGroup(
                        ProducerSchema.fromYaml("--- false\n"),
                        ProducerSchema.fromJson("false"),
                        ProducerSchema.fromJson("false"))
                .addEqualityGroup(
                        ProducerSchema.fromYaml(CLOSED_YAML), ProducerSchema.fromJson(CLOSED_JSON))
                .testEquals();
    }

    @Test
    void shouldConvertClosedJsonToOpenObjectModels() {
        // Given:
        final ProducerSchema producerSchema = ProducerSchema.fromJson(CLOSED_JSON);

        // When:
        final ConsumerSchema consumerSchema = producerSchema.toConsumerSchema();

        // Then:
        assertThat(consumerSchema.asJsonText(), is(OPEN_JSON));

        assertThat(consumerSchema.toString(), is(OPEN_YAML));
    }

    @Test
    void shouldConvertClosedYamlToOpenObjectModels() {
        // Given:
        final ProducerSchema producerSchema = ProducerSchema.fromYaml(CLOSED_YAML);

        // When:
        final ConsumerSchema consumerSchema = producerSchema.toConsumerSchema();

        // Then:
        assertThat(consumerSchema.toString(), is(OPEN_YAML));

        assertThat(consumerSchema.asJsonText(), is(OPEN_JSON));
    }

    @Test
    void shouldConvertToJson() {
        // Given:
        final ProducerSchema schema = ProducerSchema.fromYaml(CLOSED_YAML);

        // When:
        final String result = schema.asJsonText();

        // Then:
        assertThat(result, is(CLOSED_JSON));
    }

    @Test
    void shouldThrowOnOpenYamlSchema() {
        // When:
        final Exception e =
                assertThrows(OpenSchemaException.class, () -> ProducerSchema.fromYaml(OPEN_YAML));

        // Then:
        assertThat(
                e.getMessage(),
                is("Producer schemas must have a closed content model: " + OPEN_YAML));
    }

    @Test
    void shouldThrowOnOpenJsonSchema() {
        // When:
        final Exception e =
                assertThrows(OpenSchemaException.class, () -> ProducerSchema.fromJson(OPEN_JSON));

        // Then:
        assertThat(
                e.getMessage(),
                is("Producer schemas must have a closed content model: " + OPEN_YAML));
    }
}
