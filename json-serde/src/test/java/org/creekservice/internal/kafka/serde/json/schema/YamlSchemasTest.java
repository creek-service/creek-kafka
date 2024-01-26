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

package org.creekservice.internal.kafka.serde.json.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.dataformat.yaml.JacksonYAMLParseException;
import com.google.common.testing.NullPointerTester;
import org.junit.jupiter.api.Test;

class YamlSchemasTest {

    private static final String OPEN_CONTENT_MODEL =
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
                    + "      additionalProperties: false\n"
                    + "    required:\n"
                    + "    - id\n";

    @Test
    void shouldThrowNPEs() {
        new NullPointerTester().testAllPublicStaticMethods(YamlSchemas.class);
    }

    @Test
    void shouldThrowOnConvertOnInvalidYaml() {
        assertThrows(
                YamlSchemas.InvalidSchemaException.class,
                () -> YamlSchemas.yamlToJson("---\nnot:\nYAML"));
    }

    @Test
    void shouldThrowOnConvertOnInvalidJson() {
        assertThrows(
                YamlSchemas.InvalidSchemaException.class, () -> YamlSchemas.jsonToYaml("Not JSON"));
    }

    @Test
    void shouldConvertYamlToJson() {
        assertThat(YamlSchemas.yamlToJson("---\ntrue"), is("true"));
    }

    @Test
    void shouldConvertJsonToYaml() {
        assertThat(YamlSchemas.jsonToYaml("true"), is("--- true\n"));
    }

    @Test
    public void shouldThrowIfNotValidYaml() {
        // Given:
        final String notYaml =
                "---\n"
                        + "$id: invalid-yaml-schema.yml\n"
                        + "$schema: http://json-schema.org/draft-07/schema\n"
                        + "additionalProperties: false\n"
                        + "type: object\n"
                        + "\n"
                        + "Not Yaml";

        // When:
        final Exception e =
                assertThrows(
                        YamlSchemas.InvalidSchemaException.class,
                        () -> YamlSchemas.validate(notYaml));

        // Then:
        assertThat(e.getMessage(), is("Invalid YAML: " + notYaml));
        assertThat(e.getCause(), is(instanceOf(JacksonYAMLParseException.class)));
    }

    @Test
    public void shouldThrowIfNotValidSchema() {
        // Given:
        final String notYaml = "---\n" + "$id: this is valid YAML, but not a valid schema\n";

        // When:
        final Exception e =
                assertThrows(
                        YamlSchemas.InvalidSchemaException.class,
                        () -> YamlSchemas.validate(notYaml));

        // Then:
        assertThat(e.getMessage(), is("Invalid YAML schema: " + notYaml));
        assertThat(e.getCause().getMessage(), containsString("URISyntaxException"));
    }

    @Test
    void shouldConvertClosedToOpenContentModel() {
        // Given:
        final String closed =
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
                        + "      additionalProperties: false\n"
                        + "    required:\n"
                        + "     - id\n";

        // When:
        final String result = YamlSchemas.toOpenContentModel(closed);

        // Then:
        assertThat(result, is(OPEN_CONTENT_MODEL));
    }

    @Test
    void shouldDetectClosedContentModel() {
        // Given:
        final String schema =
                "---\n"
                        + "$schema: http://json-schema.org/draft-07/schema#\n"
                        + "type: object\n"
                        + "additionalProperties: false\n";

        assertThat(YamlSchemas.isClosedContentModel(schema), is(true));
    }

    @Test
    void shouldIgnorePropertiesCalledAdditionalProperties() {
        // Given:
        final String schema =
                "---\n"
                        + "$schema: http://json-schema.org/draft-07/schema#\n"
                        + "type: object\n"
                        + "additionalProperties: false\n"
                        + "properties:\n"
                        + "  zar:\n"
                        + "    type: string\n"
                        + "  additionalProperties: true\n";

        assertThat(YamlSchemas.isClosedContentModel(schema), is(true));
    }

    @Test
    void shouldDetectImplicitOpenContentModel() {
        // Given:
        final String schema =
                "---\n" + "$schema: http://json-schema.org/draft-07/schema#\n" + "type: object\n";

        assertThat(YamlSchemas.isClosedContentModel(schema), is(false));
    }

    @Test
    void shouldDetectExplicitOpenContentModel() {
        // Given:
        final String schema =
                "---\n"
                        + "$schema: http://json-schema.org/draft-07/schema#\n"
                        + "type: object\n"
                        + "additionalProperties: true\n";

        assertThat(YamlSchemas.isClosedContentModel(schema), is(false));
    }

    @Test
    void shouldDetectExplicitPartiallyOpenContentModel() {
        // Given:
        final String schema =
                "---\n"
                        + "$schema: http://json-schema.org/draft-07/schema#\n"
                        + "type: object\n"
                        + "additionalProperties:\n"
                        + "  type: string\n";

        assertThat(YamlSchemas.isClosedContentModel(schema), is(false));
    }

    @Test
    void shouldDetectNestedOpenModels() {
        final String schema =
                "---\n"
                        + "$schema: http://json-schema.org/draft-07/schema#\n"
                        + "type: object\n"
                        + "additionalProperties: false\n"
                        + "properties:\n"
                        + "  zar:\n"
                        + "    $ref: '#/definitions/Zar'\n"
                        + "definitions:\n"
                        + "  Zar:\n"
                        + "    type: object\n"
                        + "    additionalProperties: true\n"
                        + "    properties:\n"
                        + "      id:\n"
                        + "        type: string\n"
                        + "    required:\n"
                        + "    - id\n";

        assertThat(YamlSchemas.isClosedContentModel(schema), is(false));
    }
}
