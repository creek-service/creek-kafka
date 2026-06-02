/*
 * Copyright 2024-2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.schema.validation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.YamlSchema;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;
import org.junit.jupiter.api.Test;

class CreekSchemaValidatorTest {

    private static final String SCHEMA_JSON =
            "{\"type\":\"object\","
                    + "\"properties\":{\"name\":{\"type\":\"string\"}},"
                    + "\"additionalProperties\":false}";

    @Test
    void shouldNotThrowIfValidationPasses() {
        // Given:
        final CreekSchemaValidator validator =
                new CreekSchemaValidator(ProducerSchema.fromJson(SCHEMA_JSON));

        // When / Then: does not throw
        validator.validate(Map.of("name", "Alice"), "t", Part.value);
    }

    @Test
    void shouldThrowOnValidationFailure() {
        // Given:
        final CreekSchemaValidator validator =
                new CreekSchemaValidator(ProducerSchema.fromJson(SCHEMA_JSON));

        // When:
        final SchemaException e =
                assertThrows(
                        SchemaException.class,
                        () -> validator.validate(Map.of("extra", "disallowed"), "t", Part.value));

        // Then:
        assertThat(e.getMessage(), containsString("topic: t"));
        assertThat(e.getMessage(), containsString("part: value"));
        assertThat(e instanceof JsonSchemaValidationFailed, is(true));
    }

    @Test
    void shouldThrowOnSchemaParseFailed() {
        // Given:
        final YamlSchema badSchema =
                new YamlSchema() {
                    @Override
                    public String asJsonText() {
                        return "{}";
                    }

                    @Override
                    public String toString() {
                        return ": [";
                    }
                };

        // When / Then:
        assertThrows(
                CreekSchemaValidator.FailedToParseSchemaException.class,
                () -> new CreekSchemaValidator(badSchema));
    }
}
