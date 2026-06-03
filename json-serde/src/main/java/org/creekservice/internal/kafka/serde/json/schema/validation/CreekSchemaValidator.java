/*
 * Copyright 2023-2026 Creek Contributors (https://github.com/creek-service)
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

import java.util.Map;
import org.creekservice.api.json.schema.validator.JsonSchemaValidator;
import org.creekservice.api.json.schema.validator.SchemaValidationException;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.api.kafka.serde.json.schema.YamlSchema;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;

/** Validator implementation backed by the Creek JSON schema validator. */
public final class CreekSchemaValidator implements SchemaValidator {

    private final JsonSchemaValidator validator;

    /**
     * @param schema the YAML schema to validate against.
     */
    public CreekSchemaValidator(final YamlSchema schema) {
        try {
            this.validator = JsonSchemaValidator.fromSchema(schema.toString());
        } catch (SchemaValidationException e) {
            throw new FailedToParseSchemaException(schema, e);
        }
    }

    @Override
    public void validate(
            final Map<String, ?> objectProperties, final String topic, final Part part) {
        try {
            validator.validate(objectProperties);
        } catch (SchemaValidationException e) {
            throw new JsonSchemaValidationFailed(topic, part, e);
        }
    }

    /** Thrown when the schema cannot be parsed by the validator. */
    public static final class FailedToParseSchemaException extends SchemaException {

        /**
         * @param schema the schema that failed to parse.
         * @param cause the underlying parse error.
         */
        public FailedToParseSchemaException(final YamlSchema schema, final Throwable cause) {
            super("Failed to parse schema: " + schema.asJsonText(), cause);
        }
    }
}
