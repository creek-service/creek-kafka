/*
 * Copyright 2023-2025 Creek Contributors (https://github.com/creek-service)
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

import static java.util.Objects.requireNonNull;

import java.util.Map;
import net.jimblackler.jsonschemafriend.Schema;
import net.jimblackler.jsonschemafriend.SchemaStore;
import net.jimblackler.jsonschemafriend.ValidationException;
import net.jimblackler.jsonschemafriend.Validator;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.api.kafka.serde.json.schema.YamlSchema;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;

/** Validator implementation that uses JsonSchemaFriend validator library. */
public final class SchemaFriendValidator implements SchemaValidator {

    private final Validator validator;
    private final Schema parsedSchema;

    public SchemaFriendValidator(final YamlSchema schema) {
        this(schema, new Validator(true));
    }

    @VisibleForTesting
    SchemaFriendValidator(final YamlSchema schema, final Validator validator) {
        this.parsedSchema = parseSchema(schema);
        this.validator = requireNonNull(validator, "validator");
    }

    @Override
    public void validate(
            final Map<String, ?> objectProperties, final String topic, final Part part) {
        try {
            validator.validate(parsedSchema, objectProperties);
        } catch (ValidationException e) {
            throw new JsonSchemaValidationFailed(topic, part, e);
        }
    }

    private Schema parseSchema(final YamlSchema schema) {
        try {
            final SchemaStore schemaStore = new SchemaStore(true);
            final Object o = schema.asObject();
            return schemaStore.loadSchema(o);
        } catch (Exception e) {
            throw new FailedToParseSchemaException(schema, e);
        }
    }

    public static final class FailedToParseSchemaException extends SchemaException {

        public FailedToParseSchemaException(final YamlSchema schema, final Throwable cause) {
            super("Failed to parse schema: " + schema.asJsonText(), cause);
        }
    }

    @VisibleForTesting
    public static final class JsonSchemaValidationFailed extends SchemaException {
        JsonSchemaValidationFailed(
                final String topic, final Part part, final ValidationException cause) {
            super("Validation failed." + " topic: " + topic + ", part: " + part, cause);
        }
    }
}
