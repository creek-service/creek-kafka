/*
 * Copyright 2024-2025 Creek Contributors (https://github.com/creek-service)
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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.creekservice.api.base.annotation.VisibleForTesting;

/** Utility class for working with YAML schemas */
public final class YamlSchemas {

    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String TYPE = "type";
    private static final String OBJECT = "object";

    private static final ObjectMapper yamlMapper =
            new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    private static final ObjectMapper jsonMapper =
            JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();

    private YamlSchemas() {}

    /**
     * Convert a JSON schema string to YAML format.
     *
     * @param json the JSON schema text to convert.
     * @return the equivalent YAML schema text.
     */
    public static String jsonToYaml(final String json) {
        requireNonNull(json);
        try {
            final JsonNode node = jsonMapper.readValue(json, JsonNode.class);
            return yamlMapper.writeValueAsString(node);
        } catch (final Exception e) {
            throw new InvalidSchemaException("Failed to convert JSON schema to YAML: " + json, e);
        }
    }

    /**
     * Convert a YAML schema string to JSON format.
     *
     * @param yaml the YAML schema text to convert.
     * @return the equivalent JSON schema text.
     */
    public static String yamlToJson(final String yaml) {
        requireNonNull(yaml);
        try {
            final JsonNode node = yamlMapper.readValue(yaml, JsonNode.class);
            return jsonMapper.writeValueAsString(node);
        } catch (final Exception e) {
            throw new InvalidSchemaException("Failed to convert YAML schema to JSON: " + yaml, e);
        }
    }

    /**
     * Convert a YAML schema string to a plain Java object suitable for use with schema validators.
     *
     * @param yaml the YAML schema text to convert.
     * @return the schema as a plain Java object.
     */
    public static Object yamlToObject(final String yaml) {
        final JsonNode jsonNode = yamlToJsonNode(requireNonNull(yaml));

        try {
            return yamlMapper.convertValue(jsonNode, Object.class);
        } catch (final Exception e) {
            throw new InvalidSchemaException("Failed to convert to object: " + yaml, e);
        }
    }

    /**
     * Validate that the provided YAML string is a valid JSON schema.
     *
     * @param yaml the YAML schema text to validate.
     * @throws InvalidSchemaException if the YAML is not a valid JSON schema.
     */
    public static void validate(final String yaml) {
        final JsonNode jsonNode = yamlToJsonNode(requireNonNull(yaml));

        try {
            final JsonSchema schema = new JsonSchema(jsonNode);
            schema.validate();
        } catch (final Exception e) {
            throw new InvalidSchemaException("Invalid YAML schema: " + yaml, e);
        }
    }

    /**
     * Check whether the given YAML schema uses a closed content model, i.e. all object definitions
     * have {@code additionalProperties: false}.
     *
     * @param yaml the YAML schema text to check.
     * @return {@code true} if all object definitions have a closed content model.
     */
    public static boolean isClosedContentModel(final String yaml) {
        final JsonNode jsonNode = yamlToJsonNode(requireNonNull(yaml));
        final AtomicBoolean notClosed = new AtomicBoolean(false);
        processTree(
                jsonNode,
                maybeModel -> {
                    if (maybeModel.isEmpty()) {
                        notClosed.set(true);
                    } else {
                        final JsonNode additionalProperties = maybeModel.get();

                        if (!(additionalProperties instanceof BooleanNode)) {
                            notClosed.set(true);
                        }
                        if (additionalProperties.asBoolean()) {
                            notClosed.set(true);
                        }
                    }
                    return Optional.empty();
                });
        return !notClosed.get();
    }

    /**
     * Convert a YAML schema to use an open content model by setting {@code additionalProperties:
     * true} on all object definitions.
     *
     * @param yaml the YAML schema text to convert.
     * @return the schema with all object definitions converted to an open content model.
     */
    public static String toOpenContentModel(final String yaml) {
        final JsonNode jsonNode = yamlToJsonNode(requireNonNull(yaml));

        final Optional<BooleanNode> open =
                Optional.of(yamlMapper.getNodeFactory().booleanNode(true));
        processTree(jsonNode, maybeOpen -> open);

        try {
            return yamlMapper.writeValueAsString(jsonNode);
        } catch (final Exception e) {
            throw new InvalidSchemaException(
                    "Failed to serialize consumer schema as YAML: " + jsonNode, e);
        }
    }

    private static void processTree(
            final JsonNode node,
            final Function<Optional<JsonNode>, Optional<? extends JsonNode>> function) {
        if (node.isObject()) {
            if (isObjectDefinition(node)) {
                function.apply(Optional.ofNullable(node.get(ADDITIONAL_PROPERTIES)))
                        .ifPresent(
                                replacement ->
                                        ((ObjectNode) node)
                                                .replace(ADDITIONAL_PROPERTIES, replacement));
            }
            node.properties().forEach(field -> processTree(field.getValue(), function));
        } else if (node.isArray()) {
            for (JsonNode element : node) {
                processTree(element, function);
            }
        }
    }

    private static boolean isObjectDefinition(final JsonNode node) {
        final JsonNode type = node.get(TYPE);
        return type instanceof TextNode && type.asText().equals(OBJECT);
    }

    private static JsonNode yamlToJsonNode(final String yaml) {
        try {
            return yamlMapper.readTree(yaml);
        } catch (final Exception e) {
            throw new InvalidSchemaException("Invalid YAML: " + yaml, e);
        }
    }

    @VisibleForTesting
    static final class InvalidSchemaException extends SchemaException {
        InvalidSchemaException(final String message, final Exception e) {
            super(message, e);
        }
    }
}
