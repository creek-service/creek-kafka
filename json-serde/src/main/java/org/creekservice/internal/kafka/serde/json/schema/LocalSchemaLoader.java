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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.base.type.schema.GeneratedSchemas;

public final class LocalSchemaLoader {

    private static final Path SCHEMA_DIRECTORY = Paths.get(File.separator, "schema", "json");

    private static final ObjectMapper yamlMapper =
            new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    private static final ObjectMapper jsonWriter = new ObjectMapper();

    private LocalSchemaLoader() {}

    public static JsonSchema loadFromClasspath(final Class<?> type) {
        final Path schemaFile =
                GeneratedSchemas.schemaFileName(type, GeneratedSchemas.yamlExtension());
        return loadFromClasspath(schemaFile);
    }

    public static JsonSchema loadFromClasspath(final Path schemaFile) {
        final URL resource = findResource(schemaFile);
        return load(resource);
    }

    private static URL findResource(final Path schemaFile) {
        final Path path = SCHEMA_DIRECTORY.resolve(schemaFile);
        final URL resource = LocalSchemaLoader.class.getResource(path.toString());
        if (resource == null) {
            throw new SchemaResourceNotFoundException(
                    "Failed to load schema resource: " + path + ". Resource not found.");
        }

        return resource;
    }

    @VisibleForTesting
    static JsonSchema load(final URL resource) {
        final String yaml = loadYaml(resource);
        final String json = yamlToJson(yaml, resource);

        try {
            final JsonSchema schema = new JsonSchema(json);
            schema.validate();
            return schema;
        } catch (final Exception e) {
            throw new InvalidSchemaException("Schema was invalid: " + resource + ".", e);
        }
    }

    private static String loadYaml(final URL resource) {
        try (InputStream s = resource.openStream()) {
            return new String(s.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final Exception e) {
            throw new SchemaResourceNotFoundException(
                    "Failed to load schema resource: " + resource + ".", e);
        }
    }

    private static String yamlToJson(final String yaml, final URL resource) {
        try {
            final Object obj = yamlMapper.readValue(yaml, Object.class);
            return jsonWriter.writeValueAsString(obj);
        } catch (final Exception e) {
            throw new InvalidSchemaException(
                    "Failed to convert schema to JSON: " + resource + ".", e);
        }
    }

    public static final class SchemaResourceNotFoundException extends SchemaException {

        SchemaResourceNotFoundException(final String message, final Throwable cause) {
            super(message, cause);
        }

        SchemaResourceNotFoundException(final String message) {
            super(message);
        }
    }

    public static final class InvalidSchemaException extends SchemaException {
        InvalidSchemaException(final String message, final Exception e) {
            super(message, e);
        }
    }
}
