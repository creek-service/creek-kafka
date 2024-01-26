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

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.base.type.schema.GeneratedSchemas;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;

public final class LocalSchemaLoader {

    private static final String SCHEMA_DIRECTORY = "/schema/json/";

    private LocalSchemaLoader() {}

    public static ProducerSchema loadFromClasspath(final Class<?> type) {
        final Path schemaFile =
                GeneratedSchemas.schemaFileName(type, GeneratedSchemas.yamlExtension());
        return loadFromClasspath(schemaFile);
    }

    public static ProducerSchema loadFromClasspath(final Path schemaFile) {
        final URL resource = findResource(schemaFile);
        return load(resource);
    }

    private static URL findResource(final Path schemaFile) {
        final String path = SCHEMA_DIRECTORY + schemaFile;
        final URL resource = LocalSchemaLoader.class.getResource(path);
        if (resource == null) {
            throw new SchemaResourceNotFoundException(
                    "Failed to load schema resource: " + path + ". Resource not found.");
        }

        return resource;
    }

    @VisibleForTesting
    static ProducerSchema load(final URL resource) {
        final String yaml = loadYaml(resource);
        return ProducerSchema.fromYaml(yaml);
    }

    private static String loadYaml(final URL resource) {
        try (InputStream s = resource.openStream()) {
            return new String(s.readAllBytes(), StandardCharsets.UTF_8);
        } catch (final Exception e) {
            throw new SchemaResourceNotFoundException(
                    "Failed to load schema resource: " + resource + ".", e);
        }
    }

    @VisibleForTesting
    static final class SchemaResourceNotFoundException extends SchemaException {

        SchemaResourceNotFoundException(final String message, final Throwable cause) {
            super(message, cause);
        }

        SchemaResourceNotFoundException(final String message) {
            super(message);
        }
    }
}
