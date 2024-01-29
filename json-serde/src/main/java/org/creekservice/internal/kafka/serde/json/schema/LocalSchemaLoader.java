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
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.base.type.schema.GeneratedSchemas;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;

public final class LocalSchemaLoader {

    private LocalSchemaLoader() {}

    public static ProducerSchema loadFromClasspath(final Class<?> type) {
        final URL resource = findResource(type);
        return load(resource);
    }

    private static URL findResource(final Class<?> type) {
        final String schemaFile =
                GeneratedSchemas.schemaFileName(type, GeneratedSchemas.yamlExtension());

        // Load from current module file:
        URL resource = type.getResource("/" + schemaFile);
        if (resource == null) {
            // Then try loading from other modules:
            resource = type.getClassLoader().getResource(schemaFile);
            if (resource == null) {
                throw new SchemaResourceNotFoundException(
                        "Failed to load schema resource: " + schemaFile + ". Resource not found.");
            }
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
