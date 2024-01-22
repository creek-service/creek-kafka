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

package org.creekservice.internal.kafka.serde.json.schema.store;

import static java.util.Objects.requireNonNull;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.HashMap;
import java.util.Map;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.internal.kafka.serde.json.schema.LocalSchemaLoader;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;
import org.creekservice.internal.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.internal.kafka.serde.json.schema.store.compatability.CompatabilityChecker;

final class SrSchemaStore implements SchemaStore {

    private final SchemaLoader loader;
    private final JsonSchemaStoreClient client;
    private final CompatabilityChecker compatabilityChecker;
    private final Map<Class<?>, JsonSchema> fromClasspath = new HashMap<>();

    SrSchemaStore(final JsonSchemaStoreClient client) {
        this(client, LocalSchemaLoader::loadFromClasspath, new CompatabilityChecker(client));
    }

    @VisibleForTesting
    SrSchemaStore(
            final JsonSchemaStoreClient client,
            final SchemaLoader loader,
            final CompatabilityChecker compatabilityChecker) {
        this.loader = requireNonNull(loader, "loader");
        this.client = requireNonNull(client, "client");
        this.compatabilityChecker = requireNonNull(compatabilityChecker, "compatabilityChecker");
    }

    @Override
    public <T> RegisteredSchema<T> registerFromClasspath(final PartDescriptor<T> part) {
        final JsonSchema schema = schemaFromClasspath(part);
        final String subject = subjectName(part);

        try {
            compatabilityChecker.checkCompatability(subject, schema);
        } catch (final RuntimeException e) {
            throw new SchemaStoreException(part, e);
        }

        try {
            client.disableCompatability(subject);
            final int schemaId = client.register(subject, schema);
            return new RegisteredSchema<>(schema, schemaId, subject, part.type());
        } catch (final RuntimeException e) {
            throw new SchemaStoreException(part, e);
        }
    }

    @Override
    public <T> RegisteredSchema<T> loadFromClasspath(final PartDescriptor<T> part) {
        final JsonSchema schema = schemaFromClasspath(part);
        final String subject = subjectName(part);

        // Note: no compatability check is required here, as the part must be using a schema that is
        // already registered.

        try {
            final int schemaId = client.registeredId(subject, schema);
            return new RegisteredSchema<>(schema, schemaId, subject, part.type());
        } catch (final RuntimeException e) {
            throw new SchemaStoreException(part, e);
        }
    }

    private <T> JsonSchema schemaFromClasspath(final PartDescriptor<T> part) {
        return fromClasspath.computeIfAbsent(part.type(), loader::loadFromClasspath);
    }

    private static String subjectName(final PartDescriptor<?> part) {
        return part.topic().name() + "." + part.name();
    }

    @VisibleForTesting
    interface SchemaLoader {
        JsonSchema loadFromClasspath(Class<?> type);
    }

    private static final class SchemaStoreException extends SchemaException {
        SchemaStoreException(final PartDescriptor<?> part, final Throwable cause) {
            super(
                    "Schema store operation failed"
                            + ", topic: "
                            + part.topic().name()
                            + ", part: "
                            + part.name()
                            + ", type: "
                            + part.type().getName(),
                    cause);
        }
    }
}
