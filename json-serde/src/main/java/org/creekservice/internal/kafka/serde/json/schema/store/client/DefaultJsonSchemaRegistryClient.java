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

package org.creekservice.internal.kafka.serde.json.schema.store.client;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.List;
import java.util.function.Function;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;

public final class DefaultJsonSchemaRegistryClient implements JsonSchemaStoreClient {

    private final String schemaRegistryName;
    private final SchemaRegistryClient client;
    private final Function<ProducerSchema, JsonSchema> converter;

    public DefaultJsonSchemaRegistryClient(
            final String schemaRegistryName, final SchemaRegistryClient client) {
        this(schemaRegistryName, client, s -> new JsonSchema(s.asJsonText()));
    }

    @VisibleForTesting
    DefaultJsonSchemaRegistryClient(
            final String schemaRegistryName,
            final SchemaRegistryClient client,
            final Function<ProducerSchema, JsonSchema> converter) {
        this.schemaRegistryName = requireNonBlank(schemaRegistryName, "schemaRegistryName");
        this.client = requireNonNull(client, "client");
        this.converter = requireNonNull(converter, "converter");
    }

    @Override
    public void disableCompatability(final String subject) {
        try {
            client.updateCompatibility(subject, "NONE");
        } catch (final Exception e) {
            throw new SchemaRegistryClientException(
                    "Failed to update subject's schema compatability checks to NONE",
                    subject,
                    schemaRegistryName,
                    e);
        }
    }

    @Override
    public int register(final String subject, final ProducerSchema schema) {
        try {
            return client.register(subject, converter.apply(schema));
        } catch (final Exception e) {
            throw new SchemaRegistryClientException(
                    "Failed to register schema. schema: " + schema, subject, schemaRegistryName, e);
        }
    }

    @Override
    public int registeredId(final String subject, final ProducerSchema schema) {
        try {
            return client.getId(subject, converter.apply(schema));
        } catch (final Exception e) {
            throw new SchemaRegistryClientException(
                    "Failed to retrieve registered schema. schema: " + schema,
                    subject,
                    schemaRegistryName,
                    e);
        }
    }

    @Override
    public List<VersionedSchema> allVersions(final String subject) {
        try {
            if (!client.getAllSubjects().contains(subject)) {
                return List.of();
            }

            return client.getAllVersions(subject, false).stream()
                    .map(
                            version ->
                                    versionedSchema(
                                            subject,
                                            version,
                                            client.getByVersion(subject, version, false)))
                    .collect(toList());
        } catch (final Exception e) {
            throw new SchemaRegistryClientException(
                    "Failed to retrieve all schema versions", subject, schemaRegistryName, e);
        }
    }

    private VersionedSchema versionedSchema(
            final String subject, final int version, final Schema schema) {
        final ParsedSchema parsed = client.parseSchema(schema).orElseThrow();

        if (!(parsed instanceof JsonSchema)) {
            throw new SchemaRegistryClientException(
                    "Existing schema is not JSON. version: "
                            + version
                            + ", type: "
                            + parsed.schemaType(),
                    subject,
                    schemaRegistryName);
        }

        return new VersionedJsonSchema(version, ProducerSchema.fromJson(parsed.canonicalString()));
    }

    private static final class VersionedJsonSchema implements VersionedSchema {

        private final int version;
        private final ProducerSchema schema;

        private VersionedJsonSchema(final int version, final ProducerSchema schema) {
            this.version = version;
            this.schema = requireNonNull(schema, "schema");
        }

        @Override
        public int version() {
            return version;
        }

        @Override
        public ProducerSchema schema() {
            return schema;
        }
    }

    private static final class SchemaRegistryClientException extends SchemaException {

        SchemaRegistryClientException(
                final String msg, final String subject, final String schemaRegistryName) {
            super(msg + ", subject: " + subject + ", schemaRegistryName: " + schemaRegistryName);
        }

        SchemaRegistryClientException(
                final String msg,
                final String subject,
                final String schemaRegistryName,
                final Throwable cause) {
            super(
                    msg + ", subject: " + subject + ", schemaRegistryName: " + schemaRegistryName,
                    cause);
        }
    }
}
