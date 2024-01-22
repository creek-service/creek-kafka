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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaRegistryEndpoint;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;
import org.creekservice.internal.kafka.serde.json.schema.store.endpoint.SystemEnvSchemaRegistryEndpointLoader;

public final class DefaultJsonSchemaRegistryClient implements JsonSchemaStoreClient {

    private static final int MAX_CACHED_SCHEMAS = 1000;

    private final SchemaRegistryClient client;

    public DefaultJsonSchemaRegistryClient(
            final String schemaRegistryName, final Factory.FactoryParams ctx) {
        this(
                createClient(
                        schemaRegistryName,
                        ctx,
                        SystemEnvSchemaRegistryEndpointLoader::new,
                        CachedSchemaRegistryClient::new));
    }

    @VisibleForTesting
    DefaultJsonSchemaRegistryClient(final SchemaRegistryClient client) {
        this.client = requireNonNull(client, "client");
    }

    @Override
    public void disableCompatability(final String subject) {
        try {
            client.updateCompatibility(subject, "NONE");
        } catch (final Exception e) {
            throw new SchemaRegistryClientException(
                    "Failed to update subject's schema compatability checks to NONE", subject, e);
        }
    }

    @Override
    public int register(final String subject, final JsonSchema schema) {
        try {
            return client.register(subject, schema);
        } catch (final Exception e) {
            throw new SchemaRegistryClientException(
                    "Failed to register schema. schema: " + schema, subject, e);
        }
    }

    @Override
    public int registeredId(final String subject, final JsonSchema schema) {
        try {
            return client.getId(subject, schema);
        } catch (final Exception e) {
            throw new SchemaRegistryClientException(
                    "Failed to retrieve registered schema. schema: " + schema, subject, e);
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
                    "Failed to retrieve all schema versions", subject, e);
        }
    }

    private VersionedSchema versionedSchema(
            final String subject, final int version, final Schema schema) {
        final ParsedSchema parsed = client.parseSchema(schema).orElseThrow();

        if (!(parsed instanceof JsonSchema)) {
            throw new SchemaRegistryClientException(
                    "Existing schema is not JSON. version: " + version, subject);
        }

        return new VersionedJsonSchema(version, (JsonSchema) parsed);
    }

    @VisibleForTesting
    static SchemaRegistryClient createClient(
            final String schemaRegistryName,
            final Factory.FactoryParams params,
            final Supplier<SchemaRegistryEndpoint.Loader> defaultEndpointLoader,
            final ClientFactory clientFactory) {
        final SchemaRegistryEndpoint endpoint =
                params.typeOverride(SchemaRegistryEndpoint.Loader.class)
                        .orElseGet(defaultEndpointLoader)
                        .load(schemaRegistryName);

        return clientFactory.create(
                endpoint.endpoints().stream().map(URI::toString).collect(toList()),
                MAX_CACHED_SCHEMAS,
                List.of(new JsonSchemaProvider()),
                endpoint.configs(),
                Map.of());
    }

    private static final class VersionedJsonSchema implements VersionedSchema {

        private final int version;
        private final JsonSchema schema;

        private VersionedJsonSchema(final int version, final JsonSchema schema) {
            this.version = version;
            this.schema = requireNonNull(schema, "schema");
        }

        @Override
        public int version() {
            return version;
        }

        @Override
        public JsonSchema schema() {
            return schema;
        }
    }

    private static final class SchemaRegistryClientException extends SchemaException {

        SchemaRegistryClientException(final String msg, final String subject) {
            super(msg + ", subject: " + subject);
        }

        SchemaRegistryClientException(
                final String msg, final String subject, final Throwable cause) {
            super(msg + ", subject: " + subject, cause);
        }
    }

    @VisibleForTesting
    interface ClientFactory {
        SchemaRegistryClient create(
                List<String> baseUrls,
                int cacheCapacity,
                List<SchemaProvider> providers,
                Map<String, ?> originals,
                Map<String, String> httpHeaders);
    }
}
