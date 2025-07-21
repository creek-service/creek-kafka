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

package org.creekservice.internal.kafka.serde.json.schema.store;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaStoreEndpoints;

/** Tracks known schema stores */
public final class SrSchemaStores {

    private final SchemaStoreEndpoints.Loader endpointsLoader;
    private final JsonSchemaStoreClient.Factory clientFactory;
    private final SchemaStoreFactory schemaStoreFactory;
    private final Map<String, SrSchemaStore> cache = new ConcurrentHashMap<>();

    public SrSchemaStores(
            final SchemaStoreEndpoints.Loader endpointsLoader,
            final JsonSchemaStoreClient.Factory clientFactory) {
        this(endpointsLoader, clientFactory, SrSchemaStore::new);
    }

    @VisibleForTesting
    SrSchemaStores(
            final SchemaStoreEndpoints.Loader endpointsLoader,
            final JsonSchemaStoreClient.Factory clientFactory,
            final SchemaStoreFactory schemaStoreFactory) {
        this.endpointsLoader = requireNonNull(endpointsLoader, "endpointsLoader");
        this.clientFactory = requireNonNull(clientFactory, "clientFactory");
        this.schemaStoreFactory = requireNonNull(schemaStoreFactory, "schemaStoreFactory");
    }

    public SchemaStore get(final String schemaRegistryName) {
        return cache.computeIfAbsent(schemaRegistryName, this::create);
    }

    private SrSchemaStore create(final String schemaRegistryName) {
        final SchemaStoreEndpoints endpoints = endpointsLoader.load(schemaRegistryName);
        final JsonSchemaStoreClient storeClient =
                clientFactory.create(schemaRegistryName, endpoints);
        return schemaStoreFactory.create(storeClient);
    }

    @VisibleForTesting
    interface SchemaStoreFactory {
        SrSchemaStore create(JsonSchemaStoreClient storeClient);
    }
}
