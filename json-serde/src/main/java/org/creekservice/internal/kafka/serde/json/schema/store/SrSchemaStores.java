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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.internal.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;

/** Tracks known schema stores */
public final class SrSchemaStores {

    private final Map<String, SrSchemaStore> cache = new ConcurrentHashMap<>();
    private final ClientFactory clientFactory;
    private final SchemaStoreFactory schemaStoreFactory;

    public SrSchemaStores(final ClientFactory clientFactory) {
        this(clientFactory, SrSchemaStore::new);
    }

    @VisibleForTesting
    SrSchemaStores(final ClientFactory clientFactory, final SchemaStoreFactory schemaStoreFactory) {
        this.clientFactory = requireNonNull(clientFactory, "clientFactory");
        this.schemaStoreFactory = requireNonNull(schemaStoreFactory, "schemaStoreFactory");
    }

    public SchemaStore get(final String schemaRegistryName) {
        return cache.computeIfAbsent(schemaRegistryName, this::create);
    }

    private SrSchemaStore create(final String schemaRegistryName) {
        final JsonSchemaStoreClient storeClient = clientFactory.create(schemaRegistryName);
        return schemaStoreFactory.create(storeClient);
    }

    @FunctionalInterface
    public interface ClientFactory {
        JsonSchemaStoreClient create(String schemaRegistryName);
    }

    @VisibleForTesting
    interface SchemaStoreFactory {
        SrSchemaStore create(JsonSchemaStoreClient storeClient);
    }
}
