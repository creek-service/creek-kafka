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

package org.creekservice.api.kafka.serde.json.schema.store.endpoint;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Type that stores the details of a schema store's endpoints */
public final class SchemaStoreEndpoints {

    private final Set<URI> endpoints;
    private final Map<String, ?> configs;

    /**
     * Build insecure endpoint
     *
     * @param endpoints endpoints the schema registry is listening on.
     * @param configs configuration for the rest client for authentication and more. See {@link
     *     io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig} for more info.
     * @return endpoint instance.
     */
    public static SchemaStoreEndpoints create(
            final Collection<URI> endpoints, final Map<String, ?> configs) {
        return new SchemaStoreEndpoints(endpoints, configs);
    }

    private SchemaStoreEndpoints(final Collection<URI> endpoints, final Map<String, ?> configs) {
        this.endpoints = Set.copyOf(requireNonNull(endpoints, "endpoints"));
        this.configs = Map.copyOf(requireNonNull(configs, "configs"));

        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("No schema registry urls supplied");
        }
    }

    public Set<URI> endpoints() {
        return endpoints;
    }

    public Map<String, ?> configs() {
        return Map.copyOf(configs);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SchemaStoreEndpoints that = (SchemaStoreEndpoints) o;
        return Objects.equals(endpoints, that.endpoints) && Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoints, configs);
    }

    @Override
    public String toString() {
        return "SchemaRegistryEndpoint{"
                + "endpoints="
                + endpoints
                // configs are sensitive:
                + ", num_configs="
                + configs.size()
                + '}';
    }

    /**
     * Type that can be used to control where the details of the schema registry are loaded from.
     *
     * <p>This type can be customised via the {@link
     * org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions.Builder#withTypeOverride}
     * method. Pass {@code SchemaRegistryEndpoint.Loader.class} as the first param and a custom
     * implementation as the second.
     *
     * <p>If not customised, the default {@link
     * org.creekservice.internal.kafka.serde.json.schema.store.endpoint.SystemEnvSchemaRegistryEndpointLoader}
     * will be used.
     */
    public interface Loader {

        /**
         * Load the endpoint data for the schema registry used by the supplied logical {@code
         * clusterName}.
         *
         * @param schemaRegistryInstance the logical name of the schema registry instance, as used
         *     in the topic descriptors, to load.
         * @return the endpoint information.
         */
        SchemaStoreEndpoints load(String schemaRegistryInstance);
    }
}
