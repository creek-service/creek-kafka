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

package org.creekservice.internal.kafka.serde.json.schema.store.endpoint;

import static java.util.stream.Collectors.toMap;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaStoreEndpoints;

/**
 * Loads SchemaRegistry endpoint info from environment variables.
 *
 * <p>Environment variable names should be prefixed with {@link
 * SystemEnvSchemaRegistryEndpointLoader#SR_PREFIX SCHEMA_REGISTRY_}, followed by the logical
 * instance name in upper case, followed by the Schema Registry client config name, in uppercase and
 * with any periods converted to {Qcode _}.
 *
 * <p>For example, an unsecured schema registry instance at {@code http://localhost:9823}, the
 * client only requires the endpoint information. Given an instance name of {@code default}, the
 * required environment variable would be:
 *
 * <p>{@code SCHEMA_REGISTRY_DEFAULT_ENDPOINTS=http://localhost:9823}
 *
 * <p>An instance secured with a username and password would require additional config:
 *
 * <p>{@code SCHEMA_REGISTRY_DEFAULT_ENDPOINTS=http://localhost:9823}
 *
 * <p>{@code SCHEMA_REGISTRY_DEFAULT_BASIC_AUTH_CREDENTIALS_SOURCE=USER_INFO}
 *
 * <p>{@code SCHEMA_REGISTRY_DEFAULT_BASIC_AUTH_USER_INFO=username:password}
 *
 * <p>See {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig} and the
 * Confluent website for more info on other supported authentication mechanisms and their required
 * configs.
 */
public final class SystemEnvSchemaRegistryEndpointLoader implements SchemaStoreEndpoints.Loader {

    private static final String SR_PREFIX = "SCHEMA_REGISTRY_";
    /**
     * Environment var used to set the endpoints for a schema registry instance.
     *
     * <p>Full var name is {@code SCHEMA_REGISTRY_<INSTANCE_NAME>_ENDPOINTS}
     */
    private static final String ENDPOINTS_CONFIG = "ENDPOINTS";

    private final Map<String, String> env;

    public SystemEnvSchemaRegistryEndpointLoader() {
        this(System.getenv());
    }

    @VisibleForTesting
    SystemEnvSchemaRegistryEndpointLoader(final Map<String, String> env) {
        this.env =
                env.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(SR_PREFIX))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public SchemaStoreEndpoints load(final String schemaRegistryInstance) {
        final List<URI> endpoints = endpoints(schemaRegistryInstance);
        final Map<String, ?> config = configs(schemaRegistryInstance);
        return SchemaStoreEndpoints.create(endpoints, config);
    }

    private List<URI> endpoints(final String schemaRegistryInstance) {
        final String endPoints = env.get(endpointConfig(schemaRegistryInstance));
        if (endPoints == null) {
            throw new InvalidSchemaRegistryEndpointException(
                    "Endpoints variable was not set", schemaRegistryInstance);
        }

        final List<URI> uris =
                Arrays.stream(endPoints.split("\\s*,\\s*"))
                        .filter(txt -> !txt.isBlank())
                        .map(URI::create)
                        .collect(Collectors.toList());
        if (uris.isEmpty()) {
            throw new InvalidSchemaRegistryEndpointException(
                    "Endpoints variable was empty", schemaRegistryInstance);
        }
        return uris;
    }

    private Map<String, String> configs(final String schemaRegistryInstance) {
        final String instanceName = schemaRegistryInstance.toUpperCase();
        final String prefix = SR_PREFIX + instanceName + "_";
        final String endpoints = endpointConfig(instanceName);
        return env.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .filter(e -> !e.getKey().equals(endpoints))
                .collect(toMap(e -> toConfigKey(e.getKey(), prefix), Map.Entry::getValue));
    }

    private String toConfigKey(final String envKey, final String prefix) {
        return envKey.replaceAll(prefix, SR_PREFIX).replaceAll("_", ".").toLowerCase();
    }

    private static String endpointConfig(final String schemaRegistryInstance) {
        return SR_PREFIX + schemaRegistryInstance.toUpperCase() + "_" + ENDPOINTS_CONFIG;
    }

    private static final class InvalidSchemaRegistryEndpointException extends RuntimeException {
        InvalidSchemaRegistryEndpointException(
                final String msg, final String schemaRegistryInstance) {
            super(
                    msg
                            + ". instanceName: "
                            + schemaRegistryInstance
                            + ", envName: "
                            + endpointConfig(schemaRegistryInstance));
        }
    }
}
