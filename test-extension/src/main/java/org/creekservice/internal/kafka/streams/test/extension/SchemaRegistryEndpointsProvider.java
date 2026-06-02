/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;

/**
 * Provider of Schema Registry endpoints.
 *
 * <p>Schema Registry endpoints are set to match the Schema Registry container instances started by
 * the test framework.
 */
public final class SchemaRegistryEndpointsProvider {

    private final Map<String, Map<String, String>> endpoints = new HashMap<>();

    /**
     * Get endpoint configuration for a schema registry.
     *
     * @param registryName the name of the schema registry instance.
     * @return the endpoint configuration.
     */
    public Map<String, String> get(final String registryName) {
        return endpoints.getOrDefault(requireNonNull(registryName, "registryName"), Map.of());
    }

    /**
     * Set endpoint configuration for a schema registry.
     *
     * @param registryName the name of the schema registry instance.
     * @param config the endpoint configuration to set.
     */
    public void put(final String registryName, final Map<String, String> config) {
        endpoints.put(
                requireNonNull(registryName, "registryName"), requireNonNull(config, "config"));
    }
}
