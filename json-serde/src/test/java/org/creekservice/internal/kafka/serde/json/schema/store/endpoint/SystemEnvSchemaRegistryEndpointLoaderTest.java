/*
 * Copyright 2024 Creek Contributors (https://github.com/creek-service)
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

import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.BEARER_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE;
import static io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.USER_INFO_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.Map;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaRegistryEndpoint;
import org.junit.jupiter.api.Test;

class SystemEnvSchemaRegistryEndpointLoaderTest {

    private static final String INSTANCE_NAME = "bob";

    @Test
    void shouldThrowIfNoEndpointsDefined() {
        // Given:
        final SystemEnvSchemaRegistryEndpointLoader loader =
                new SystemEnvSchemaRegistryEndpointLoader(Map.of());

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> loader.load(INSTANCE_NAME));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Endpoints variable was not set. instanceName: bob, envName:"
                                + " SCHEMA_REGISTRY_BOB_ENDPOINTS"));
    }

    @Test
    void shouldThrowIfEmptyEndpointsDefined() {
        // Given:
        final SystemEnvSchemaRegistryEndpointLoader loader =
                new SystemEnvSchemaRegistryEndpointLoader(
                        Map.of("SCHEMA_REGISTRY_BOB_ENDPOINTS", " "));

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> loader.load(INSTANCE_NAME));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Endpoints variable was empty. instanceName: bob, envName:"
                                + " SCHEMA_REGISTRY_BOB_ENDPOINTS"));
    }

    @Test
    void shouldLoadCorrectEndpoints() {
        // Given:
        final SystemEnvSchemaRegistryEndpointLoader loader =
                new SystemEnvSchemaRegistryEndpointLoader(
                        Map.of(
                                "SCHEMA_REGISTRY_OTHER_ENDPOINTS", "https://wrong",
                                "SCHEMA_REGISTRY_BOB_ENDPOINTS", "https://something"));

        // When:
        final SchemaRegistryEndpoint endpoint = loader.load(INSTANCE_NAME);

        // Then:
        assertThat(endpoint.endpoints(), contains(URI.create("https://something")));
    }

    @Test
    void shouldSupportMultipleEndpoints() {
        // Given:
        final SystemEnvSchemaRegistryEndpointLoader loader =
                new SystemEnvSchemaRegistryEndpointLoader(
                        Map.of("SCHEMA_REGISTRY_BOB_ENDPOINTS", "a, b,c\t,\td"));

        // When:
        final SchemaRegistryEndpoint endpoint = loader.load(INSTANCE_NAME);

        // Then:
        assertThat(
                endpoint.endpoints(),
                containsInAnyOrder(
                        URI.create("a"), URI.create("b"), URI.create("c"), URI.create("d")));
    }

    @Test
    void shouldExtractConfig() {
        // Given:
        final SystemEnvSchemaRegistryEndpointLoader loader =
                new SystemEnvSchemaRegistryEndpointLoader(
                        Map.of(
                                "SCHEMA_REGISTRY_BOB_ENDPOINTS", "a",
                                "SCHEMA_REGISTRY_BOB_BASIC_AUTH_USER_INFO", "user:pw",
                                "SCHEMA_REGISTRY_BOB_BEARER_AUTH_CREDENTIALS_SOURCE", "foo"));

        // When:
        final SchemaRegistryEndpoint endpoint = loader.load(INSTANCE_NAME);

        // Then:
        assertThat(
                endpoint.configs(),
                is(
                        Map.of(
                                CLIENT_NAMESPACE + USER_INFO_CONFIG, "user:pw",
                                CLIENT_NAMESPACE + BEARER_AUTH_CREDENTIALS_SOURCE, "foo")));
    }
}
