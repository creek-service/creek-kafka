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

package org.creekservice.internal.kafka.serde.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions;
import org.creekservice.api.kafka.serde.schema.store.endpoint.SchemaStoreEndpoints;
import org.creekservice.api.service.extension.CreekExtensionOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonSchemaTestExtensionInitializerTest {

    private static final String INSTANCE_NAME = "registry-a";
    private static final String ENDPOINT_URL = "http://localhost:38081";

    private JsonSchemaTestExtensionInitializer initializer;

    @BeforeEach
    void setUp() {
        initializer = new JsonSchemaTestExtensionInitializer();
    }

    @Test
    void shouldReturnSingleExtensionOption() {
        // When:
        final List<CreekExtensionOptions> result =
                initializer.extensionOptions(name -> Map.of("endpoints", ENDPOINT_URL));

        // Then:
        assertThat(result, hasSize(1));
        assertThat(result.get(0), is(instanceOf(JsonSerdeExtensionOptions.class)));
    }

    @Test
    void shouldConfigureSchemaStoreEndpointsLoader() {
        // Given:
        final List<CreekExtensionOptions> result =
                initializer.extensionOptions(name -> Map.of("endpoints", ENDPOINT_URL));

        final JsonSerdeExtensionOptions options = (JsonSerdeExtensionOptions) result.get(0);
        final SchemaStoreEndpoints.Loader loader =
                options.typeOverride(SchemaStoreEndpoints.Loader.class).orElseThrow();

        // When:
        final SchemaStoreEndpoints endpoints = loader.load(INSTANCE_NAME);

        // Then:
        assertThat(endpoints.endpoints(), is(Set.of(URI.create(ENDPOINT_URL))));
    }

    @Test
    void shouldThrowIfEndpointsKeyMissing() {
        // Given:
        final List<CreekExtensionOptions> result = initializer.extensionOptions(name -> Map.of());

        final JsonSerdeExtensionOptions options = (JsonSerdeExtensionOptions) result.get(0);
        final SchemaStoreEndpoints.Loader loader =
                options.typeOverride(SchemaStoreEndpoints.Loader.class).orElseThrow();

        // When:
        final Exception e =
                assertThrows(IllegalStateException.class, () -> loader.load(INSTANCE_NAME));

        // Then:
        assertThat(e.getMessage(), containsString(INSTANCE_NAME));
    }

    @Test
    void shouldThrowIfEndpointsValueIsBlank() {
        // Given:
        final List<CreekExtensionOptions> result =
                initializer.extensionOptions(name -> Map.of("endpoints", ""));

        final JsonSerdeExtensionOptions options = (JsonSerdeExtensionOptions) result.get(0);
        final SchemaStoreEndpoints.Loader loader =
                options.typeOverride(SchemaStoreEndpoints.Loader.class).orElseThrow();

        // When:
        final Exception e =
                assertThrows(IllegalStateException.class, () -> loader.load(INSTANCE_NAME));

        // Then:
        assertThat(e.getMessage(), containsString(INSTANCE_NAME));
    }
}
