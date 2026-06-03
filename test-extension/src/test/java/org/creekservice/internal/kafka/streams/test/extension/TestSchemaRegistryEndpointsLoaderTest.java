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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import org.creekservice.api.kafka.serde.schema.store.endpoint.SchemaStoreEndpoints;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TestSchemaRegistryEndpointsLoaderTest {

    private static final String INSTANCE_NAME = "registry-a";
    private static final String ENDPOINT_URL = "http://localhost:38081";

    @Mock private SchemaRegistryEndpointsProvider provider;

    private TestSchemaRegistryEndpointsLoader loader;

    @BeforeEach
    void setUp() {
        loader = new TestSchemaRegistryEndpointsLoader(provider);
    }

    @Test
    void shouldLoadEndpointsFromProvider() {
        // Given:
        when(provider.get(INSTANCE_NAME)).thenReturn(Map.of("endpoints", ENDPOINT_URL));

        // When:
        final SchemaStoreEndpoints result = loader.load(INSTANCE_NAME);

        // Then:
        assertThat(result.endpoints(), is(Set.of(URI.create(ENDPOINT_URL))));
    }

    @Test
    void shouldThrowIfEndpointsKeyMissing() {
        // Given:
        when(provider.get(INSTANCE_NAME)).thenReturn(Map.of());

        // When:
        final Exception e =
                assertThrows(IllegalStateException.class, () -> loader.load(INSTANCE_NAME));

        // Then:
        assertThat(e.getMessage(), containsString(INSTANCE_NAME));
    }

    @Test
    void shouldThrowIfEndpointsValueIsBlank() {
        // Given:
        when(provider.get(INSTANCE_NAME)).thenReturn(Map.of("endpoints", ""));

        // When:
        final Exception e =
                assertThrows(IllegalStateException.class, () -> loader.load(INSTANCE_NAME));

        // Then:
        assertThat(e.getMessage(), containsString(INSTANCE_NAME));
    }
}
