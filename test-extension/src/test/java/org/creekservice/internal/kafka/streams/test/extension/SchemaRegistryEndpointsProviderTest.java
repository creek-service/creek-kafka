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
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaRegistryEndpointsProviderTest {

    private static final String REGISTRY_NAME = "registry-a";

    private SchemaRegistryEndpointsProvider provider;

    @BeforeEach
    void setUp() {
        provider = new SchemaRegistryEndpointsProvider();
    }

    @Test
    void shouldReturnEmptyMapForUnknownRegistry() {
        assertThat(provider.get(REGISTRY_NAME), is(Map.of()));
    }

    @Test
    void shouldReturnConfigAfterPut() {
        // Given:
        final Map<String, String> config = Map.of("endpoints", "http://localhost:38081");
        provider.put(REGISTRY_NAME, config);

        // When / Then:
        assertThat(provider.get(REGISTRY_NAME), is(config));
    }

    @Test
    void shouldSupportMultipleRegistries() {
        // Given:
        final Map<String, String> configA = Map.of("endpoints", "http://localhost:38081");
        final Map<String, String> configB = Map.of("endpoints", "http://localhost:38082");
        provider.put("registry-a", configA);
        provider.put("registry-b", configB);

        // Then:
        assertThat(provider.get("registry-a"), is(configA));
        assertThat(provider.get("registry-b"), is(configB));
    }

    @Test
    void shouldOverwriteExistingConfig() {
        // Given:
        final Map<String, String> original = Map.of("endpoints", "http://localhost:38081");
        final Map<String, String> updated = Map.of("endpoints", "http://localhost:38082");
        provider.put(REGISTRY_NAME, original);

        // When:
        provider.put(REGISTRY_NAME, updated);

        // Then:
        assertThat(provider.get(REGISTRY_NAME), is(updated));
    }

    @Test
    void shouldThrowOnNullRegistryNameInGet() {
        assertThrows(NullPointerException.class, () -> provider.get(null));
    }

    @Test
    void shouldThrowOnNullRegistryNameInPut() {
        assertThrows(
                NullPointerException.class,
                () -> provider.put(null, Map.of("endpoints", "http://localhost:38081")));
    }

    @Test
    void shouldThrowOnNullConfigInPut() {
        assertThrows(NullPointerException.class, () -> provider.put(REGISTRY_NAME, null));
    }
}
