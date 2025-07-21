/*
 * Copyright 2025 Creek Contributors (https://github.com/creek-service)
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.testing.NullPointerTester;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SchemaStoreEndpointsTest {

    private static final URI ENDPOINT_1 = URI.create("sr://1");
    private static final URI ENDPOINT_2 = URI.create("sr://2");

    @Test
    void shouldThrowNPEs() {
        new NullPointerTester()
                .setDefault(String.class, "not blank")
                .setDefault(Collection.class, List.of(URI.create("endpoint")))
                .testAllPublicStaticMethods(SchemaStoreEndpoints.class);
    }

    @Test
    void shouldThrowIfNoEndpoints() {
        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> SchemaStoreEndpoints.create(List.of(), Map.of()));

        // Then:
        assertThat(e.getMessage(), is("No schema registry urls supplied"));
    }

    @Test
    void shouldCreateEndpoint() {
        // When:
        final SchemaStoreEndpoints endpoint =
                SchemaStoreEndpoints.create(
                        List.of(ENDPOINT_1, ENDPOINT_2), Map.of("some", "config"));

        // Then:
        assertThat(endpoint.endpoints(), is(Set.of(ENDPOINT_1, ENDPOINT_2)));
        assertThat(endpoint.configs(), is(Map.of("some", "config")));
    }
}
