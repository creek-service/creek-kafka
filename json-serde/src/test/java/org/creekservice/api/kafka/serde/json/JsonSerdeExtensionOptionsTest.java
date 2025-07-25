/*
 * Copyright 2024-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.serde.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Map;
import java.util.Optional;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.MockEndpointsLoader;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaStoreEndpoints;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonSerdeExtensionOptionsTest {

    private JsonSerdeExtensionOptions.Builder builder;

    @BeforeEach
    void setUp() {
        builder = JsonSerdeExtensionOptions.builder();
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        JsonSerdeExtensionOptions.builder().build(),
                        JsonSerdeExtensionOptions.builder().build())
                .addEqualityGroup(
                        JsonSerdeExtensionOptions.builder().withSubtypes(String.class).build())
                .addEqualityGroup(
                        JsonSerdeExtensionOptions.builder()
                                .withSubtype(String.class, "name")
                                .build())
                .addEqualityGroup(
                        JsonSerdeExtensionOptions.builder()
                                .withTypeOverride(String.class, "diff")
                                .build())
                .testEquals();
    }

    @Test
    void shouldThrowNPEs() {
        final NullPointerTester tester =
                new NullPointerTester().setDefault(String.class, "not empty");

        tester.testAllPublicInstanceMethods(JsonSerdeExtensionOptions.builder());
        tester.testAllPublicInstanceMethods(JsonSerdeExtensionOptions.builder().build());
    }

    @Test
    void shouldDefaultToNoOverride() {
        assertThat(builder.build().typeOverride(String.class), is(Optional.empty()));
    }

    @Test
    void shouldSupportOverrides() {
        // When:
        builder.withTypeOverride(String.class, "value");

        // Then:
        assertThat(builder.build().typeOverride(String.class), is(Optional.of("value")));
    }

    @Test
    void shouldSupportNamedSubTypes() {
        // When:
        builder.withSubtype(String.class, "name");

        // Then:
        assertThat(builder.build().subTypes(), is(Map.of(String.class, "name")));
    }

    @Test
    void shouldSupportUnnamedSubTypes() {
        // When:
        builder.withSubtypes(String.class, Integer.class);

        // Then:
        assertThat(builder.build().subTypes(), is(Map.of(String.class, "", Integer.class, "")));
    }

    @Test
    void shouldSetMockStoreClient() {
        // When:
        final JsonSerdeExtensionOptions options = JsonSerdeExtensionOptions.testBuilder().build();

        // Then: the following doesn't throw due to no actual Schema Registry instance:
        final Optional<JsonSchemaStoreClient.Factory> factory =
                options.typeOverride(JsonSchemaStoreClient.Factory.class);
        assertThat(factory, is(not(Optional.empty())));
        final JsonSchemaStoreClient client = factory.orElseThrow().create("bob", null);
        client.disableCompatability("test");
    }

    @Test
    void shouldSetMockEndpointsLoader() {
        // When:
        final JsonSerdeExtensionOptions options = JsonSerdeExtensionOptions.testBuilder().build();

        // Then:
        final Optional<SchemaStoreEndpoints.Loader> loader =
                options.typeOverride(SchemaStoreEndpoints.Loader.class);
        assertThat(loader, is(not(Optional.empty())));
        assertThat(loader.orElseThrow(), is(instanceOf(MockEndpointsLoader.class)));
    }
}
