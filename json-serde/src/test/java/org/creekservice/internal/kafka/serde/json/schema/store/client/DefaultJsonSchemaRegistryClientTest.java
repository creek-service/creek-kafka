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

package org.creekservice.internal.kafka.serde.json.schema.store.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaRegistryEndpoint;
import org.creekservice.internal.kafka.serde.json.schema.store.client.JsonSchemaStoreClient.VersionedSchema;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultJsonSchemaRegistryClientTest {

    @Mock private SchemaRegistryClient underlying;

    @Nested
    class ClientCreationTest {

        private static final String SR_NAME = "sr-name";
        @Mock private JsonSchemaStoreClient.Factory.FactoryParams params;
        @Mock private Supplier<SchemaRegistryEndpoint.Loader> defaultEndpointLoader;
        @Mock private DefaultJsonSchemaRegistryClient.ClientFactory clientFactory;
        @Mock private SchemaRegistryEndpoint.Loader endpointLoader;
        @Mock private SchemaRegistryEndpoint endpoint;
        @Mock private SchemaRegistryClient client;

        @BeforeEach
        void setUp() {
            when(params.typeOverride(SchemaRegistryEndpoint.Loader.class))
                    .thenReturn(Optional.of(endpointLoader));
            when(endpointLoader.load(SR_NAME)).thenReturn(endpoint);
            when(endpoint.endpoints()).thenReturn(Set.of(URI.create("a"), URI.create("b")));

            when(clientFactory.create(any(), anyInt(), any(), any(), any())).thenReturn(client);
        }

        @Test
        void shouldThrowIfEndpointLoaderThrows() {
            // Given:
            final RuntimeException exception = new RuntimeException();
            when(endpointLoader.load(any())).thenThrow(exception);

            // When:
            final Exception e =
                    assertThrows(
                            RuntimeException.class,
                            () ->
                                    DefaultJsonSchemaRegistryClient.createClient(
                                            SR_NAME, params, defaultEndpointLoader, clientFactory));

            // Then:
            assertThat(e, is(exception));
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Test
        void shouldWorkWithDefaultEndpointLoader() {
            // Given:
            when(params.typeOverride(SchemaRegistryEndpoint.Loader.class))
                    .thenReturn(Optional.empty());
            final SchemaRegistryEndpoint.Loader endpointLoader2 = mock();
            when(defaultEndpointLoader.get()).thenReturn(endpointLoader2);
            when(endpointLoader2.load(any())).thenReturn(endpoint);

            final Map<String, ?> configs = Map.of("this", "that");
            when(endpoint.configs()).thenReturn((Map) configs);

            // When:
            DefaultJsonSchemaRegistryClient.createClient(
                    SR_NAME, params, defaultEndpointLoader, clientFactory);

            // Then:
            verify(defaultEndpointLoader).get();
            verify(endpointLoader, never()).load(SR_NAME);
            verify(endpointLoader2).load(SR_NAME);

            // Then:
            verify(clientFactory)
                    .create(
                            ArgumentMatchers.argThat(
                                    Matchers.containsInAnyOrder("a", "b")::matches),
                            anyInt(),
                            any(),
                            eq(configs),
                            any());
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Test
        void shouldWorkWithCustomEndpointLoader() {
            // Given:
            when(params.typeOverride(SchemaRegistryEndpoint.Loader.class))
                    .thenReturn(Optional.of(endpointLoader));

            final Map<String, ?> configs = Map.of("this", "that");
            when(endpoint.configs()).thenReturn((Map) configs);

            // When:
            DefaultJsonSchemaRegistryClient.createClient(
                    SR_NAME, params, defaultEndpointLoader, clientFactory);

            // Then:
            verify(clientFactory)
                    .create(
                            ArgumentMatchers.argThat(
                                    Matchers.containsInAnyOrder("a", "b")::matches),
                            anyInt(),
                            any(),
                            eq(configs),
                            any());
        }

        @Test
        void shouldCreateFactoryWithCorrectCommonParams() {
            // When:
            DefaultJsonSchemaRegistryClient.createClient(
                    SR_NAME, params, defaultEndpointLoader, clientFactory);

            // Then:
            verify(clientFactory)
                    .create(
                            any(),
                            eq(1000),
                            argThat(
                                    providers ->
                                            providers.size() == 1
                                                    && providers.get(0)
                                                            instanceof JsonSchemaProvider),
                            any(),
                            eq(Map.of()));
        }
    }

    @Nested
    class ClientFunctionalityTest {

        private static final String SUBJECT = "the-subject";

        @Mock(name = "json-schema-text")
        private JsonSchema jsonSchema;

        @Mock private Schema schema1;
        @Mock private Schema schema2;
        @Mock private JsonSchema parsed1;
        @Mock private JsonSchema parsed2;
        private DefaultJsonSchemaRegistryClient client;

        @BeforeEach
        void setUp() throws Exception {
            client = new DefaultJsonSchemaRegistryClient(underlying);

            when(underlying.getAllSubjects()).thenReturn(Set.of(SUBJECT, "other"));
        }

        @Test
        void shouldDisableCompatability() throws Exception {
            // When:
            client.disableCompatability(SUBJECT);

            // Then:
            verify(underlying).updateCompatibility(SUBJECT, "NONE");
        }

        @Test
        void shouldThrowOnFailureToDisableCompatability() throws Exception {
            // Given:
            final RuntimeException exception = new RuntimeException();
            doThrow(exception).when(underlying).updateCompatibility(any(), any());

            // When:
            final Exception e =
                    assertThrows(
                            RuntimeException.class, () -> client.disableCompatability(SUBJECT));

            // Then:
            assertThat(
                    e.getMessage(),
                    is(
                            "Failed to update subject's schema compatability checks to NONE,"
                                    + " subject: the-subject"));
            assertThat(e.getCause(), is(sameInstance(exception)));
        }

        @Test
        void shouldRegisterSchema() throws Exception {
            // Given:
            when(underlying.register(anyString(), any(JsonSchema.class))).thenReturn(245);

            // When:
            final int id = client.register(SUBJECT, jsonSchema);

            // Then:
            verify(underlying).register(SUBJECT, jsonSchema);
            assertThat(id, is(245));
        }

        @Test
        void shouldThrowOnFailureToRegisterSchema() throws Exception {
            // Given:
            final RuntimeException exception = new RuntimeException();
            doThrow(exception).when(underlying).register(anyString(), any(JsonSchema.class));

            // When:
            final Exception e =
                    assertThrows(
                            RuntimeException.class, () -> client.register(SUBJECT, jsonSchema));

            // Then:
            assertThat(
                    e.getMessage(),
                    is(
                            "Failed to register schema. schema: json-schema-text, subject:"
                                    + " the-subject"));
            assertThat(e.getCause(), is(sameInstance(exception)));
        }

        @Test
        void shouldGetRegisteredSchemasId() throws Exception {
            // Given:
            when(underlying.getId(anyString(), any(JsonSchema.class))).thenReturn(245);

            // When:
            final int id = client.registeredId(SUBJECT, jsonSchema);

            // Then:
            verify(underlying).getId(SUBJECT, jsonSchema);
            assertThat(id, is(245));
        }

        @Test
        void shouldThrowOnFailureToGetRegisteredSchemasId() throws Exception {
            // Given:
            final RuntimeException exception = new RuntimeException();
            doThrow(exception).when(underlying).getId(anyString(), any(JsonSchema.class));

            // When:
            final Exception e =
                    assertThrows(
                            RuntimeException.class, () -> client.registeredId(SUBJECT, jsonSchema));

            // Then:
            assertThat(
                    e.getMessage(),
                    is(
                            "Failed to retrieve registered schema. schema: json-schema-text,"
                                    + " subject: the-subject"));
            assertThat(e.getCause(), is(sameInstance(exception)));
        }

        @Test
        void shouldGetNoSchemaVersionsForUnknownSubject() throws Exception {
            // Given:
            when(underlying.getAllSubjects()).thenReturn(Set.of());

            // When:
            final List<VersionedSchema> versions = client.allVersions(SUBJECT);

            // Then:
            assertThat(versions, is(empty()));
            verify(underlying, never()).getAllVersions(any());
        }

        @Test
        void shouldGetAllSchemaVersions() throws Exception {
            // Given:
            when(underlying.getAllVersions(SUBJECT, false)).thenReturn(List.of(1, 2));
            when(underlying.getByVersion(SUBJECT, 1, false)).thenReturn(schema1);
            when(underlying.getByVersion(SUBJECT, 2, false)).thenReturn(schema2);
            when(underlying.parseSchema(schema1)).thenReturn(Optional.of(parsed1));
            when(underlying.parseSchema(schema2)).thenReturn(Optional.of(parsed2));

            // When:
            final List<VersionedSchema> versions = client.allVersions(SUBJECT);

            // Then:
            assertThat(versions, hasSize(2));
            assertThat(versions.get(0).version(), is(1));
            assertThat(versions.get(0).schema(), is(parsed1));
            assertThat(versions.get(1).version(), is(2));
            assertThat(versions.get(1).schema(), is(parsed2));
        }

        @Test
        void shouldThrowOnFailureToGetAllVersions() throws Exception {
            // Given:
            final RuntimeException exception = new RuntimeException();
            doThrow(exception).when(underlying).getAllVersions(anyString(), anyBoolean());

            // When:
            final Exception e =
                    assertThrows(RuntimeException.class, () -> client.allVersions(SUBJECT));

            // Then:
            assertThat(
                    e.getMessage(),
                    is("Failed to retrieve all schema versions, subject: the-subject"));
            assertThat(e.getCause(), is(sameInstance(exception)));
        }

        @Test
        void shouldThrowOnFailureToParseAllVersions() throws Exception {
            // Given:
            when(underlying.getAllVersions(SUBJECT, false)).thenReturn(List.of(1));
            when(underlying.getByVersion(SUBJECT, 1, false)).thenReturn(schema1);
            when(underlying.parseSchema(schema1)).thenReturn(Optional.empty());

            // When:
            final Exception e =
                    assertThrows(RuntimeException.class, () -> client.allVersions(SUBJECT));

            // Then:
            assertThat(
                    e.getMessage(),
                    is("Failed to retrieve all schema versions, subject: the-subject"));
        }

        @Test
        void shouldThrowFromAllVersionsIfNotSchemaSchema() throws Exception {
            // Given:
            when(underlying.getAllVersions(SUBJECT, false)).thenReturn(List.of(1));
            when(underlying.getByVersion(SUBJECT, 1, false)).thenReturn(schema1);
            final ParsedSchema otherSchema = mock();
            when(underlying.parseSchema(schema1)).thenReturn(Optional.of(otherSchema));

            // When:
            final Exception e =
                    assertThrows(RuntimeException.class, () -> client.allVersions(SUBJECT));

            // Then:
            assertThat(
                    e.getMessage(),
                    is("Failed to retrieve all schema versions, subject: the-subject"));
            assertThat(
                    e.getCause().getMessage(),
                    is("Existing schema is not JSON. version: 1, subject: the-subject"));
        }
    }
}
