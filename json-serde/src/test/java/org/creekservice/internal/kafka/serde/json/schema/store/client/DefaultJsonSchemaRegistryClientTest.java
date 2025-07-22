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

package org.creekservice.internal.kafka.serde.json.schema.store.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient.VersionedSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultJsonSchemaRegistryClientTest {

    private static final String SUBJECT = "the-subject";
    @Mock private SchemaRegistryClient underlying;
    @Mock private Function<ProducerSchema, JsonSchema> converter;

    @Mock(name = "producer-schema-text")
    private ProducerSchema producerSchema;

    @Mock private Schema rawSchema1;
    @Mock private Schema rawSchema2;
    @Mock private io.confluent.kafka.schemaregistry.json.JsonSchema parsed1;
    @Mock private io.confluent.kafka.schemaregistry.json.JsonSchema parsed2;
    private DefaultJsonSchemaRegistryClient client;

    @BeforeEach
    void setUp() throws Exception {
        client = new DefaultJsonSchemaRegistryClient("sr-name", underlying, converter);

        when(underlying.getAllSubjects()).thenReturn(Set.of(SUBJECT, "other"));
        when(converter.apply(any())).thenReturn(parsed1);
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
                assertThrows(RuntimeException.class, () -> client.disableCompatability(SUBJECT));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to update subject's schema compatability checks to NONE,"
                                + " subject: the-subject, schemaRegistryName: sr-name"));
        assertThat(e.getCause(), is(sameInstance(exception)));
    }

    @Test
    void shouldRegisterSchema() throws Exception {
        // Given:
        when(underlying.register(
                        anyString(), any(io.confluent.kafka.schemaregistry.json.JsonSchema.class)))
                .thenReturn(245);

        // When:
        final int id = client.register(SUBJECT, producerSchema);

        // Then:
        verify(underlying).register(SUBJECT, parsed1);
        assertThat(id, is(245));
    }

    @Test
    void shouldThrowOnFailureToRegisterSchema() throws Exception {
        // Given:
        final RuntimeException exception = new RuntimeException();
        doThrow(exception)
                .when(underlying)
                .register(
                        anyString(), any(io.confluent.kafka.schemaregistry.json.JsonSchema.class));

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> client.register(SUBJECT, producerSchema));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to register schema. schema: producer-schema-text, subject:"
                                + " the-subject, schemaRegistryName: sr-name"));
        assertThat(e.getCause(), is(sameInstance(exception)));
    }

    @Test
    void shouldGetRegisteredSchemasId() throws Exception {
        // Given:
        when(underlying.getId(
                        anyString(), any(io.confluent.kafka.schemaregistry.json.JsonSchema.class)))
                .thenReturn(245);

        // When:
        final int id = client.registeredId(SUBJECT, producerSchema);

        // Then:
        verify(underlying).getId(SUBJECT, parsed1);
        assertThat(id, is(245));
    }

    @Test
    void shouldThrowOnFailureToGetRegisteredSchemasId() throws Exception {
        // Given:
        final RuntimeException exception = new RuntimeException();
        doThrow(exception)
                .when(underlying)
                .getId(anyString(), any(io.confluent.kafka.schemaregistry.json.JsonSchema.class));

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> client.registeredId(SUBJECT, producerSchema));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to retrieve registered schema. schema: producer-schema-text,"
                                + " subject: the-subject, schemaRegistryName: sr-name"));
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
        when(underlying.getByVersion(SUBJECT, 1, false)).thenReturn(rawSchema1);
        when(underlying.getByVersion(SUBJECT, 2, false)).thenReturn(rawSchema2);
        when(underlying.parseSchema(rawSchema1)).thenReturn(Optional.of(parsed1));
        when(underlying.parseSchema(rawSchema2)).thenReturn(Optional.of(parsed2));
        when(parsed1.canonicalString()).thenReturn("true");
        when(parsed2.canonicalString()).thenReturn("false");

        // When:
        final List<VersionedSchema> versions = client.allVersions(SUBJECT);

        // Then:
        assertThat(versions, hasSize(2));
        assertThat(versions.get(0).version(), is(1));
        assertThat(versions.get(0).schema(), is(ProducerSchema.fromJson("true")));
        assertThat(versions.get(1).version(), is(2));
        assertThat(versions.get(1).schema(), is(ProducerSchema.fromJson("false")));
    }

    @Test
    void shouldThrowOnFailureToGetAllVersions() throws Exception {
        // Given:
        final RuntimeException exception = new RuntimeException();
        doThrow(exception).when(underlying).getAllVersions(anyString(), anyBoolean());

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> client.allVersions(SUBJECT));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to retrieve all schema versions, subject: the-subject,"
                                + " schemaRegistryName: sr-name"));
        assertThat(e.getCause(), is(sameInstance(exception)));
    }

    @Test
    void shouldThrowOnFailureToParseAllVersions() throws Exception {
        // Given:
        when(underlying.getAllVersions(SUBJECT, false)).thenReturn(List.of(1));
        when(underlying.getByVersion(SUBJECT, 1, false)).thenReturn(rawSchema1);
        when(underlying.parseSchema(rawSchema1)).thenReturn(Optional.empty());

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> client.allVersions(SUBJECT));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to retrieve all schema versions, subject: the-subject,"
                                + " schemaRegistryName: sr-name"));
    }

    @Test
    void shouldThrowFromAllVersionsIfNotSchemaSchema() throws Exception {
        // Given:
        when(underlying.getAllVersions(SUBJECT, false)).thenReturn(List.of(1));
        when(underlying.getByVersion(SUBJECT, 1, false)).thenReturn(rawSchema1);
        final ParsedSchema otherSchema = mock();
        when(otherSchema.schemaType()).thenReturn("avro");
        when(underlying.parseSchema(rawSchema1)).thenReturn(Optional.of(otherSchema));

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> client.allVersions(SUBJECT));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to retrieve all schema versions, subject: the-subject,"
                                + " schemaRegistryName: sr-name"));
        assertThat(
                e.getCause().getMessage(),
                is(
                        "Existing schema is not JSON. version: 1, type: avro, subject: the-subject,"
                                + " schemaRegistryName: sr-name"));
    }
}
