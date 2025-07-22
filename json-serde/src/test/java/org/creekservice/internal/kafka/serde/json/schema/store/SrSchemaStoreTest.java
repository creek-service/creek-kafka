/*
 * Copyright 2023-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.schema.store;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.internal.kafka.serde.json.schema.store.compatability.CompatabilityChecker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class SrSchemaStoreTest {

    private static final Class<?> PART_TYPE = String.class;

    @Mock private SrSchemaStore.SchemaLoader loader;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private PartDescriptor<?> part;

    @Mock private JsonSchemaStoreClient client;
    @Mock private ProducerSchema schema;
    @Mock private CompatabilityChecker compatabilityChecker;
    private SrSchemaStore store;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @BeforeEach
    void setUp() {
        store = new SrSchemaStore(client, loader, compatabilityChecker);

        when(part.name()).thenReturn(PartDescriptor.Part.value);
        when(part.type()).thenReturn((Class) PART_TYPE);
        when(part.topic().name()).thenReturn("BigBadBob");
        when(loader.loadFromClasspath(PART_TYPE)).thenReturn(schema);
    }

    @Test
    void shouldThrowOnRegisterOnMissingSchema() {
        // Given:
        final RuntimeException expected = new RuntimeException("boom");
        when(loader.loadFromClasspath(part.type())).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> store.registerFromClasspath(part));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldThrowOnRegisterOnClientError() {
        // Given:
        when(client.register(any(), any())).thenThrow(new RuntimeException("boom"));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> store.registerFromClasspath(part));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Schema store operation failed, topic: BigBadBob, part: value, type:"
                                + " java.lang.String"));
        assertThat(e.getCause().getMessage(), is("boom"));
    }

    @Test
    void shouldRegisterKeySchema() {
        // Given:
        when(part.name()).thenReturn(PartDescriptor.Part.key);
        when(client.register("BigBadBob.key", schema)).thenReturn(39);

        // When:
        final RegisteredSchema<?> result = store.registerFromClasspath(part);

        // Then:
        assertThat(result, is(new RegisteredSchema<>(schema, 39, "BigBadBob.key", PART_TYPE)));
    }

    @Test
    void shouldRegisterValueSchema() {
        // Given:
        when(part.name()).thenReturn(PartDescriptor.Part.value);
        when(client.register("BigBadBob.value", schema)).thenReturn(843);

        // When:
        final RegisteredSchema<?> result = store.registerFromClasspath(part);

        // Then:
        assertThat(result, is(new RegisteredSchema<>(schema, 843, "BigBadBob.value", PART_TYPE)));
    }

    @Test
    void shouldDisableServerSideCompatabilityChecks() {
        // When:
        store.registerFromClasspath(part);

        // Then:
        verify(client).disableCompatability("BigBadBob.value");
    }

    @Test
    void shouldNotCheckSchemaCompatabilityOnLoad() {
        // When:
        store.loadFromClasspath(part);

        // Then:
        verify(compatabilityChecker, never()).checkCompatability(any(), any());
    }

    @Test
    void shouldCheckSchemaCompatabilityOnRegister() {
        // When:
        store.registerFromClasspath(part);

        // Then:
        verify(compatabilityChecker).checkCompatability("BigBadBob.value", schema);
    }

    @Test
    void shouldThrowOnIncompatibleSchema() {
        // Given:
        final RuntimeException exception = new RuntimeException("Boom");
        doThrow(exception).when(compatabilityChecker).checkCompatability(any(), any());

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> store.registerFromClasspath(part));

        // Then:
        verify(client, never()).register(any(), any());
        assertThat(
                e.getMessage(),
                is(
                        "Schema store operation failed, topic: BigBadBob, part: value, type:"
                                + " java.lang.String"));
        assertThat(e.getCause(), is(exception));
    }

    @Test
    void shouldThrowOnLoadOnMissingSchema() {
        // Given:
        final RuntimeException expected = new RuntimeException("boom");
        when(loader.loadFromClasspath(part.type())).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> store.loadFromClasspath(part));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldThrowOnLoadOnClientError() {
        // Given:
        when(client.registeredId(any(), any())).thenThrow(new RuntimeException("boom"));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> store.loadFromClasspath(part));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Schema store operation failed, topic: BigBadBob, part: value, type:"
                                + " java.lang.String"));
        assertThat(e.getCause().getMessage(), is("boom"));
    }

    @Test
    void shouldLoadKeySchema() {
        // Given:
        when(part.name()).thenReturn(PartDescriptor.Part.key);
        when(client.registeredId(any(), any())).thenReturn(39);

        // When:
        final RegisteredSchema<?> result = store.loadFromClasspath(part);

        // Then:
        assertThat(result, is(new RegisteredSchema<>(schema, 39, "BigBadBob.key", PART_TYPE)));
    }

    @Test
    void shouldLoadValueSchema() {
        // Given:
        when(part.name()).thenReturn(PartDescriptor.Part.value);
        when(client.registeredId(any(), any())).thenReturn(298);

        // When:
        final RegisteredSchema<?> result = store.loadFromClasspath(part);

        // Then:
        assertThat(result, is(new RegisteredSchema<>(schema, 298, "BigBadBob.value", PART_TYPE)));
    }
}
