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

package org.creekservice.internal.kafka.serde.json.schema.resource;

import static org.creekservice.internal.kafka.serde.json.util.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.creekservice.internal.kafka.serde.json.util.TopicDescriptors.outputTopic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.creekservice.api.kafka.metadata.schema.JsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.creekservice.internal.kafka.serde.json.model.TestKeyV0;
import org.creekservice.internal.kafka.serde.json.model.TestValueV0;
import org.creekservice.internal.kafka.serde.json.schema.store.SchemaStore;
import org.creekservice.internal.kafka.serde.json.schema.store.SrSchemaStores;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JsonSchemaResourceHandlerTest {

    private static final OwnedKafkaTopicOutput<TestKeyV0, TestValueV0> TOPIC =
            outputTopic("bob", TestKeyV0.class, TestValueV0.class, withPartitions(1));

    private static final JsonSchemaDescriptor<TestKeyV0> KEY_SCHEMA =
            TOPIC.resources()
                    .filter(JsonSchemaDescriptor.class::isInstance)
                    .map(JsonSchemaDescriptor.class::cast)
                    .filter(res -> res.part().type().equals(TestKeyV0.class))
                    .map(res -> (JsonSchemaDescriptor<TestKeyV0>) res)
                    .findAny()
                    .orElseThrow();
    private static final JsonSchemaDescriptor<TestValueV0> VALUE_SCHEMA =
            TOPIC.resources()
                    .filter(JsonSchemaDescriptor.class::isInstance)
                    .map(JsonSchemaDescriptor.class::cast)
                    .filter(res -> res.part().type().equals(TestValueV0.class))
                    .map(res -> (JsonSchemaDescriptor<TestValueV0>) res)
                    .findAny()
                    .orElseThrow();

    @Mock private SrSchemaStores schemaStores;
    @Mock private SchemaResourceValidator validator;

    @Mock private SchemaStore schemaStore;
    private final TestStructuredLogger logger = TestStructuredLogger.create();
    private JsonSchemaResourceHandler handler;

    @BeforeEach
    void setUp() {
        handler = new JsonSchemaResourceHandler(schemaStores, validator, logger);

        when(schemaStores.get(anyString())).thenReturn(schemaStore);
    }

    @Test
    void shouldLogSerdeOnEnsure() {
        // When:
        handler.ensure(List.of(KEY_SCHEMA));

        // Then:
        assertThat(
                logger.textEntries(),
                hasItem(
                        "DEBUG: {message=Ensuring topic schemas,"
                                + " schema-ids=[schema://default/bob/key]}"));
    }

    @Test
    void shouldGetEachStoreOnlyOnce() {
        // When:
        handler.ensure(List.of(KEY_SCHEMA, VALUE_SCHEMA));

        // Then:
        verify(schemaStores, times(1)).get(any());
    }

    @Test
    void shouldEnsureEach() {
        // When:
        handler.ensure(List.of(KEY_SCHEMA, VALUE_SCHEMA));

        // Then:
        verify(schemaStore).registerFromClasspath(TOPIC.key());
        verify(schemaStore).registerFromClasspath(TOPIC.value());
    }

    @Test
    void shouldThrowOnEnsureIfNotCreatable() {
        // Given:
        final JsonSchemaDescriptor<?> badSchema = mock();
        when(badSchema.id()).thenReturn(KEY_SCHEMA.id());

        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class, () -> handler.ensure(List.of(badSchema)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Schema descriptor is not creatable: schema-id: schema://default/bob/key,"
                                + " schema: Mock"));
    }
}
