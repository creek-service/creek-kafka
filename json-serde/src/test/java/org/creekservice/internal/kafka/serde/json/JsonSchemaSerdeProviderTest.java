/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.creekservice.internal.kafka.serde.json.util.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.metadata.schema.JsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaStoreEndpoints;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.kafka.serde.test.KafkaSerdeProviderTester;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.component.model.ComponentModelContainer.HandlerTypeRef;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.creekservice.internal.kafka.serde.json.model.TestValueV0;
import org.creekservice.internal.kafka.serde.json.schema.resource.JsonSchemaResourceHandler;
import org.creekservice.internal.kafka.serde.json.schema.serde.JsonSchemaSerdeFactory;
import org.creekservice.internal.kafka.serde.json.schema.store.RegisteredSchema;
import org.creekservice.internal.kafka.serde.json.schema.store.SchemaStore;
import org.creekservice.internal.kafka.serde.json.schema.store.SrSchemaStores;
import org.creekservice.internal.kafka.serde.json.util.TopicDescriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JsonSchemaSerdeProviderTest {

    private static final KafkaTopicDescriptor<Long, TestValueV0> DESCRIPTOR =
            TopicDescriptors.outputTopic(
                    "cluster-name",
                    "sr-name",
                    "topic-name",
                    long.class,
                    TestValueV0.class,
                    withPartitions(1));

    @Nested
    class ProviderTest {

        @Mock private JsonSchemaStoreClient.Factory defaultStoreClientFactory;
        @Mock private JsonSchemaSerdeProvider.SchemaStoresFactory schemaStoresFactory;
        @Mock private SchemaStoreEndpoints.Loader defaultEndpointsLoader;
        @Mock private SchemaStoreEndpoints endpoints;
        @Mock private SrSchemaStores schemaStores;

        @Mock(answer = Answers.RETURNS_DEEP_STUBS)
        private CreekService api;

        private JsonSchemaSerdeProvider provider;

        @BeforeEach
        void setUp() {
            provider =
                    new JsonSchemaSerdeProvider(
                            defaultStoreClientFactory, defaultEndpointsLoader, schemaStoresFactory);

            when(api.options().get(any())).thenReturn(Optional.empty());
            when(schemaStoresFactory.create(any(), any())).thenReturn(schemaStores);
            when(defaultEndpointsLoader.load(any())).thenReturn(endpoints);
        }

        @Test
        void shouldBeValid() {
            KafkaSerdeProviderTester.tester(JsonSchemaSerdeProvider.class)
                    .withExpectedFormat(serializationFormat("json-schema"))
                    .test();
        }

        @Test
        void shouldUseDefaultSchemaStoreClient() {
            // Given:
            final JsonSerdeExtensionOptions options = mock();
            when(api.options().get(JsonSerdeExtensionOptions.class))
                    .thenReturn(Optional.of(options));

            // When:
            final KafkaSerdeProvider.SerdeFactory serdeFactory = provider.initialize(api);

            // Then:
            verify(schemaStoresFactory).create(any(), eq(defaultStoreClientFactory));
            assertThat(serdeFactory, is(notNullValue()));
        }

        @Test
        void shouldUseCustomSchemaStoreClient() {
            // Given:
            final JsonSchemaStoreClient.Factory customStoreClientFactory =
                    mock(JsonSchemaStoreClient.Factory.class);

            final JsonSerdeExtensionOptions options = mock();
            when(api.options().get(JsonSerdeExtensionOptions.class))
                    .thenReturn(Optional.of(options));
            when(options.typeOverride(JsonSchemaStoreClient.Factory.class))
                    .thenReturn(Optional.of(customStoreClientFactory));

            // When:
            final KafkaSerdeProvider.SerdeFactory serdeFactory = provider.initialize(api);

            // Then:
            verify(schemaStoresFactory).create(any(), eq(customStoreClientFactory));
            assertThat(serdeFactory, is(notNullValue()));
            verify(defaultStoreClientFactory, never()).create(any(), any());
        }

        @Test
        void shouldWorkWithDefaultEndpointLoader() {
            // Given:
            final JsonSerdeExtensionOptions options = mock();
            when(api.options().get(JsonSerdeExtensionOptions.class))
                    .thenReturn(Optional.of(options));

            // When:
            final KafkaSerdeProvider.SerdeFactory serdeFactory = provider.initialize(api);

            // Then:
            verify(schemaStoresFactory).create(eq(defaultEndpointsLoader), any());
            assertThat(serdeFactory, is(notNullValue()));
        }

        @Test
        void shouldWorkWithCustomEndpointLoader() {
            // Given:
            final SchemaStoreEndpoints.Loader customLoader =
                    mock(SchemaStoreEndpoints.Loader.class);

            final JsonSerdeExtensionOptions options = mock();
            when(api.options().get(JsonSerdeExtensionOptions.class))
                    .thenReturn(Optional.of(options));
            when(options.typeOverride(SchemaStoreEndpoints.Loader.class))
                    .thenReturn(Optional.of(customLoader));

            // When:
            final KafkaSerdeProvider.SerdeFactory serdeFactory = provider.initialize(api);

            // Then:
            verify(schemaStoresFactory).create(eq(customLoader), any());
            assertThat(serdeFactory, is(notNullValue()));
            verify(defaultEndpointsLoader, never()).load(any());
        }

        @Test
        void shouldRegisterResourceHandler() {
            // When:
            provider.initialize(api);

            // Then:
            verify(api.components().model())
                    .addResource(
                            ArgumentMatchers.<HandlerTypeRef<JsonSchemaDescriptor<?>>>argThat(
                                    typeRef ->
                                            typeRef.toString()
                                                    .equals(
                                                            "HandlerTypeRef<"
                                                                + "org.creekservice.api.kafka.metadata.schema.JsonSchemaDescriptor"
                                                                + ">(){}")),
                            isA(JsonSchemaResourceHandler.class));
        }
    }

    @SuppressWarnings("resource")
    @Nested
    class FactoryTest {

        @Mock private SrSchemaStores schemaStores;
        @Mock private SchemaStore schemaStore;

        @Mock private RegisteredSchema<TestValueV0> registeredSchema;
        @Mock private JsonSchemaSerdeFactory jsonSchemaSerdeFactory;
        @Mock private Serde<Object> serde;
        private final TestStructuredLogger logger = TestStructuredLogger.create();
        private JsonSchemaSerdeProvider.JsonSerdeFactory factory;
        private KafkaTopicDescriptor.PartDescriptor<TestValueV0> part;

        @BeforeEach
        void setUp() {
            factory =
                    new JsonSchemaSerdeProvider.JsonSerdeFactory(
                            schemaStores, jsonSchemaSerdeFactory, logger);

            part = spy(DESCRIPTOR.value());

            when(schemaStores.get(any())).thenReturn(schemaStore);
            when(schemaStore.loadFromClasspath(part)).thenReturn(registeredSchema);
            when(jsonSchemaSerdeFactory.create(any())).thenReturn(serde);
        }

        @Test
        void shouldThrowIfNotAssociatedWithJsonSchema() {
            // Given:
            when(part.resources()).thenReturn(Stream.of());

            // When:
            final Exception e =
                    assertThrows(IllegalStateException.class, () -> factory.createSerde(part));

            // Then:
            assertThat(
                    e.getMessage(),
                    startsWith(
                            "Part is not associated with a JSON schema. topic:"
                                + " kafka-topic://cluster-name/topic-name, part: value (file:"));
        }

        @Test
        void shouldGetSchemaStoreByName() {
            // When:
            factory.createSerde(part);

            // Then:
            verify(schemaStores).get("sr-name");
        }

        @Test
        void shouldThrowFromCreateIfSchemaNotRegistered() {
            // Given:
            final Exception expected = new RuntimeException("Boom");
            when(schemaStore.loadFromClasspath(any())).thenThrow(expected);

            // When:
            final Exception e =
                    assertThrows(RuntimeException.class, () -> factory.createSerde(part));

            // Then:
            assertThat(e, is(expected));
        }

        @Test
        void shouldLogOnCreate() {
            // Given:
            when(registeredSchema.id()).thenReturn(12857);

            // When:
            factory.createSerde(part);

            // Then:
            assertThat(
                    logger.textEntries(),
                    hasItem(
                            "DEBUG: {message=Building JSON Schema serde, part=value,"
                                    + " schemaId=12857,"
                                    + " topicId=kafka-topic://cluster-name/topic-name}"));
        }

        @Test
        void shouldCreateSerde() {
            // When:
            final Serde<TestValueV0> result = factory.createSerde(part);

            // Then:
            verify(jsonSchemaSerdeFactory).create(registeredSchema);
            assertThat(result, is(sameInstance(serde)));
        }
    }
}
