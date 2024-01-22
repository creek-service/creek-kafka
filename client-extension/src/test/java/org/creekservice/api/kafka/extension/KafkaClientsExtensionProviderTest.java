/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.extension;

import static org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider.InitializeParams;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProviders;
import org.creekservice.api.platform.metadata.AggregateDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.component.model.ComponentModelContainer.HandlerTypeRef;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.extension.config.ClustersPropertiesFactory;
import org.creekservice.internal.kafka.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.extension.resource.TopicResourceHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaClientsExtensionProviderTest {

    private static final KafkaClientsExtensionOptions DEFAULT_OPTIONS =
            KafkaClientsExtensionOptions.builder().build();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekService api;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ClientsExtensionOptions userOptions;

    @Mock private ClustersPropertiesFactory propertiesFactory;
    @Mock private KafkaClientsExtensionProvider.KafkaSerdeProvidersFactory serdeProvidersFactory;
    @Mock private KafkaClientsExtensionProvider.HandlerFactory handlerFactory;
    @Mock private KafkaClientsExtensionProvider.ExtensionFactory extensionFactory;
    @Mock private KafkaSerdeProviders serdeProviders;
    @Mock private ClustersProperties clustersProperties;
    @Mock private ResourceRegistry resourceRegistry;
    @Mock private ClientsExtension clientsExtension;
    @Mock private TopicResourceHandler topicHandler;

    private Collection<? extends ComponentDescriptor> components;

    private KafkaClientsExtensionProvider provider;

    @BeforeEach
    void setUp() {
        components = List.of(mock(ServiceDescriptor.class), mock(AggregateDescriptor.class));

        provider =
                new KafkaClientsExtensionProvider(
                        propertiesFactory,
                        resourceRegistry,
                        handlerFactory,
                        extensionFactory,
                        serdeProvidersFactory);

        when(propertiesFactory.create(any(), any())).thenReturn(clustersProperties);
        when(handlerFactory.create(any(), any(), any(), any())).thenReturn(topicHandler);
        when(extensionFactory.create(any(), any())).thenReturn(clientsExtension);
        when(serdeProvidersFactory.create(any(), any())).thenReturn(serdeProviders);

        when(api.options().get(any())).thenReturn(Optional.empty());
        when(api.components().descriptors().stream()).thenAnswer(inv -> components.stream());
    }

    @Test
    void shouldCreateTopicHandlerWithDefaultOverrides() {
        // When:
        provider.initialize(api);

        // Then:
        verify(handlerFactory)
                .create(
                        DEFAULT_OPTIONS.typeOverrides(),
                        resourceRegistry,
                        clustersProperties,
                        serdeProviders);
    }

    @Test
    void shouldCreateTopicHandlerWithCustomOverrides() {
        // Given:
        when(api.options().get(ClientsExtensionOptions.class)).thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(handlerFactory)
                .create(
                        userOptions.typeOverrides(),
                        resourceRegistry,
                        clustersProperties,
                        serdeProviders);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldRegisterTopicHandler() {
        // When:
        provider.initialize(api);

        // Then:
        verify(api.components().model()).addResource(any(HandlerTypeRef.class), eq(topicHandler));
    }

    @Test
    void shouldBuildClustersPropertiesWithDefaultOptions() {
        // When:
        provider.initialize(api);

        // Then:
        verify(propertiesFactory).create(components, DEFAULT_OPTIONS);
    }

    @Test
    void shouldBuildClustersPropertiesWithWithUserOptions() {
        // Given:
        when(api.options().get(ClientsExtensionOptions.class)).thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(propertiesFactory).create(components, userOptions);
    }

    @Test
    void shouldBuildSerdeProvidersWithDefaultOptions() {
        // Given:
        final ArgumentCaptor<InitializeParams> capture =
                ArgumentCaptor.forClass(InitializeParams.class);

        // When:
        provider.initialize(api);

        // Then:
        verify(serdeProvidersFactory).create(eq(api), capture.capture());

        final InitializeParams params = capture.getValue();

        // When:
        final Optional<?> result = params.typeOverride(String.class);

        // Then:
        assertThat(result, is(Optional.empty()));
    }

    @Test
    void shouldBuildSerdeProvidersWithWithUserOptions() {
        // Given:
        when(api.options().get(ClientsExtensionOptions.class)).thenReturn(Optional.of(userOptions));
        final ArgumentCaptor<InitializeParams> capture =
                ArgumentCaptor.forClass(InitializeParams.class);

        // When:
        provider.initialize(api);

        // Then:
        verify(serdeProvidersFactory).create(eq(api), capture.capture());

        final InitializeParams params = capture.getValue();

        // When:
        params.typeOverride(String.class);

        // Then:
        verify(userOptions.typeOverrides()).get(String.class);
    }

    @Test
    void shouldBuildExtension() {
        // When:
        provider.initialize(api);

        // Then:
        verify(extensionFactory).create(clustersProperties, resourceRegistry);
    }

    @Test
    void shouldReturnExtension() {
        // When:
        final KafkaClientsExtension result = provider.initialize(api);

        // Then:
        assertThat(result, is(sameInstance(clientsExtension)));
    }
}
