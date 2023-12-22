/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.platform.metadata.AggregateDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.component.model.ComponentModelContainer.HandlerTypeRef;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.extension.config.ClustersPropertiesFactory;
import org.creekservice.internal.kafka.extension.resource.KafkaResourceValidator;
import org.creekservice.internal.kafka.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.extension.resource.TopicResourceHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Answer;

@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaClientsExtensionProviderTest {

    public static final KafkaClientsExtensionOptions DEFAULT_OPTIONS =
            KafkaClientsExtensionOptions.builder().build();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekService api;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ClientsExtensionOptions userOptions;

    @Mock private KafkaResourceValidator resourceValidator;
    @Mock private ClustersPropertiesFactory propertiesFactory;
    @Mock private KafkaClientsExtensionProvider.HandlerFactory handlerFactory;
    @Mock private KafkaClientsExtensionProvider.ExtensionFactory extensionFactory;
    @Mock private ClustersProperties clustersProperties;
    @Mock private ResourceRegistry resourceRegistry;
    @Mock private ClientsExtension clientsExtension;
    @Mock private TopicResourceHandler topicHandler;

    private Collection<? extends ComponentDescriptor> components;

    private final List<ComponentDescriptor> validatedComponents = new ArrayList<>();

    private KafkaClientsExtensionProvider provider;

    @BeforeEach
    void setUp() {
        components = List.of(mock(ServiceDescriptor.class), mock(AggregateDescriptor.class));
        validatedComponents.clear();

        provider =
                new KafkaClientsExtensionProvider(
                        resourceValidator,
                        propertiesFactory,
                        resourceRegistry,
                        handlerFactory,
                        extensionFactory);

        when(propertiesFactory.create(any(), any())).thenReturn(clustersProperties);
        when(handlerFactory.create(any(), any(), any())).thenReturn(topicHandler);
        when(extensionFactory.create(any(), any())).thenReturn(clientsExtension);

        when(api.options().get(any())).thenReturn(Optional.empty());
        when(api.components().descriptors().stream()).thenAnswer(inv -> components.stream());
    }

    @Test
    void shouldValidateResources() {
        // Given:
        doAnswer(trackValidatedComponents()).when(resourceValidator).validate(any());

        // When:
        provider.initialize(api);

        // Then:
        assertThat(validatedComponents, is(components));
    }

    @Test
    void shouldValidateResourcesFirst() {
        // When:
        provider.initialize(api);

        // Then:
        final InOrder inOrder = inOrder(resourceValidator, propertiesFactory, handlerFactory);

        inOrder.verify(resourceValidator).validate(any());
        inOrder.verify(propertiesFactory).create(any(), any());
        inOrder.verify(handlerFactory).create(any(), any(), any());
    }

    @Test
    void shouldThrowIfValidatorThrows() {
        // Given:
        final IllegalArgumentException cause = new IllegalArgumentException("Boom");
        doThrow(cause).when(resourceValidator).validate(any());

        // When:
        final Exception e =
                assertThrows(IllegalArgumentException.class, () -> provider.initialize(api));

        // Then:
        assertThat(e, is(sameInstance(cause)));
    }

    @Test
    void shouldCreateTopicHandlerWithDefaultOverrides() {
        // When:
        provider.initialize(api);

        // Then:
        verify(handlerFactory)
                .create(DEFAULT_OPTIONS.typeOverrides(), resourceRegistry, clustersProperties);
    }

    @Test
    void shouldCreateTopicHandlerWithCustomOverrides() {
        // Given:
        when(api.options().get(ClientsExtensionOptions.class)).thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(handlerFactory)
                .create(userOptions.typeOverrides(), resourceRegistry, clustersProperties);
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

    private Answer<Void> trackValidatedComponents() {
        return inv -> {
            final Stream<? extends ComponentDescriptor> s = inv.getArgument(0);
            s.forEachOrdered(validatedComponents::add);
            return null;
        };
    }
}
