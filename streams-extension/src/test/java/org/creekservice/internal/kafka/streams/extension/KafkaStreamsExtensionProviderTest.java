/*
 * Copyright 2022 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.kafka.common.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.kafka.streams.extension.client.TopicClient;
import org.creekservice.api.platform.metadata.AggregateDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ResourceHandler;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.internal.kafka.common.resource.KafkaResourceValidator;
import org.creekservice.internal.kafka.streams.extension.config.ClustersPropertiesFactory;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistryFactory;
import org.creekservice.internal.kafka.streams.extension.resource.TopicResourceHandler;
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

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaStreamsExtensionProviderTest {

    public static final KafkaStreamsExtensionOptions DEFAULT_OPTIONS =
            KafkaStreamsExtensionOptions.builder().build();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekService api;

    @Mock private KafkaResourceValidator resourceValidator;
    @Mock private KafkaStreamsExtensionOptions userOptions;
    @Mock private ClustersPropertiesFactory propertiesFactory;
    @Mock private KafkaStreamsExtensionProvider.TopicClientFactory topicClientFactory;
    @Mock private KafkaStreamsExtensionProvider.BuilderFactory builderFactory;
    @Mock private KafkaStreamsExtensionProvider.ExecutorFactory executorFactory;
    @Mock private KafkaStreamsExtensionProvider.ExtensionFactory extensionFactory;
    @Mock private ClustersProperties clustersProperties;
    @Mock private ResourceRegistryFactory resourceFactory;
    @Mock private TopicClient topicClient;
    @Mock private KafkaStreamsBuilder streamsBuilder;
    @Mock private KafkaStreamsExecutor streamsExecutor;
    @Mock private StreamsExtension streamsExtension;
    @Mock private ResourceRegistry resources;

    private Collection<? extends ComponentDescriptor> components;

    private final List<ComponentDescriptor> validatedComponents = new ArrayList<>();

    private KafkaStreamsExtensionProvider provider;

    @BeforeEach
    void setUp() {
        components = List.of(mock(ServiceDescriptor.class), mock(AggregateDescriptor.class));
        validatedComponents.clear();

        provider =
                new KafkaStreamsExtensionProvider(
                        resourceValidator,
                        propertiesFactory,
                        topicClientFactory,
                        builderFactory,
                        executorFactory,
                        extensionFactory,
                        resourceFactory);

        when(propertiesFactory.create(any(), any())).thenReturn(clustersProperties);
        when(topicClientFactory.create(any())).thenReturn(topicClient);
        when(builderFactory.create(any())).thenReturn(streamsBuilder);
        when(executorFactory.create(any())).thenReturn(streamsExecutor);
        when(resourceFactory.create(any(), any())).thenReturn(resources);
        when(extensionFactory.create(any(), any(), any(), any())).thenReturn(streamsExtension);

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
        final InOrder inOrder =
                inOrder(
                        resourceValidator,
                        propertiesFactory,
                        topicClientFactory,
                        builderFactory,
                        executorFactory,
                        resourceFactory);

        inOrder.verify(resourceValidator).validate(any());

        inOrder.verify(propertiesFactory).create(any(), any());
        inOrder.verify(resourceFactory).create(any(), any());
        inOrder.verify(topicClientFactory).create(any());
        inOrder.verify(builderFactory).create(any());
        inOrder.verify(executorFactory).create(any());
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

    @SuppressWarnings({"RedundantCast", "unchecked", "rawtypes"})
    @Test
    void shouldHandleTopicResources() {
        // When:
        provider.initialize(api);

        // Then:
        verify(api.components().model())
                .addResource(
                        KafkaTopicDescriptor.class,
                        (ResourceHandler) new TopicResourceHandler(topicClient));
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
        when(api.options().get(KafkaStreamsExtensionOptions.class))
                .thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(propertiesFactory).create(components, userOptions);
    }

    @Test
    void shouldBuildTopicClient() {
        // When:
        provider.initialize(api);

        // Then:
        verify(topicClientFactory).create(clustersProperties);
    }

    @SuppressWarnings({"unchecked", "RedundantCast", "rawtypes"})
    @Test
    void shouldUseSuppliedTopicClient() {
        // Given:
        final TopicClient userClient = mock(TopicClient.class);
        when(userOptions.topicClient()).thenReturn(Optional.of(userClient));

        when(api.options().get(KafkaStreamsExtensionOptions.class))
                .thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(topicClientFactory, never()).create(any());
        verify(api.components().model())
                .addResource(
                        KafkaTopicDescriptor.class,
                        (ResourceHandler) new TopicResourceHandler(userClient));
    }

    @Test
    void shouldBuildStreamsBuilder() {
        // When:
        provider.initialize(api);

        // Then:
        verify(builderFactory).create(clustersProperties);
    }

    @Test
    void shouldBuildResources() {
        // When:
        provider.initialize(api);

        // Then:
        verify(resourceFactory).create(components, clustersProperties);
    }

    @Test
    void shouldBuildExecutorWithDefaultOptions() {
        // When:
        provider.initialize(api);

        // Then:
        verify(executorFactory).create(DEFAULT_OPTIONS);
    }

    @Test
    void shouldBuildExecutorWithUserOptions() {
        // Given:
        when(api.options().get(KafkaStreamsExtensionOptions.class))
                .thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(executorFactory).create(userOptions);
    }

    @Test
    void shouldBuildExtension() {
        // When:
        provider.initialize(api);

        // Then:
        verify(extensionFactory)
                .create(clustersProperties, resources, streamsBuilder, streamsExecutor);
    }

    @Test
    void shouldReturnExtension() {
        // When:
        final KafkaStreamsExtension result = provider.initialize(api);

        // Then:
        assertThat(result, is(sameInstance(streamsExtension)));
    }

    private Answer<Void> trackValidatedComponents() {
        return inv -> {
            final Stream<? extends ComponentDescriptor> s = inv.getArgument(0);
            s.forEachOrdered(validatedComponents::add);
            return null;
        };
    }
}
