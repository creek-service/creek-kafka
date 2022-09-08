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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.kafka.common.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.model.ComponentModelContainer;
import org.creekservice.api.service.extension.option.OptionCollection;
import org.creekservice.internal.kafka.common.resource.KafkaResourceValidator;
import org.creekservice.internal.kafka.streams.extension.config.ClustersPropertiesFactory;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

    @Mock private KafkaResourceValidator resourceValidator;
    @Mock private ServiceDescriptor component;
    @Mock private KafkaStreamsExtensionOptions userOptions;
    @Mock private ClustersPropertiesFactory propertiesFactory;
    @Mock private KafkaStreamsExtensionProvider.BuilderFactory builderFactory;
    @Mock private KafkaStreamsExtensionProvider.ExecutorFactory executorFactory;
    @Mock private KafkaStreamsExtensionProvider.ExtensionFactory extensionFactory;
    @Mock private ClustersProperties clustersProperties;
    @Mock private ResourceRegistryFactory resourceFactory;
    @Mock private KafkaStreamsBuilder streamsBuilder;
    @Mock private KafkaStreamsExecutor streamsExecutor;
    @Mock private StreamsExtension streamsExtension;
    @Mock private ResourceRegistry resources;
    @Mock private CreekService api;
    @Mock private OptionCollection options;
    @Mock private ComponentModelContainer model;
    private final List<ComponentDescriptor> validatedComponents = new ArrayList<>();

    private KafkaStreamsExtensionProvider provider;

    @BeforeEach
    void setUp() {
        validatedComponents.clear();

        provider =
                new KafkaStreamsExtensionProvider(
                        resourceValidator,
                        propertiesFactory,
                        builderFactory,
                        executorFactory,
                        extensionFactory,
                        resourceFactory);

        when(propertiesFactory.create(any(), any())).thenReturn(clustersProperties);
        when(builderFactory.create(any())).thenReturn(streamsBuilder);
        when(executorFactory.create(any())).thenReturn(streamsExecutor);
        when(resourceFactory.create(any(), any())).thenReturn(resources);
        when(extensionFactory.create(any(), any(), any(), any())).thenReturn(streamsExtension);

        when(api.options()).thenReturn(options);
        when(api.model()).thenReturn(model);
        when(api.service()).thenReturn(component);
    }

    @Test
    void shouldValidateResources() {
        // Given:
        doAnswer(trackValidatedComponents()).when(resourceValidator).validate(any());

        // When:
        provider.initialize(api);

        // Then:
        assertThat(validatedComponents, contains(component));
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
                        builderFactory,
                        executorFactory,
                        resourceFactory);
        inOrder.verify(resourceValidator).validate(any());
        inOrder.verify(propertiesFactory).create(any(), any());
        inOrder.verify(resourceFactory).create(any(), any());
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

    @Test
    void shouldHandleTopicResources() {
        // When:
        provider.initialize(api);

        // Then:
        verify(model).addResource(eq(KafkaTopicDescriptor.class), any());
    }

    @Test
    void shouldBuildClustersPropertiesWithDefaultOptions() {
        // When:
        provider.initialize(api);

        // Then:
        verify(propertiesFactory).create(component, DEFAULT_OPTIONS);
    }

    @Test
    void shouldBuildClustersPropertiesWithWithUserOptions() {
        // Given:
        when(options.get(KafkaStreamsExtensionOptions.class)).thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(propertiesFactory).create(component, userOptions);
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
        verify(resourceFactory).create(component, clustersProperties);
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
        when(options.get(KafkaStreamsExtensionOptions.class)).thenReturn(Optional.of(userOptions));

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
        final StreamsExtension result = provider.initialize(api);

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
