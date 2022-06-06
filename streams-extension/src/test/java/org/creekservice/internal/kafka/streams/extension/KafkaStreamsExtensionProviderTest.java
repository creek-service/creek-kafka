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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.model.ModelContainer;
import org.creekservice.api.service.extension.option.OptionCollection;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaStreamsExtensionProviderTest {

    public static final KafkaStreamsExtensionOptions DEFAULT_OPTIONS =
            KafkaStreamsExtensionOptions.builder().build();
    private KafkaStreamsExtensionProvider provider;
    @Mock private ServiceDescriptor component;
    @Mock private KafkaStreamsExtensionOptions userOptions;
    @Mock private KafkaStreamsExtensionProvider.BuilderFactory builderFactory;
    @Mock private KafkaStreamsExtensionProvider.ExecutorFactory executorFactory;
    @Mock private KafkaStreamsExtensionProvider.ExtensionFactory extensionFactory;
    @Mock private ResourceRegistryFactory resourceFactory;
    @Mock private KafkaStreamsBuilder streamsBuilder;
    @Mock private KafkaStreamsExecutor streamsExecutor;
    @Mock private StreamsExtension streamsExtension;
    @Mock private ResourceRegistry resources;
    @Mock private CreekService api;
    @Mock private OptionCollection options;
    @Mock private ModelContainer model;

    @BeforeEach
    void setUp() {
        provider =
                new KafkaStreamsExtensionProvider(
                        builderFactory, executorFactory, extensionFactory, resourceFactory);

        when(builderFactory.create(any())).thenReturn(streamsBuilder);
        when(executorFactory.create(any())).thenReturn(streamsExecutor);
        when(resourceFactory.create(any(), any())).thenReturn(resources);
        when(extensionFactory.create(any(), any(), any(), any())).thenReturn(streamsExtension);

        when(api.options()).thenReturn(options);
        when(api.model()).thenReturn(model);
        when(api.service()).thenReturn(component);
    }

    @Test
    void shouldHandleTopicResources() {
        // When:
        provider.initialize(api);

        // Then:
        verify(model).addResource(eq(KafkaTopicDescriptor.class), any());
    }

    @Test
    void shouldBuildStreamsBuilderWithDefaultOptions() {
        // When:
        provider.initialize(api);

        // Then:
        verify(builderFactory).create(DEFAULT_OPTIONS);
    }

    @Test
    void shouldBuildStreamsBuilderWithUserOptions() {
        // Given:
        when(options.get(KafkaStreamsExtensionOptions.class)).thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(builderFactory).create(userOptions);
    }

    @Test
    void shouldBuildResourcesWithDefaultOptions() {
        // When:
        provider.initialize(api);

        // Then:
        verify(resourceFactory).create(component, DEFAULT_OPTIONS);
    }

    @Test
    void shouldBuildResourcesWithUserOptions() {
        // Given:
        when(options.get(KafkaStreamsExtensionOptions.class)).thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(resourceFactory).create(component, userOptions);
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
    void shouldBuildExtensionWithDefaultOptions() {
        // When:
        provider.initialize(api);

        // Then:
        verify(extensionFactory)
                .create(DEFAULT_OPTIONS, resources, streamsBuilder, streamsExecutor);
    }

    @Test
    void shouldBuildExtensionWithUserOptions() {
        // Given:
        when(options.get(KafkaStreamsExtensionOptions.class)).thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(extensionFactory).create(userOptions, resources, streamsBuilder, streamsExecutor);
    }

    @Test
    void shouldReturnExtension() {
        // When:
        final StreamsExtension result = provider.initialize(api);

        // Then:
        assertThat(result, is(sameInstance(streamsExtension)));
    }
}
