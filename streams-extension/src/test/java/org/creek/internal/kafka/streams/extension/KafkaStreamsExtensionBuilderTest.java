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

package org.creek.internal.kafka.streams.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.creek.api.kafka.metadata.KafkaTopicDescriptor;
import org.creek.api.kafka.metadata.KafkaTopicInput;
import org.creek.api.kafka.metadata.KafkaTopicInternal;
import org.creek.api.kafka.metadata.KafkaTopicOutput;
import org.creek.api.kafka.metadata.OwnedKafkaTopicInput;
import org.creek.api.kafka.metadata.OwnedKafkaTopicOutput;
import org.creek.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creek.api.platform.metadata.ComponentDescriptor;
import org.creek.api.platform.metadata.ResourceDescriptor;
import org.creek.api.service.extension.CreekExtensionOptions;
import org.creek.internal.kafka.streams.extension.resource.ResourceRegistry;
import org.creek.internal.kafka.streams.extension.resource.ResourceRegistryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaStreamsExtensionBuilderTest {

    public static final KafkaStreamsExtensionOptions DEFAULT_OPTIONS =
            KafkaStreamsExtensionOptions.builder().build();
    private KafkaStreamsExtensionBuilder builder;
    @Mock private ComponentDescriptor component;
    @Mock private KafkaStreamsExtensionOptions userOptions;
    @Mock private KafkaStreamsExtensionBuilder.BuilderFactory builderFactory;
    @Mock private KafkaStreamsExtensionBuilder.ExecutorFactory executorFactory;
    @Mock private KafkaStreamsExtensionBuilder.ExtensionFactory extensionFactory;
    @Mock private ResourceRegistryFactory resourceFactory;
    @Mock private KafkaStreamsBuilder streamsBuilder;
    @Mock private KafkaStreamsExecutor streamsExecutor;
    @Mock private StreamsExtension streamsExtension;
    @Mock private ResourceRegistry resources;

    @BeforeEach
    void setUp() {
        builder =
                new KafkaStreamsExtensionBuilder(
                        builderFactory, executorFactory, extensionFactory, resourceFactory);

        when(builderFactory.create(any())).thenReturn(streamsBuilder);
        when(executorFactory.create(any())).thenReturn(streamsExecutor);
        when(resourceFactory.create(any(), any())).thenReturn(resources);
        when(extensionFactory.create(any(), any(), any(), any())).thenReturn(streamsExtension);
    }

    @Test
    void shouldReturnName() {
        assertThat(builder.name(), is("Kafka-streams"));
    }

    @ParameterizedTest
    @MethodSource("resourceTypes")
    void shouldHandleTopicResources(
            final Class<? extends KafkaTopicDescriptor<?, ?>> resourceType) {
        assertThat(builder.handles(mock(resourceType)), is(true));
    }

    @Test
    void shouldNotHandleOtherResourceTypes() {
        assertThat(builder.handles(mock(ResourceDescriptor.class)), is(false));
    }

    @Test
    void shouldReturnTrueToIndicateKafkaStreamsExtensionOptionsAreSupported() {
        assertThat(builder.with(mock(KafkaStreamsExtensionOptions.class)), is(true));
    }

    @Test
    void shouldReturnFalseToIndicateOtherOptionTypesAreNotSupported() {
        assertThat(builder.with(mock(CreekExtensionOptions.class)), is(false));
    }

    @Test
    void shouldThrowIfUserSetsOptionsTwice() {
        // Given:
        final KafkaStreamsExtensionOptions options = mock(KafkaStreamsExtensionOptions.class);
        builder.with(options);

        // Then:
        assertThrows(IllegalStateException.class, () -> builder.with(options));
    }

    @Test
    void shouldBuildBuilderWithDefaultOptions() {
        // When:
        builder.build(component);

        // Then:
        verify(builderFactory).create(DEFAULT_OPTIONS);
    }

    @Test
    void shouldBuildBuilderWithUserOptions() {
        // Given:
        builder.with(userOptions);

        // When:
        builder.build(component);

        // Then:
        verify(builderFactory).create(userOptions);
    }

    @Test
    void shouldBuildResourcesWithDefaultOptions() {
        // When:
        builder.build(component);

        // Then:
        verify(resourceFactory).create(component, DEFAULT_OPTIONS);
    }

    @Test
    void shouldBuildResourcesWithUserOptions() {
        // Given:
        builder.with(userOptions);

        // When:
        builder.build(component);

        // Then:
        verify(resourceFactory).create(component, userOptions);
    }

    @Test
    void shouldBuildExecutorWithDefaultOptions() {
        // When:
        builder.build(component);

        // Then:
        verify(executorFactory).create(DEFAULT_OPTIONS);
    }

    @Test
    void shouldBuildExecutorWithUserOptions() {
        // Given:
        builder.with(userOptions);

        // When:
        builder.build(component);

        // Then:
        verify(executorFactory).create(userOptions);
    }

    @Test
    void shouldBuildExtensionWithDefaultOptions() {
        // When:
        builder.build(component);

        // Then:
        verify(extensionFactory)
                .create(DEFAULT_OPTIONS, resources, streamsBuilder, streamsExecutor);
    }

    @Test
    void shouldBuildExtensionWithUserOptions() {
        // Given:
        builder.with(userOptions);

        // When:
        builder.build(component);

        // Then:
        verify(extensionFactory).create(userOptions, resources, streamsBuilder, streamsExecutor);
    }

    @Test
    void shouldReturnExtension() {
        // When:
        final StreamsExtension result = builder.build(component);

        // Then:
        assertThat(result, is(sameInstance(streamsExtension)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Stream<Class<? extends KafkaTopicDescriptor<Object, Object>>> resourceTypes() {
        return (Stream)
                Stream.of(
                        KafkaTopicInput.class,
                        KafkaTopicInternal.class,
                        KafkaTopicOutput.class,
                        OwnedKafkaTopicInput.class,
                        OwnedKafkaTopicOutput.class);
    }
}
