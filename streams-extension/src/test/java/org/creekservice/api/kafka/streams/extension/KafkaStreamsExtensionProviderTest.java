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

package org.creekservice.api.kafka.streams.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionProvider;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.api.service.extension.extension.ExtensionContainer;
import org.creekservice.internal.kafka.streams.extension.KafkaStreamsBuilder;
import org.creekservice.internal.kafka.streams.extension.KafkaStreamsExecutor;
import org.creekservice.internal.kafka.streams.extension.StreamsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaStreamsExtensionProviderTest {

    public static final KafkaStreamsExtensionOptions DEFAULT_OPTIONS =
            KafkaStreamsExtensionOptions.builder().build();

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekService api;

    @Mock private ExtensionContainer extensions;
    @Mock private KafkaStreamsExtensionOptions userOptions;
    @Mock private KafkaStreamsExtensionProvider.BuilderFactory builderFactory;
    @Mock private KafkaStreamsExtensionProvider.ExecutorFactory executorFactory;
    @Mock private KafkaStreamsExtensionProvider.ExtensionFactory extensionFactory;
    @Mock private KafkaStreamsBuilder streamsBuilder;
    @Mock private KafkaStreamsExecutor streamsExecutor;
    @Mock private StreamsExtension streamsExtension;
    @Mock private KafkaClientsExtension clientsExtension;

    private KafkaStreamsExtensionProvider provider;

    @BeforeEach
    void setUp() {
        provider =
                new KafkaStreamsExtensionProvider(
                        builderFactory, executorFactory, extensionFactory);

        when(builderFactory.create(any())).thenReturn(streamsBuilder);
        when(executorFactory.create(any())).thenReturn(streamsExecutor);
        when(extensionFactory.create(any(), any(), any())).thenReturn(streamsExtension);

        when(api.options().get(any())).thenReturn(Optional.empty());
        when(api.extensions()).thenReturn(extensions);
        doReturn(clientsExtension)
                .when(extensions)
                .ensureExtension(KafkaClientsExtensionProvider.class);
    }

    @Test
    void shouldThrowIfUserUsesClientsOptions() {
        // Given:
        final KafkaClientsExtensionOptions clientOptions = mock(KafkaClientsExtensionOptions.class);
        when(api.options().get(KafkaClientsExtensionOptions.class))
                .thenReturn(Optional.of(clientOptions));

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> provider.initialize(api));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "KafkaClientsExtensionOptions detected: use KafkaStreamsExtensionOptions"
                                + " for streams based apps"));
    }

    @Test
    void shouldAddDefaultStreamsPropertiesToDefaultOptions() {
        // When:
        provider.initialize(api);

        // Then:
        verify(executorFactory).create(DEFAULT_OPTIONS);
    }

    @Test
    void shouldAddDefaultStreamsPropertiesToUserOptions() {
        // Given:
        when(api.options().get(KafkaStreamsExtensionOptions.class))
                .thenReturn(Optional.of(userOptions));

        // When:
        provider.initialize(api);

        // Then:
        verify(executorFactory).create(userOptions);
    }

    @Test
    void shouldBuildStreamsBuilder() {
        // When:
        provider.initialize(api);

        // Then:
        verify(builderFactory).create(clientsExtension);
    }

    @Test
    void shouldBuildExtension() {
        // When:
        provider.initialize(api);

        // Then:
        verify(extensionFactory).create(clientsExtension, streamsBuilder, streamsExecutor);
    }

    @Test
    void shouldReturnExtension() {
        // When:
        final KafkaStreamsExtension result = provider.initialize(api);

        // Then:
        assertThat(result, is(sameInstance(streamsExtension)));
    }
}
