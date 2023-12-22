/*
 * Copyright 2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.extension.resource;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.creekservice.api.kafka.extension.config.TypeOverrides;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProviders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClustersSerdeProvidersTest {

    private static final SerializationFormat FORMAT = serializationFormat("some-format");

    @Mock private TypeOverrides typeOverrides;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private KafkaSerdeProviders underlyingProviders;

    private ClustersSerdeProviders providers;

    @BeforeEach
    void setUp() {
        providers = new ClustersSerdeProviders(typeOverrides, underlyingProviders);
    }

    @Test
    void shouldThrowOnUnknownFormat() {
        // Given:
        final RuntimeException expected = new RuntimeException("Boom");
        when(underlyingProviders.get(any())).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> providers.get(FORMAT, "bob"));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldThrowIfInitializationFails() {
        // Given:
        final RuntimeException expected = new RuntimeException("Boom");
        when(underlyingProviders.get(any()).initialize(any(), any())).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> providers.get(FORMAT, "bob"));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldGetByFormat() {
        // When:
        providers.get(FORMAT, "bob");

        // Then:
        verify(underlyingProviders).get(FORMAT);
    }

    @Test
    void shouldInitializeWithCorrectParams() {
        // When:
        providers.get(FORMAT, "bob");

        // Then:
        final ArgumentCaptor<KafkaSerdeProvider.InitializeParams> captor =
                ArgumentCaptor.forClass(KafkaSerdeProvider.InitializeParams.class);
        verify(underlyingProviders.get(FORMAT)).initialize(eq("bob"), captor.capture());

        // When:
        captor.getValue().typeOverride(String.class);

        // Then:
        verify(typeOverrides).get(String.class);
    }

    @Test
    void shouldReturnInitialized() {
        // When:
        final KafkaSerdeProvider.SerdeProvider result = providers.get(FORMAT, "bob");

        // Then:
        assertThat(result, is(notNullValue()));
    }

    @Test
    void shouldInitializeOnlyOncePerClusterAndFormat() {
        // Given:
        final KafkaSerdeProvider.SerdeProvider first = providers.get(FORMAT, "bob");

        // When:
        final KafkaSerdeProvider.SerdeProvider second = providers.get(FORMAT, "bob");

        // Then:
        verify(underlyingProviders, times(1)).get(any());
        verify(underlyingProviders.get(any()), times(1)).initialize(any(), any());
        assertThat(second, is(first));
    }

    @Test
    void shouldInitializePerFormat() {
        // Given:
        final SerializationFormat otherFormat = serializationFormat("diff");
        final KafkaSerdeProvider.SerdeProvider first = providers.get(FORMAT, "bob");

        // When:
        final KafkaSerdeProvider.SerdeProvider second = providers.get(otherFormat, "bob");

        // Then:
        verify(underlyingProviders.get(FORMAT)).initialize(any(), any());
        verify(underlyingProviders.get(otherFormat)).initialize(any(), any());
        assertThat(second, is(not(first)));
    }

    @Test
    void shouldInitializePerCluster() {
        // Given:
        final KafkaSerdeProvider.SerdeProvider first = providers.get(FORMAT, "bob");

        // When:
        final KafkaSerdeProvider.SerdeProvider second = providers.get(FORMAT, "diff");

        // Then:
        verify(underlyingProviders.get(any()), times(2)).initialize(any(), any());
        assertThat(second, is(not(first)));
    }
}
