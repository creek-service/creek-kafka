/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.provider;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ProviderLoaderTest {

    private static final SerializationFormat FORMAT_A = serializationFormat("A");
    private static final SerializationFormat FORMAT_B = serializationFormat("B");

    @Mock private ProviderLoader.LoaderFactory factory;
    @Mock private ServiceLoader<KafkaSerdeProvider> loaded;

    @Mock(name = "providerA")
    private KafkaSerdeProvider providerA;

    @Mock(name = "providerB")
    private KafkaSerdeProvider providerB;

    private ProviderLoader loader;

    @BeforeEach
    void setUp() {
        loader = new ProviderLoader(factory);

        when(factory.load(KafkaSerdeProvider.class)).thenReturn(loaded);
        when(providerA.format()).thenReturn(FORMAT_A);
        when(providerB.format()).thenReturn(FORMAT_B);

        final ServiceLoader.Provider<KafkaSerdeProvider> serviceProviderA =
                serviceProvider(providerA);
        final ServiceLoader.Provider<KafkaSerdeProvider> serviceProviderB =
                serviceProvider(providerB);
        when(loaded.stream()).thenReturn(Stream.of(serviceProviderA, serviceProviderB));
    }

    @Test
    void shouldLookupSerdeProviders() {
        // When:
        loader.load();

        // Then:
        verify(factory).load(KafkaSerdeProvider.class);
    }

    @Test
    void shouldReturnEmptyMapIfNoProviders() {
        // Given:
        when(loaded.stream()).thenReturn(Stream.of());

        // When:
        final Map<SerializationFormat, KafkaSerdeProvider> result = loader.load();

        // Then:
        assertThat(result.entrySet(), is(empty()));
    }

    @Test
    void shouldReturnMapOfProviders() {
        // When:
        final Map<SerializationFormat, KafkaSerdeProvider> result = loader.load();

        // Then:
        assertThat(
                result, is(Map.of(providerA.format(), providerA, providerB.format(), providerB)));
    }

    @Test
    void shouldThrowIfToProvidersHandleTheSameFormat() {
        // Given:
        when(providerB.format()).thenReturn(FORMAT_A);

        // When:
        final Exception e = assertThrows(IllegalStateException.class, loader::load);

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Encountered a second Kafka serde provider for a previously seen format"));
        assertThat(e.getMessage(), containsString(" format=" + FORMAT_A));
        assertThat(e.getMessage(), containsString("providerA"));
        assertThat(e.getMessage(), containsString("providerB"));
    }

    @SuppressWarnings("unchecked")
    private static ServiceLoader.Provider<KafkaSerdeProvider> serviceProvider(
            final KafkaSerdeProvider serdeProvider) {
        final ServiceLoader.Provider<KafkaSerdeProvider> provider =
                mock(ServiceLoader.Provider.class);
        when(provider.get()).thenReturn(serdeProvider);
        return provider;
    }
}
