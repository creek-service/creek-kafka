/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider.SystemTestSerde;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SystemTestSerdeProvidersTest {

    private static final SerializationFormat FORMAT_A = serializationFormat("format-a");
    private static final SerializationFormat FORMAT_B = serializationFormat("format-b");

    @Mock private KafkaSystemTestSerdeProvider provider1;
    @Mock private KafkaSystemTestSerdeProvider provider2;
    @Mock private SystemTestSerde serde1;
    @Mock private SystemTestSerde serde2;
    @Mock private KafkaTopicDescriptor<?, ?> descriptor;
    @Mock private KafkaTopicDescriptor.PartDescriptor<?> keyPart;
    @Mock private KafkaTopicDescriptor.PartDescriptor<?> valuePart;

    @BeforeEach
    void setUp() {
        when(provider1.format()).thenReturn(FORMAT_A);
        when(provider1.create()).thenReturn(serde1);

        when(provider2.format()).thenReturn(FORMAT_B);
        when(provider2.create()).thenReturn(serde2);

        doReturn(keyPart).when(descriptor).key();
        doReturn(valuePart).when(descriptor).value();
        when(descriptor.name()).thenReturn("test-topic");
        when(keyPart.format()).thenReturn(FORMAT_A);
        when(valuePart.format()).thenReturn(FORMAT_A);
    }

    @Test
    void shouldCreateFromProviders() {
        // When:
        final SystemTestSerdeProviders providers =
                SystemTestSerdeProviders.create(List.of(provider1));

        // Then:
        assertThat(providers.get(descriptor), is(notNullValue()));
    }

    @Test
    void shouldCallCreateOnProvider() {
        // When:
        new SystemTestSerdeProviders(Map.of(FORMAT_A, provider1));

        // Then:
        verify(provider1).create();
    }

    @Test
    void shouldGetTopicForKnownFormat() {
        // Given:
        final SystemTestSerdeProviders providers =
                new SystemTestSerdeProviders(Map.of(FORMAT_A, provider1));

        // When:
        final TestKafkaTopic result = providers.get(descriptor);

        // Then:
        assertThat(result, is(notNullValue()));
        assertThat(result.descriptor(), is(descriptor));
    }

    @Test
    void shouldThrowOnUnknownFormat() {
        // Given:
        final SystemTestSerdeProviders providers =
                new SystemTestSerdeProviders(Map.of(FORMAT_A, provider1));
        when(keyPart.format()).thenReturn(serializationFormat("unknown"));

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> providers.get(descriptor));

        // Then:
        assertThat(e.getMessage(), containsString("Unknown serialization format"));
    }

    @Test
    void shouldThrowOnDuplicateFormat() {
        // Given:
        final KafkaSystemTestSerdeProvider duplicate = provider2;
        when(duplicate.format()).thenReturn(FORMAT_A);

        // When:
        final Exception e =
                assertThrows(
                        IllegalStateException.class,
                        () -> SystemTestSerdeProviders.create(List.of(provider1, duplicate)));

        // Then:
        assertThat(e.getMessage(), containsString("second system test serde provider"));
    }

    @Test
    void shouldHandleMultipleFormats() {
        // Given:
        when(keyPart.format()).thenReturn(FORMAT_A);
        when(valuePart.format()).thenReturn(FORMAT_B);

        // When:
        final SystemTestSerdeProviders providers =
                SystemTestSerdeProviders.create(List.of(provider1, provider2));

        // Then:
        assertThat(providers.get(descriptor), is(notNullValue()));
    }
}
