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

package org.creekservice.api.kafka.serde.provider;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaSerdeProvidersTest {

    private static final SerializationFormat FORMAT = serializationFormat("A");

    @Mock private KafkaSerdeProvider provider;
    private KafkaSerdeProviders providers;

    @BeforeEach
    void setUp() {
        providers = new KafkaSerdeProviders(Map.of(FORMAT, provider));
    }

    @Test
    void shouldGetProvider() {
        assertThat(providers.get(FORMAT), is(provider));
    }

    @Test
    void shouldThrowOnUnknownFormat() {
        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> providers.get(serializationFormat("unknown")));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Unknown serialization format. Are you missing a runtime serde jar?"
                                + " format=unknown"));
    }
}
