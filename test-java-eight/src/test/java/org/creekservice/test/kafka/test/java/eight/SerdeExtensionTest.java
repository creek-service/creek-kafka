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

package org.creekservice.test.kafka.test.java.eight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.creekservice.api.kafka.metadata.serde.NativeKafkaSerde;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProviders;
import org.creekservice.api.service.extension.CreekService;
import org.creekservice.test.api.kafka.serde.test.PublicTestSerdeProvider;
import org.creekservice.test.internal.kafka.serde.test.PrivateTestSerdeProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SerdeExtensionTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekService api;

    private KafkaSerdeProviders providers;

    @BeforeEach
    void setUp() {
        providers = KafkaSerdeProviders.create(api);
    }

    @Test
    void shouldFindKafkaFormat() {
        // When:
        final KafkaSerdeProvider.SerdeFactory actual = providers.get(NativeKafkaSerde.format());

        // Then:
        assertThat(actual, is(notNullValue()));
        assertThat(
                actual.getClass().getName(),
                is("org.creekservice.internal.kafka.serde.provider.NativeKafkaSerdeProvider$1"));
    }

    @Test
    void shouldFindPublicTestSerde() {
        // When:
        final KafkaSerdeProvider.SerdeFactory actual =
                providers.get(PublicTestSerdeProvider.FORMAT);

        // Then:
        assertThat(actual, is(notNullValue()));
        assertThat(
                actual.getClass().getName(),
                is("org.creekservice.test.api.kafka.serde.test.PublicTestSerdeProvider$1"));
    }

    @Test
    void shouldFindPrivateTestSerde() {
        // When:
        final KafkaSerdeProvider.SerdeFactory actual =
                providers.get(PrivateTestSerdeProvider.FORMAT);

        // Then:
        assertThat(actual, is(notNullValue()));
        assertThat(
                actual.getClass().getName(),
                is("org.creekservice.test.internal.kafka.serde.test.PrivateTestSerdeProvider$1"));
    }
}
