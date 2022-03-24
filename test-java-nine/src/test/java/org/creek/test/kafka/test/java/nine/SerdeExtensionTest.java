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

package org.creek.test.kafka.test.java.nine;

import static org.creek.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import org.creek.api.kafka.serde.NativeKafkaSerdeProvider;
import org.creek.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creek.api.kafka.serde.provider.KafkaSerdeProviders;
import org.creek.test.api.kafka.serde.test.PublicTestSerdeProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SerdeExtensionTest {

    private KafkaSerdeProviders providers;

    @BeforeEach
    void setUp() {
        providers = KafkaSerdeProviders.create();
    }

    @Test
    void shouldFindKafkaFormat() {
        assertThat(
                providers.get(NativeKafkaSerdeProvider.FORMAT),
                is(instanceOf(NativeKafkaSerdeProvider.class)));
    }

    @Test
    void shouldFindPublicTestSerde() {
        assertThat(
                providers.get(PublicTestSerdeProvider.FORMAT),
                is(instanceOf(PublicTestSerdeProvider.class)));
    }

    @Test
    void shouldFindPrivateTestSerde() {
        // When:
        final KafkaSerdeProvider result = providers.get(serializationFormat("test-private"));

        // Then:
        assertThat(result.getClass().getSimpleName(), is("PrivateTestSerdeProvider"));
    }
}
