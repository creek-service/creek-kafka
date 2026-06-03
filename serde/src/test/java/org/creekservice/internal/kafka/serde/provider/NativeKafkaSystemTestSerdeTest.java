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

package org.creekservice.internal.kafka.serde.provider;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicBoolean;
import org.creekservice.api.kafka.metadata.serde.NativeKafkaSerde;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
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
class NativeKafkaSystemTestSerdeTest {

    private static final String TOPIC = "test-topic";

    @Mock private PartDescriptor<String> stringPart;

    @Mock private PartDescriptor<Long> longPart;

    private NativeKafkaSystemTestSerdeProvider provider;
    private SystemTestSerde factory;

    @BeforeEach
    void setUp() {
        when(stringPart.type()).thenReturn(String.class);
        when(longPart.type()).thenReturn(Long.class);

        provider = new NativeKafkaSystemTestSerdeProvider();
        factory = provider.create();
    }

    @Test
    void shouldReturnNativeFormat() {
        assertThat(provider.format(), is(NativeKafkaSerde.format()));
    }

    @Test
    void shouldCreateFactory() {
        assertThat(factory, is(notNullValue()));
    }

    @Test
    void shouldSerializeString() {
        // When:
        final byte[] bytes = factory.serialize("hello", stringPart, TOPIC);
        final Object result = factory.deserialize(bytes, stringPart, TOPIC);

        // Then:
        assertThat(result, is("hello"));
    }

    @Test
    void shouldSerializeLong() {
        // When:
        final byte[] bytes = factory.serialize(42L, longPart, TOPIC);
        final Object result = factory.deserialize(bytes, longPart, TOPIC);

        // Then:
        assertThat(result, is(42L));
    }

    @Test
    void shouldSerializeWithTypeNormalisation() {
        // When:
        final byte[] bytes = factory.serialize(10, longPart, TOPIC);
        final Object result = factory.deserialize(bytes, longPart, TOPIC);

        // Then:
        assertThat(result, is(10L));
    }

    @Test
    void shouldDeserializeString() {
        // Given:
        final byte[] bytes = factory.serialize("hello", stringPart, TOPIC);

        // When:
        final Object result = factory.deserialize(bytes, stringPart, TOPIC);

        // Then:
        assertThat(result, is("hello"));
    }

    @Test
    void shouldDeserializeLong() {
        // Given:
        final byte[] bytes = factory.serialize(42L, longPart, TOPIC);

        // When:
        final Object result = factory.deserialize(bytes, longPart, TOPIC);

        // Then:
        assertThat(result, is(42L));
    }

    @Test
    void shouldDeserializeNull() {
        // When:
        final Object result = factory.deserialize(null, stringPart, TOPIC);

        // Then:
        assertThat(result, is(nullValue()));
    }

    @Test
    void shouldNormaliseIntToLong() {
        // When:
        final Object result = factory.normalise(10, longPart, TOPIC);

        // Then:
        assertThat(result, is(10L));
    }

    @Test
    void shouldNormalisePassthrough() {
        // When:
        final Object result = factory.normalise("hello", stringPart, TOPIC);

        // Then:
        assertThat(result, is("hello"));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldThrowForUnsupportedType() {
        // Given:
        final PartDescriptor part = stringPart;
        when(part.type()).thenReturn(AtomicBoolean.class);

        // When: value is already of the target type, so normalise passes through,
        // but serdeFor throws because AtomicBoolean is not a supported Kafka type.
        assertThrows(
                IllegalArgumentException.class,
                () -> factory.serialize(new AtomicBoolean(true), part, TOPIC));
    }
}
