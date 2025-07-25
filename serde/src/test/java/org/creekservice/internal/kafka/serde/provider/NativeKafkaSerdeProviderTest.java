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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.creekservice.api.kafka.metadata.serde.NativeKafkaSerde;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.kafka.serde.test.KafkaSerdeProviderTester;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class NativeKafkaSerdeProviderTest {

    private KafkaSerdeProvider.SerdeFactory serdeFactory;

    @BeforeEach
    void setUp() {
        serdeFactory = new NativeKafkaSerdeProvider().initialize(null);
    }

    @Test
    void shouldBeValid() {
        KafkaSerdeProviderTester.tester(NativeKafkaSerdeProvider.class)
                .withExpectedFormat(serializationFormat("kafka"))
                .test();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("supportedTypesAndExpectedSerde")
    void shouldReturnExpectedSerde(
            final Class<?> type, final Class<? extends Serde<?>> expectedSerdeType) {
        // Given:
        final PartDescriptor<?> part = partWithType(type);

        // When:
        final Serde<?> serde = serdeFactory.createSerde(part);

        // Then:
        assertThat(serde, is(instanceOf(expectedSerdeType)));
    }

    @SuppressWarnings("resource")
    @Test
    void shouldThrowOnUnsupportedType() {
        // Given:
        final PartDescriptor<AtomicInteger> part = partWithType(AtomicInteger.class);

        // When:
        final Exception e =
                assertThrows(IllegalArgumentException.class, () -> serdeFactory.createSerde(part));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "The supplied type is not supported by the kafka format: "
                                + AtomicInteger.class.getName()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("supportedTypes")
    void shouldBeKeptInlineWithNativeKafkaSerde(final Class<?> type) {
        assertThat(NativeKafkaSerde.supports(type), is(true));
    }

    public static Stream<Arguments> supportedTypesAndExpectedSerde() {
        return Stream.of(
                arguments(UUID.class, Serdes.UUIDSerde.class),
                arguments(long.class, Serdes.LongSerde.class),
                arguments(Long.class, Serdes.LongSerde.class),
                arguments(int.class, Serdes.IntegerSerde.class),
                arguments(Integer.class, Serdes.IntegerSerde.class),
                arguments(short.class, Serdes.ShortSerde.class),
                arguments(Short.class, Serdes.ShortSerde.class),
                arguments(float.class, Serdes.FloatSerde.class),
                arguments(Float.class, Serdes.FloatSerde.class),
                arguments(double.class, Serdes.DoubleSerde.class),
                arguments(Double.class, Serdes.DoubleSerde.class),
                arguments(String.class, Serdes.StringSerde.class),
                arguments(ByteBuffer.class, Serdes.ByteBufferSerde.class),
                arguments(Bytes.class, Serdes.BytesSerde.class),
                arguments(byte[].class, Serdes.ByteArraySerde.class),
                arguments(Void.class, Serdes.VoidSerde.class));
    }

    public static Stream<Class<?>> supportedTypes() {
        return NativeKafkaSerdeProvider.SUPPLIERS.keySet().stream();
    }

    @SuppressWarnings("unchecked")
    private <T> PartDescriptor<T> partWithType(final Class<T> type) {
        final PartDescriptor<T> part = mock(PartDescriptor.class);
        when(part.type()).thenReturn(type);
        return part;
    }
}
