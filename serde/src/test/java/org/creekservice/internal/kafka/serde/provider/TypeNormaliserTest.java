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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.creekservice.internal.kafka.serde.provider.TypeNormaliser.NormalisationFailureException;
import org.junit.jupiter.api.Test;

class TypeNormaliserTest {

    private final TypeNormaliser normaliser = new TypeNormaliser();

    @Test
    void shouldNormaliseNulls() {
        assertThat(normaliser.normalise(null, String.class), is(nullValue()));
    }

    @Test
    void shouldHandleValuesOfCorrectType() {
        assertThat(normaliser.normalise("hello", String.class), is("hello"));
    }

    @Test
    void shouldHandleSubTypes() {
        assertThat(normaliser.normalise("hello", Object.class), is("hello"));
    }

    @Test
    void shouldThrowOnNonNormalisableType() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise("hello", AtomicBoolean.class));

        assertThat(
                e.getMessage(),
                is(
                        "Can not normalise java.lang.String"
                                + " to java.util.concurrent.atomic.AtomicBoolean"));
    }

    @Test
    void shouldThrowIfNoNormalisationRule() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise("hello", long.class));

        assertThat(e.getMessage(), is("Can not normalise java.lang.String to java.lang.Long"));
    }

    @Test
    void shouldNormaliseDecimalToInt() {
        assertThat(
                normaliser.normalise(new BigDecimal(Integer.MIN_VALUE), int.class),
                is(Integer.MIN_VALUE));
        assertThat(
                normaliser.normalise(new BigDecimal(Integer.MAX_VALUE), int.class),
                is(Integer.MAX_VALUE));
    }

    @Test
    void shouldFailToConvertDecimalToIntIfFractional() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(new BigDecimal("1.5"), int.class));

        assertThat(
                e.getMessage(),
                is("Number with fractional part can not be converted to" + " java.lang.Integer"));
    }

    @Test
    void shouldFailToConvertDecimalToIntIfBelowValidRange() {
        final BigDecimal belowMin = new BigDecimal(Integer.MIN_VALUE).subtract(BigDecimal.ONE);

        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(belowMin, int.class));

        assertThat(
                e.getMessage(),
                is(
                        "Number is below the range of values that can be held by a"
                                + " java.lang.Integer"));
    }

    @Test
    void shouldFailToConvertDecimalToIntIfAboveValidRange() {
        final BigDecimal aboveMax = new BigDecimal(Integer.MAX_VALUE).add(BigDecimal.ONE);

        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(aboveMax, int.class));

        assertThat(
                e.getMessage(),
                is(
                        "Number is above the range of values that can be held by a"
                                + " java.lang.Integer"));
    }

    @Test
    void shouldNormaliseIntToLong() {
        assertThat(normaliser.normalise(10, long.class), is(10L));
    }

    @Test
    void shouldNormaliseDecimalToLong() {
        assertThat(
                normaliser.normalise(new BigDecimal(Long.MIN_VALUE), long.class),
                is(Long.MIN_VALUE));
        assertThat(
                normaliser.normalise(new BigDecimal(Long.MAX_VALUE), long.class),
                is(Long.MAX_VALUE));
    }

    @Test
    void shouldFailToConvertDecimalToLongIfFractional() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(new BigDecimal("1.5"), long.class));

        assertThat(
                e.getMessage(),
                is("Number with fractional part can not be converted to" + " java.lang.Long"));
    }

    @Test
    void shouldFailToConvertDecimalToLongIfBelowValidRange() {
        final BigDecimal belowMin = new BigDecimal(Long.MIN_VALUE).subtract(BigDecimal.ONE);

        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(belowMin, long.class));

        assertThat(
                e.getMessage(),
                is(
                        "Number is below the range of values that can be held by a"
                                + " java.lang.Long"));
    }

    @Test
    void shouldFailToConvertDecimalToLongIfAboveValidRange() {
        final BigDecimal aboveMax = new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE);

        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(aboveMax, long.class));

        assertThat(
                e.getMessage(),
                is(
                        "Number is above the range of values that can be held by a"
                                + " java.lang.Long"));
    }

    @Test
    void shouldNormaliseIntToShort() {
        assertThat(normaliser.normalise(1, short.class), is((short) 1));
        assertThat(normaliser.normalise((int) Short.MIN_VALUE, short.class), is(Short.MIN_VALUE));
        assertThat(normaliser.normalise((int) Short.MAX_VALUE, short.class), is(Short.MAX_VALUE));
    }

    @Test
    void shouldFailToConvertIntToShortIfBelowValidRange() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(Short.MIN_VALUE - 1, short.class));

        assertThat(
                e.getMessage(),
                is("Number is below the range of values that can be held by a java.lang.Short"));
    }

    @Test
    void shouldFailToConvertIntToShortIfAboveValidRange() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(Short.MAX_VALUE + 1, short.class));

        assertThat(
                e.getMessage(),
                is("Number is above the range of values that can be held by a java.lang.Short"));
    }

    @Test
    void shouldNormaliseLongToShort() {
        assertThat(normaliser.normalise((long) Short.MIN_VALUE, short.class), is(Short.MIN_VALUE));
        assertThat(normaliser.normalise((long) Short.MAX_VALUE, short.class), is(Short.MAX_VALUE));
    }

    @Test
    void shouldFailToConvertLongToShortIfAboveValidRange() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise((long) Short.MAX_VALUE + 1, short.class));

        assertThat(
                e.getMessage(),
                is("Number is above the range of values that can be held by a java.lang.Short"));
    }

    @Test
    void shouldFailToConvertLongToShortIfBelowValidRange() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise((long) Short.MIN_VALUE - 1, short.class));

        assertThat(
                e.getMessage(),
                is("Number is below the range of values that can be held by a java.lang.Short"));
    }

    @Test
    void shouldNormaliseDecimalToShort() {
        assertThat(
                normaliser.normalise(new BigDecimal(Short.MIN_VALUE), short.class),
                is(Short.MIN_VALUE));
        assertThat(
                normaliser.normalise(new BigDecimal(Short.MAX_VALUE), short.class),
                is(Short.MAX_VALUE));
    }

    @Test
    void shouldFailToConvertDecimalToShortIfFractional() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(new BigDecimal("1.5"), short.class));

        assertThat(
                e.getMessage(),
                is("Number with fractional part can not be converted to java.lang.Short"));
    }

    @Test
    void shouldFailToConvertDecimalToShortIfOutOfRange() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () ->
                                normaliser.normalise(
                                        new BigDecimal(Short.MAX_VALUE + 1), short.class));

        assertThat(
                e.getMessage(),
                is("Number is above the range of values that can be held by a java.lang.Short"));
    }

    @Test
    void shouldNormaliseIntToFloat() {
        assertThat(normaliser.normalise(1, float.class), is(1.0f));
        assertThat(normaliser.normalise(0, float.class), is(0.0f));
    }

    @Test
    void shouldNormaliseDoubleToFloat() {
        assertThat(normaliser.normalise(1.5, float.class), is(1.5f));
        assertThat(normaliser.normalise(0.0, float.class), is(0.0f));
    }

    @Test
    void shouldNormaliseDoubleToFloatAtBoundaries() {
        assertThat(
                normaliser.normalise((double) Float.MAX_VALUE, float.class), is(Float.MAX_VALUE));
        assertThat(
                normaliser.normalise((double) -Float.MAX_VALUE, float.class), is(-Float.MAX_VALUE));
    }

    @Test
    void shouldFailToConvertDoubleToFloatIfAboveValidRange() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(Double.MAX_VALUE, float.class));

        assertThat(
                e.getMessage(),
                is("Number is outside the range of values that can be held by a java.lang.Float"));
    }

    @Test
    void shouldFailToConvertDoubleToFloatIfBelowValidRange() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise(-Double.MAX_VALUE, float.class));

        assertThat(
                e.getMessage(),
                is("Number is outside the range of values that can be held by a java.lang.Float"));
    }

    @Test
    void shouldNormaliseIntToDecimal() {
        assertThat(normaliser.normalise(10, BigDecimal.class), is(new BigDecimal(10)));
    }

    @Test
    void shouldNormaliseLongToDecimal() {
        assertThat(normaliser.normalise(10L, BigDecimal.class), is(new BigDecimal(10L)));
    }

    @Test
    void shouldNormaliseStringToUUID() {
        final UUID uuid = UUID.randomUUID();
        assertThat(normaliser.normalise(uuid.toString(), UUID.class), is(uuid));
    }

    @Test
    void shouldThrowIfStringCanNotBeConvertedToUUID() {
        final Exception e =
                assertThrows(
                        NormalisationFailureException.class,
                        () -> normaliser.normalise("not a uuid", UUID.class));

        assertThat(e.getMessage(), is("String can not be converted to UUID: not a uuid"));
    }
}
