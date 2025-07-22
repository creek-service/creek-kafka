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

package org.creekservice.internal.kafka.streams.test.extension.yaml;

import static java.math.BigDecimal.ONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.creekservice.internal.kafka.streams.test.extension.yaml.TypeCoercer.CoercionFailureException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TypeCoercerTest {

    private TypeCoercer coercer;

    @BeforeEach
    void setUp() {
        coercer = new TypeCoercer();
    }

    @Test
    void shouldCoerceNulls() {
        assertThat(coercer.coerce(null, Long.class), is(nullValue()));
        assertThat(coercer.coerce(null, String.class), is(nullValue()));
    }

    @Test
    void shouldHandleValuesOfCorrectType() {
        assertThat(coercer.coerce(10L, Long.class), is(10L));
        assertThat(coercer.coerce("text", String.class), is("text"));
    }

    @Test
    void shouldHandleSubTypes() {
        assertThat(coercer.coerce(10L, Number.class), is(10L));
    }

    @Test
    void shouldThrowOnNonCoercibleType() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class,
                        () -> coercer.coerce("text", AtomicBoolean.class));

        // Then:
        assertThat(
                e.getMessage(),
                is("Can not coerce java.lang.String to java.util.concurrent.atomic.AtomicBoolean"));
    }

    @Test
    void shouldThrowIfOnCoercionRule() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class, () -> coercer.coerce("text", long.class));

        // Then:
        assertThat(e.getMessage(), is("Can not coerce java.lang.String to java.lang.Long"));
    }

    @Test
    void shouldCoerceDecimalToInt() {
        assertThat(
                coercer.coerce(new BigDecimal(Integer.MIN_VALUE), int.class),
                is(Integer.MIN_VALUE));
        assertThat(
                coercer.coerce(new BigDecimal(Integer.MAX_VALUE), Integer.class),
                is(Integer.MAX_VALUE));
    }

    @Test
    void shouldFailToConvertDecimalToIntIfFractional() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class,
                        () -> coercer.coerce(new BigDecimal("1.01"), int.class));

        // Then:
        assertThat(
                e.getMessage(),
                is("Number with fractional part can not be converted to java.lang.Integer"));
    }

    @Test
    void shouldFailToConvertDecimalToIntIfBelowValidRange() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class,
                        () ->
                                coercer.coerce(
                                        new BigDecimal(Integer.MIN_VALUE).subtract(ONE),
                                        int.class));

        // Then:
        assertThat(
                e.getMessage(),
                is("Number is below the range of values that can be held by a java.lang.Integer"));
    }

    @Test
    void shouldFailToConvertDecimalToIntIfAboveValidRange() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class,
                        () ->
                                coercer.coerce(
                                        new BigDecimal(Integer.MAX_VALUE).add(ONE), int.class));

        // Then:
        assertThat(
                e.getMessage(),
                is("Number is above the range of values that can be held by a java.lang.Integer"));
    }

    @Test
    void shouldCoerceIntToLong() {
        assertThat(coercer.coerce(10, long.class), is(10L));
        assertThat(coercer.coerce(-1937575, Long.class), is(-1937575L));
    }

    @Test
    void shouldCoerceDecimalToLong() {
        assertThat(coercer.coerce(new BigDecimal(Long.MIN_VALUE), long.class), is(Long.MIN_VALUE));
        assertThat(coercer.coerce(new BigDecimal(Long.MAX_VALUE), Long.class), is(Long.MAX_VALUE));
    }

    @Test
    void shouldFailToConvertDecimalToLongIfFractional() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class,
                        () -> coercer.coerce(new BigDecimal("1.01"), long.class));

        // Then:
        assertThat(
                e.getMessage(),
                is("Number with fractional part can not be converted to java.lang.Long"));
    }

    @Test
    void shouldFailToConvertDecimalToLongIfBelowValidRange() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class,
                        () ->
                                coercer.coerce(
                                        new BigDecimal(Long.MIN_VALUE).subtract(ONE), long.class));

        // Then:
        assertThat(
                e.getMessage(),
                is("Number is below the range of values that can be held by a java.lang.Long"));
    }

    @Test
    void shouldFailToConvertDecimalToLongIfAboveValidRange() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class,
                        () -> coercer.coerce(new BigDecimal(Long.MAX_VALUE).add(ONE), long.class));

        // Then:
        assertThat(
                e.getMessage(),
                is("Number is above the range of values that can be held by a java.lang.Long"));
    }

    @Test
    void shouldCoerceIntToDecimal() {
        assertThat(coercer.coerce(10, BigDecimal.class), is(new BigDecimal("10")));
    }

    @Test
    void shouldCoerceLongToDecimal() {
        assertThat(coercer.coerce(10L, BigDecimal.class), is(new BigDecimal("10")));
    }

    @Test
    void shouldCoerceStringToUUID() {
        // Given:
        final UUID uuid = UUID.randomUUID();

        // Then:
        assertThat(coercer.coerce(uuid.toString(), UUID.class), is(uuid));
    }

    @Test
    void shouldThrowIfStringCanNotBeConvertedToUUID() {
        // When:
        final Exception e =
                assertThrows(
                        CoercionFailureException.class,
                        () -> coercer.coerce("not a uuid", UUID.class));

        // Then:
        assertThat(e.getMessage(), is("String can not be converted to UUID: not a uuid"));
    }
}
