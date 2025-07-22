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

import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.base.type.Primitives;

/**
 * Coerce the values deserialized from test files to the types expected by topics.
 *
 * <p>Types deserialized from test files may not bo of the correct type, e.g. while a topic may have
 * a {@code long} key, small numbers used in test files will be deserialized as {@code Integer}.
 */
public final class TypeCoercer {

    private static final Map<Class<?>, Coercer<?>> COERCERS =
            Map.of(
                    Integer.class,
                    Coercer.coercerFor(Integer.class)
                            .withRule(
                                    BigDecimal.class,
                                    value ->
                                            tryCoerceDecimal(
                                                    Integer.class,
                                                    value,
                                                    new BigDecimal(Integer.MIN_VALUE),
                                                    new BigDecimal(Integer.MAX_VALUE),
                                                    BigDecimal::intValue)),
                    Long.class,
                    Coercer.coercerFor(Long.class)
                            .withRule(Integer.class, Integer::longValue)
                            .withRule(
                                    BigDecimal.class,
                                    value ->
                                            tryCoerceDecimal(
                                                    Long.class,
                                                    value,
                                                    new BigDecimal(Long.MIN_VALUE),
                                                    new BigDecimal(Long.MAX_VALUE),
                                                    BigDecimal::longValue)),
                    BigDecimal.class,
                    Coercer.coercerFor(BigDecimal.class)
                            .withRule(Integer.class, BigDecimal::new)
                            .withRule(Long.class, BigDecimal::new),
                    UUID.class,
                    Coercer.coercerFor(UUID.class)
                            .withRule(String.class, TypeCoercer::uuidFromString));

    /**
     * Coerce the supplied {@code value} to the supplied {@code targetType}.
     *
     * @param value the object to coerce.
     * @param targetType the type to coerce to.
     * @param <T> the type to coerce to.
     * @return the coerced value.
     */
    @SuppressWarnings("unchecked")
    public <T> T coerce(final Object value, final Class<T> targetType) {
        if (value == null) {
            return null;
        }

        if (targetType.isAssignableFrom(value.getClass())) {
            return (T) value;
        }

        return coercer(targetType).coerce(value);
    }

    @SuppressWarnings("unchecked")
    private <T> Coercer<T> coercer(final Class<T> target) {
        final Coercer<?> coercer = COERCERS.get(Primitives.box(target));
        if (coercer != null) {
            return (Coercer<T>) coercer;
        }

        return Coercer.coercerFor(target);
    }

    private static <T> T tryCoerceDecimal(
            final Class<T> target,
            final BigDecimal decimal,
            final BigDecimal min,
            final BigDecimal max,
            final Function<BigDecimal, T> mapper) {
        if (decimal.stripTrailingZeros().scale() > 0) {
            throw new CoercionFailureException(
                    "Number with fractional part can not be converted to " + target.getName());
        }

        if (decimal.compareTo(min) < 0) {
            throw new CoercionFailureException(
                    "Number is below the range of values that can be held by a "
                            + target.getName());
        }

        if (decimal.compareTo(max) > 0) {
            throw new CoercionFailureException(
                    "Number is above the range of values that can be held by a "
                            + target.getName());
        }

        return mapper.apply(decimal);
    }

    private static UUID uuidFromString(final String s) {
        try {
            return UUID.fromString(s);
        } catch (final Exception e) {
            throw new CoercionFailureException("String can not be converted to UUID: " + s);
        }
    }

    private static final class Coercer<T> {

        private final Class<T> targetType;
        private final Map<Class<?>, Function<?, T>> coercers;

        private Coercer(final Class<T> targetType, final Map<Class<?>, Function<?, T>> coercers) {
            this.targetType = requireNonNull(targetType, "targetType");
            this.coercers = Map.copyOf(coercers);
        }

        static <T> Coercer<T> coercerFor(final Class<T> targetType) {
            return new Coercer<>(targetType, Map.of());
        }

        <S> Coercer<T> withRule(final Class<S> instanceType, final Function<S, T> mapper) {
            final Map<Class<?>, Function<?, T>> coercers = new HashMap<>(this.coercers);
            coercers.put(instanceType, mapper);
            return new Coercer<>(targetType, coercers);
        }

        @SuppressWarnings("unchecked")
        public <S> T coerce(final S value) {
            final Function<S, T> mapper =
                    (Function<S, T>) coercers.getOrDefault(value.getClass(), this::unsupportedType);
            return mapper.apply(value);
        }

        private T unsupportedType(final Object value) {
            throw new CoercionFailureException(
                    "Can not coerce " + value.getClass().getName() + " to " + targetType.getName());
        }
    }

    @VisibleForTesting
    static final class CoercionFailureException extends RuntimeException {
        CoercionFailureException(final String msg) {
            super(msg);
        }
    }
}
