/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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

import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.base.type.Primitives;

/**
 * Normalise values deserialized from test files to the types expected by topics.
 *
 * <p>Types deserialized from test files may not be of the correct type, e.g. while a topic may have
 * a {@code long} key, small numbers used in test files will be deserialized as {@code Integer}.
 */
public final class TypeNormaliser {

    private static final Map<Class<?>, Normaliser<?>> NORMALISERS =
            Map.of(
                    Short.class,
                    Normaliser.normaliserFor(Short.class)
                            .withRule(
                                    Integer.class,
                                    value ->
                                            tryNormaliseDecimal(
                                                    Short.class,
                                                    new BigDecimal(value),
                                                    new BigDecimal(Short.MIN_VALUE),
                                                    new BigDecimal(Short.MAX_VALUE),
                                                    BigDecimal::shortValue))
                            .withRule(
                                    Long.class,
                                    value ->
                                            tryNormaliseDecimal(
                                                    Short.class,
                                                    new BigDecimal(value),
                                                    new BigDecimal(Short.MIN_VALUE),
                                                    new BigDecimal(Short.MAX_VALUE),
                                                    BigDecimal::shortValue))
                            .withRule(
                                    BigDecimal.class,
                                    value ->
                                            tryNormaliseDecimal(
                                                    Short.class,
                                                    value,
                                                    new BigDecimal(Short.MIN_VALUE),
                                                    new BigDecimal(Short.MAX_VALUE),
                                                    BigDecimal::shortValue)),
                    Integer.class,
                    Normaliser.normaliserFor(Integer.class)
                            .withRule(
                                    BigDecimal.class,
                                    value ->
                                            tryNormaliseDecimal(
                                                    Integer.class,
                                                    value,
                                                    new BigDecimal(Integer.MIN_VALUE),
                                                    new BigDecimal(Integer.MAX_VALUE),
                                                    BigDecimal::intValue)),
                    Long.class,
                    Normaliser.normaliserFor(Long.class)
                            .withRule(Integer.class, Integer::longValue)
                            .withRule(
                                    BigDecimal.class,
                                    value ->
                                            tryNormaliseDecimal(
                                                    Long.class,
                                                    value,
                                                    new BigDecimal(Long.MIN_VALUE),
                                                    new BigDecimal(Long.MAX_VALUE),
                                                    BigDecimal::longValue)),
                    Float.class,
                    Normaliser.normaliserFor(Float.class)
                            .withRule(Integer.class, Integer::floatValue)
                            .withRule(Double.class, TypeNormaliser::tryNormaliseFloat),
                    BigDecimal.class,
                    Normaliser.normaliserFor(BigDecimal.class)
                            .withRule(Integer.class, BigDecimal::new)
                            .withRule(Long.class, BigDecimal::new),
                    UUID.class,
                    Normaliser.normaliserFor(UUID.class)
                            .withRule(String.class, TypeNormaliser::uuidFromString));

    /**
     * Normalise the supplied {@code value} to the supplied {@code targetType}.
     *
     * @param value the object to normalise.
     * @param targetType the type to normalise to.
     * @param <T> the type to normalise to.
     * @return the normalised value.
     */
    @SuppressWarnings("unchecked")
    public <T> T normalise(final Object value, final Class<T> targetType) {
        if (value == null) {
            return null;
        }

        if (targetType.isAssignableFrom(value.getClass())) {
            return (T) value;
        }

        return normaliser(targetType).normalise(value);
    }

    @SuppressWarnings("unchecked")
    private <T> Normaliser<T> normaliser(final Class<T> target) {
        final Normaliser<?> normaliser = NORMALISERS.get(Primitives.box(target));
        if (normaliser != null) {
            return (Normaliser<T>) normaliser;
        }

        return Normaliser.normaliserFor(target);
    }

    private static <T> T tryNormaliseDecimal(
            final Class<T> target,
            final BigDecimal decimal,
            final BigDecimal min,
            final BigDecimal max,
            final Function<BigDecimal, T> mapper) {
        if (decimal.stripTrailingZeros().scale() > 0) {
            throw new NormalisationFailureException(
                    "Number with fractional part can not be converted to " + target.getName());
        }

        if (decimal.compareTo(min) < 0) {
            throw new NormalisationFailureException(
                    "Number is below the range of values that can be held by a "
                            + target.getName());
        }

        if (decimal.compareTo(max) > 0) {
            throw new NormalisationFailureException(
                    "Number is above the range of values that can be held by a "
                            + target.getName());
        }

        return mapper.apply(decimal);
    }

    private static Float tryNormaliseFloat(final Double value) {
        final float result = value.floatValue();
        if (Float.isInfinite(result) && !Double.isInfinite(value)) {
            throw new NormalisationFailureException(
                    "Number is outside the range of values that can be held by a "
                            + Float.class.getName());
        }
        return result;
    }

    private static UUID uuidFromString(final String s) {
        try {
            return UUID.fromString(s);
        } catch (final Exception e) {
            throw new NormalisationFailureException("String can not be converted to UUID: " + s);
        }
    }

    private static final class Normaliser<T> {

        private final Class<T> targetType;
        private final Map<Class<?>, Function<?, T>> normalisers;

        private Normaliser(
                final Class<T> targetType, final Map<Class<?>, Function<?, T>> normalisers) {
            this.targetType = requireNonNull(targetType, "targetType");
            this.normalisers = Map.copyOf(normalisers);
        }

        static <T> Normaliser<T> normaliserFor(final Class<T> targetType) {
            return new Normaliser<>(targetType, Map.of());
        }

        <S> Normaliser<T> withRule(final Class<S> instanceType, final Function<S, T> mapper) {
            final Map<Class<?>, Function<?, T>> rules = new HashMap<>(normalisers);
            rules.put(instanceType, mapper);
            return new Normaliser<>(targetType, rules);
        }

        @SuppressWarnings("unchecked")
        public <S> T normalise(final S value) {
            final Function<S, T> mapper =
                    (Function<S, T>)
                            normalisers.getOrDefault(value.getClass(), this::unsupportedType);
            return mapper.apply(value);
        }

        private T unsupportedType(final Object value) {
            throw new NormalisationFailureException(
                    "Can not normalise "
                            + value.getClass().getName()
                            + " to "
                            + targetType.getName());
        }
    }

    @VisibleForTesting
    static final class NormalisationFailureException extends RuntimeException {
        NormalisationFailureException(final String msg) {
            super(msg);
        }
    }
}
