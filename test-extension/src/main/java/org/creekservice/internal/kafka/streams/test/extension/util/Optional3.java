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

package org.creekservice.internal.kafka.streams.test.extension.util;

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * A null-safe tri-state value type.
 *
 * <p>Tri states:
 *
 * <ul>
 *   <li>not value proviced
 *   <li>present as null
 *   <li>present as value
 * </ul>
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class Optional3<T> {

    private static final Optional3<?> NOT_PROVIDED = new Optional3<>(false, Optional.empty());
    private static final Optional3<?> EXPLICITLY_NULL = new Optional3<>(true, Optional.empty());

    private final Optional<T> value;
    private final boolean hasValue;

    /**
     * No value provided
     *
     * @param <T> the value type
     * @return {@link Optional3} indicating no value was provided.
     */
    @SuppressWarnings("unchecked")
    public static <T> Optional3<T> notProvided() {
        return (Optional3<T>) NOT_PROVIDED;
    }

    /**
     * Null value provided.
     *
     * @param <T> the value type
     * @return {@link Optional3} indicating a {@code null} value was provided.
     */
    @SuppressWarnings("unchecked")
    public static <T> Optional3<T> explicitlyNull() {
        return (Optional3<T>) EXPLICITLY_NULL;
    }

    /**
     * Non-null value provided
     *
     * @param value the non-null value.
     * @param <T> the value type
     * @return {@link Optional3} indicating a non-null value was provided.
     */
    public static <T> Optional3<T> of(final T value) {
        return new Optional3<>(true, Optional.of(value));
    }

    /**
     * Protentially null value provided.
     *
     * @param value the potentially null value.
     * @param <T> the value type
     * @return {@link Optional3} indicating value was provided.
     */
    @JsonCreator
    public static <T> Optional3<T> ofNullable(final T value) {
        return new Optional3<>(true, Optional.ofNullable(value));
    }

    private Optional3(final boolean hasValue, final Optional<T> value) {
        this.hasValue = hasValue;
        this.value = requireNonNull(value, "value");
    }

    /**
     * Get the value, if provided.
     *
     * @return {@link Optional} representing the value
     * @throws NoSuchElementException if no value was provided
     */
    @JsonValue
    public Optional<T> get() {
        if (!isProvided()) {
            throw new NoSuchElementException();
        }
        return value;
    }

    /**
     * @return {@code true} if the instance has a value, null or otherwise.
     */
    public boolean isProvided() {
        return hasValue;
    }

    /**
     * @return {@code true} if the instance has a non-null value.
     */
    public boolean isPresent() {
        return isProvided() && value.isPresent();
    }

    /**
     * Transform any non-null value using the supplied {@code mapper}.
     *
     * @param mapper the mapper.
     * @param <O> the resulting type.
     * @return the result of the transformation.
     */
    @SuppressWarnings("unchecked")
    public <O> Optional3<O> map(final Function<T, O> mapper) {
        if (!isPresent()) {
            return (Optional3<O>) this;
        }
        return new Optional3<>(hasValue, value.map(mapper));
    }

    /**
     * Get the contained non-null value, or else one of the supplied parameters if there is none.
     *
     * @param ifNull the value to return if the instance has a null value.
     * @param ifNotProvided the value to return if the instance has no value.
     * @return the non-null value, or one of the parameters.
     */
    public T orElse(final T ifNull, final T ifNotProvided) {
        return hasValue ? value.orElse(ifNull) : ifNotProvided;
    }

    /**
     * Return the contained non-null value, or else throw.
     *
     * @return the non-null value.
     * @throws NoSuchElementException if the value is null or not provided.
     */
    public T orElseThrow() {
        return value.orElseThrow();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Optional3<?> that = (Optional3<?>) o;
        return hasValue == that.hasValue && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, hasValue);
    }

    @Override
    public String toString() {
        return map(Objects::toString).orElse("null", "<not-set>");
    }
}
