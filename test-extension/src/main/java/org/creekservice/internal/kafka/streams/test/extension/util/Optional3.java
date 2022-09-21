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
 *   <li>not present
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

    @SuppressWarnings("unchecked")
    public static <T> Optional3<T> notProvided() {
        return (Optional3<T>) NOT_PROVIDED;
    }

    @SuppressWarnings("unchecked")
    public static <T> Optional3<T> explicitlyNull() {
        return (Optional3<T>) EXPLICITLY_NULL;
    }

    public static <T> Optional3<T> of(final T value) {
        return new Optional3<>(true, Optional.of(value));
    }

    @JsonCreator
    public static <T> Optional3<T> ofNullable(final T value) {
        return new Optional3<>(true, Optional.ofNullable(value));
    }

    private Optional3(final boolean hasValue, final Optional<T> value) {
        this.hasValue = hasValue;
        this.value = requireNonNull(value, "value");
    }

    @JsonValue
    public Optional<T> get() {
        if (!isProvided()) {
            throw new NoSuchElementException();
        }
        return value;
    }

    public boolean isProvided() {
        return hasValue;
    }

    public boolean isPresent() {
        return isProvided() && value.isPresent();
    }

    @SuppressWarnings("unchecked")
    public <O> Optional3<O> map(final Function<T, O> mapper) {
        if (!isPresent()) {
            return (Optional3<O>) this;
        }
        return new Optional3<>(hasValue, value.map(mapper));
    }

    public T orElse(final T ifNull, final T ifNotProvided) {
        return hasValue ? value.orElse(ifNull) : ifNotProvided;
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
