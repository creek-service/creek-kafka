/*
 * Copyright 2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.serde.json;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.creekservice.api.service.extension.CreekExtensionOptions;

public final class JsonSerdeExtensionOptions implements CreekExtensionOptions {

    private final Map<Class<?>, ?> typeOverrides;

    /**
     * @return new builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    private JsonSerdeExtensionOptions(final Map<Class<?>, ?> typeOverrides) {
        this.typeOverrides = Map.copyOf(requireNonNull(typeOverrides, "typeOverrides"));
    }

    /**
     * Retrieve the override instance for the supplied {@code type}, if one is set.
     *
     * @param type the type to look up.
     * @return the instance to use, if set, otherwise {@link Optional#empty()}.
     * @param <T> the type to look up.
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> typeOverride(final Class<T> type) {
        return Optional.ofNullable((T) typeOverrides.get(type));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JsonSerdeExtensionOptions that = (JsonSerdeExtensionOptions) o;
        return Objects.equals(typeOverrides, that.typeOverrides);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeOverrides);
    }

    @Override
    public String toString() {
        return "JsonSerdeExtensionOptions{" + "typeOverrides=" + typeOverrides + '}';
    }

    /** Build of client extension options. */
    public static final class Builder {

        private final Map<Class<?>, Object> overrides = new HashMap<>();

        private Builder() {}

        /**
         * Override a specific implementation of a {@code type} used internally.
         *
         * <p>Allows the customisation of certain types within the extension. See other parts of the
         * Creek documentation for example uses.
         *
         * @param <T> the type to be overridden.
         * @param type the type to be overridden.
         * @param instance the instance to use.
         * @return self.
         */
        public <T> Builder withTypeOverride(final Class<T> type, final T instance) {
            overrides.put(requireNonNull(type, "type"), requireNonNull(instance, "instance"));
            return this;
        }

        /**
         * Build the immutable options.
         *
         * @return the built options.
         */
        public JsonSerdeExtensionOptions build() {
            return new JsonSerdeExtensionOptions(overrides);
        }
    }
}
