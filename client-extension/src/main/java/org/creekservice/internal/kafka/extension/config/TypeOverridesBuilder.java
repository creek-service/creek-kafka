/*
 * Copyright 2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.extension.config;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.creekservice.api.kafka.extension.config.TypeOverrides;

/** Builder of {@link TypeOverrides}. */
public final class TypeOverridesBuilder {

    private final Map<Class<?>, Object> overrides = new HashMap<>();

    /**
     * Set the overriding {@code instance} for the supplied {@code type}.
     *
     * @param type the type, normally an interface, which is to be overridden.
     * @param instance the instance to use.
     * @param <T> the type being overridden.
     */
    public <T> TypeOverridesBuilder set(final Class<T> type, final T instance) {
        overrides.put(requireNonNull(type, "type"), requireNonNull(instance, "instance"));
        return this;
    }

    /**
     * @return the built overrides.
     */
    public TypeOverrides build() {
        return new ConfiguredTypeOverrides(overrides);
    }

    private static final class ConfiguredTypeOverrides implements TypeOverrides {

        private final Map<Class<?>, ?> overrides;

        ConfiguredTypeOverrides(final Map<Class<?>, ?> overrides) {
            this.overrides = Map.copyOf(overrides);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> Optional<T> get(final Class<T> type) {
            return Optional.ofNullable((T) overrides.get(type));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ConfiguredTypeOverrides that = (ConfiguredTypeOverrides) o;
            return Objects.equals(overrides, that.overrides);
        }

        @Override
        public int hashCode() {
            return Objects.hash(overrides);
        }

        @Override
        public String toString() {
            return "ConfiguredTypeOverrides{" + "overrides=" + overrides + '}';
        }
    }
}
