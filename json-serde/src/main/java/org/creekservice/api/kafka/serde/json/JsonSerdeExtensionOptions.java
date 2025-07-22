/*
 * Copyright 2024-2025 Creek Contributors (https://github.com/creek-service)
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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.MockEndpointsLoader;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaStoreEndpoints;
import org.creekservice.api.service.extension.CreekExtensionOptions;
import org.creekservice.internal.kafka.serde.json.schema.store.client.DefaultJsonSchemaRegistryClient;

/**
 * Options class to customise how the JSON serde operate.
 *
 * <p>An instance of this class can be registered via the {@code CreekServices.Builder.with} method.
 */
public final class JsonSerdeExtensionOptions implements CreekExtensionOptions {

    private final Map<Class<?>, String> subtypes;
    private final Map<Class<?>, ?> typeOverrides;

    /**
     * @return new builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Options builder for use in testing.
     *
     * <p>This options builder can be used in testing, without the need for a Schema Registry
     * service, e.g. unit testing:
     *
     * <pre>
     * CreekServices.builder(new MyServiceDescriptor())
     *    .with(JsonSerdeExtensionOptions.testBuilder().build()));
     *    .build();
     * </pre>
     *
     * <p>The options come preconfigured with a mock Schema Registry client, hence the serde do not
     * need an actual Schema Registry instance to connect to.
     *
     * @return an options builder pre-configured to allow disconnected unit testing.
     */
    public static Builder testBuilder() {
        return builder()
                .withTypeOverride(
                        JsonSchemaStoreClient.Factory.class,
                        (schemaRegistryName, endpoints) ->
                                new DefaultJsonSchemaRegistryClient(
                                        schemaRegistryName,
                                        new MockSchemaRegistryClient(
                                                List.of(new JsonSchemaProvider()))))
                .withTypeOverride(SchemaStoreEndpoints.Loader.class, new MockEndpointsLoader() {});
    }

    private JsonSerdeExtensionOptions(
            final Map<Class<?>, ?> typeOverrides, final Map<Class<?>, String> subtypes) {
        this.typeOverrides = Map.copyOf(requireNonNull(typeOverrides, "typeOverrides"));
        this.subtypes = Map.copyOf(requireNonNull(subtypes, "subtypes"));
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

    /**
     * @return manually registered subtypes
     */
    public Map<Class<?>, String> subTypes() {
        return Map.copyOf(subtypes);
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
        return Objects.equals(subtypes, that.subtypes)
                && Objects.equals(typeOverrides, that.typeOverrides);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subtypes, typeOverrides);
    }

    @Override
    public String toString() {
        return "JsonSerdeExtensionOptions{"
                + "subtypes="
                + subtypes
                + ", typeOverrides="
                + typeOverrides
                + '}';
    }

    /** Build of client extension options. */
    public static final class Builder {

        private final Map<Class<?>, String> subtypes = new HashMap<>();
        private final Map<Class<?>, Object> overrides = new HashMap<>();

        private Builder() {}

        /**
         * Register a named subtype of a polymorphic type.
         *
         * <p>Use this method to register subtypes when a base type is annotated with {@link
         * com.fasterxml.jackson.annotation.JsonTypeInfo}, but not with {@link
         * com.fasterxml.jackson.annotation.JsonSubTypes}.
         *
         * @param type the subtype to register
         * @param logicalName the logical name that should be used in the JSON payload for this
         *     type. If an empty string, default naming will be used.
         * @return self, to allow chaining
         */
        public Builder withSubtype(final Class<?> type, final String logicalName) {
            subtypes.put(requireNonNull(type, "type"), requireNonNull(logicalName, "logicalName"));
            return this;
        }

        /**
         * Register subtypes of a polymorphic type.
         *
         * <p>Use this method to register subtypes when a base type is annotated with {@link
         * com.fasterxml.jackson.annotation.JsonTypeInfo}, but not with {@link
         * com.fasterxml.jackson.annotation.JsonSubTypes}.
         *
         * <p>Default naming will be used for each type. Use {@link #withSubtype(Class, String)} to
         * customise the logical name of a type.
         *
         * @param types the subtypes to register
         * @return self, to allow chaining
         */
        public Builder withSubtypes(final Class<?>... types) {
            Arrays.stream(types).forEach(type -> withSubtype(type, ""));
            return this;
        }

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
            return new JsonSerdeExtensionOptions(overrides, subtypes);
        }
    }
}
