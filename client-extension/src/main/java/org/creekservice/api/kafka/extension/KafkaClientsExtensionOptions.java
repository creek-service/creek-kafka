/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.extension;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides;
import org.creekservice.api.kafka.extension.config.SystemEnvPropertyOverrides;
import org.creekservice.api.kafka.extension.config.TypeOverrides;
import org.creekservice.internal.kafka.extension.config.TypeOverridesBuilder;

/** Options for the Kafka client extension. */
public final class KafkaClientsExtensionOptions implements ClientsExtensionOptions {

    /** More sensible Kafka client defaults: */
    private static final Map<String, ?> CLIENT_DEFAULTS =
            Map.of(
                    // If not offsets exist (e.g. on first run of an application), then default to
                    // reading data from the start.
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    // More resilient default for replication:
                    ProducerConfig.ACKS_CONFIG, "all",
                    // Turn on compression by default as it's almost always quicker as it reduces
                    // payload size and the network
                    // is almost always the bottleneck:
                    ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    private final ClustersProperties.Builder properties;
    private final TypeOverrides typeOverrides;

    /**
     * @return new builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    private KafkaClientsExtensionOptions(
            final ClustersProperties.Builder properties, final TypeOverrides typeOverrides) {
        this.properties = requireNonNull(properties, "properties");
        this.typeOverrides = requireNonNull(typeOverrides, "typeOverrides");
    }

    @Override
    public ClustersProperties.Builder propertiesBuilder() {
        return properties;
    }

    @Override
    public TypeOverrides typeOverrides() {
        return typeOverrides;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KafkaClientsExtensionOptions that = (KafkaClientsExtensionOptions) o;
        return Objects.equals(properties, that.properties)
                && Objects.equals(typeOverrides, that.typeOverrides);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties, typeOverrides);
    }

    @Override
    public String toString() {
        return "KafkaClientsExtensionOptions{"
                + "properties="
                + properties
                + ", typeOverrides="
                + typeOverrides
                + '}';
    }

    /** Builder of the client extension options. */
    public static final class Builder implements ClientsExtensionOptions.Builder {

        private final ClustersProperties.Builder properties =
                ClustersProperties.propertiesBuilder();

        private final TypeOverridesBuilder typeOverrides = new TypeOverridesBuilder();

        private Builder() {
            CLIENT_DEFAULTS.forEach(properties::putCommon);
            properties.withOverridesProvider(
                    SystemEnvPropertyOverrides.systemEnvPropertyOverrides());
        }

        @Override
        public Builder withKafkaPropertiesOverrides(
                final KafkaPropertyOverrides overridesProvider) {
            properties.withOverridesProvider(overridesProvider);
            return this;
        }

        @Override
        public Builder withKafkaProperty(final String name, final Object value) {
            properties.putCommon(name, value);
            return this;
        }

        @Override
        public Builder withKafkaProperty(
                final String cluster, final String name, final Object value) {
            properties.put(cluster, name, value);
            return this;
        }

        @Override
        public <T> Builder withTypeOverride(final Class<T> type, final T instance) {
            typeOverrides.set(type, instance);
            return this;
        }

        public KafkaClientsExtensionOptions build() {
            return new KafkaClientsExtensionOptions(properties, typeOverrides.build());
        }
    }
}
