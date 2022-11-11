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

package org.creekservice.api.kafka.extension;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.creekservice.api.kafka.extension.client.TopicClient;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides;
import org.creekservice.api.kafka.extension.config.SystemEnvPropertyOverrides;

/** Options for the Kafka client extension. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
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
    private final Optional<TopicClient> topicClient;

    /**
     * @return new builder instance.
     */
    public static Builder builder() {
        return new Builder();
    }

    private KafkaClientsExtensionOptions(
            final ClustersProperties.Builder properties, final Optional<TopicClient> topicClient) {
        this.properties = requireNonNull(properties, "properties");
        this.topicClient = requireNonNull(topicClient, "topicClient");
    }

    @Override
    public ClustersProperties.Builder propertiesBuilder() {
        return properties;
    }

    @Override
    public Optional<TopicClient> topicClient() {
        return topicClient;
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
                && Objects.equals(topicClient, that.topicClient);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties, topicClient);
    }

    @Override
    public String toString() {
        return "KafkaClientsExtensionOptions{"
                + "properties="
                + properties
                + ", topicClient="
                + topicClient
                + '}';
    }

    /** Builder of the client extension options. */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class Builder implements ClientsExtensionOptions.Builder {

        private final ClustersProperties.Builder properties =
                ClustersProperties.propertiesBuilder();
        private Optional<TopicClient> topicClient = Optional.empty();

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
        public Builder withTopicClient(final TopicClient topicClient) {
            this.topicClient = Optional.of(topicClient);
            return this;
        }

        public KafkaClientsExtensionOptions build() {
            return new KafkaClientsExtensionOptions(properties, topicClient);
        }
    }
}
