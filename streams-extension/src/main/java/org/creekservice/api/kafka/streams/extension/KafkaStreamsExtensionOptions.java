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

package org.creekservice.api.kafka.streams.extension;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.kafka.common.config.ClustersProperties.propertiesBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.creekservice.api.kafka.common.config.ClustersProperties;
import org.creekservice.api.kafka.common.config.KafkaPropertyOverrides;
import org.creekservice.api.kafka.common.config.SystemEnvPropertyOverrides;
import org.creekservice.api.kafka.streams.extension.exception.StreamsExceptionHandlers;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creekservice.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creekservice.api.service.extension.CreekExtensionOptions;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultLifecycleObserver;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultStateRestoreObserver;

/** Options for the Kafka streams extension. */
public final class KafkaStreamsExtensionOptions implements CreekExtensionOptions {

    public static final Duration DEFAULT_STREAMS_CLOSE_TIMEOUT = Duration.ofSeconds(30);

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

    /** More sensible Kafka Streams defaults: */
    private static final Map<String, ?> STREAMS_DEFAULTS =
            Map.of(
                    // Kafka default is only 1. This isn't very resilient, so up to a more sensible
                    // 3:
                    StreamsConfig.REPLICATION_FACTOR_CONFIG,
                    3,
                    // Default to exactly once semantics:
                    StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                    StreamsConfig.EXACTLY_ONCE_BETA,
                    // Reduce default commit interval from 30s to 1s:
                    // Reducing eos publishing delays and reducing the number of messages replayed
                    // on failure.
                    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                    1000,
                    // Configure an exception handler to log structured messages:
                    StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                    StreamsExceptionHandlers.LogAndFailProductionExceptionHandler.class);

    private final ClustersProperties properties;
    private final Duration streamsCloseTimeout;
    private final LifecycleObserver lifecycleObserver;
    private final StateRestoreObserver restoreObserver;
    private final KafkaMetricsPublisherOptions metricsPublishing;

    public static Builder builder() {
        return new Builder();
    }

    private KafkaStreamsExtensionOptions(
            final ClustersProperties properties,
            final Duration streamsCloseTimeout,
            final LifecycleObserver lifecycleObserver,
            final StateRestoreObserver restoreObserver,
            final KafkaMetricsPublisherOptions metricsPublishing) {
        this.properties = requireNonNull(properties, "properties");
        this.streamsCloseTimeout = requireNonNull(streamsCloseTimeout, "streamsCloseTimeout");
        this.lifecycleObserver = requireNonNull(lifecycleObserver, "lifecycleObserver");
        this.restoreObserver = requireNonNull(restoreObserver, "restoreObserver");
        this.metricsPublishing = requireNonNull(metricsPublishing, "metricsPublishing");
    }

    /**
     * Get Kafka client properties.
     *
     * @param clusterName the name of the Kafka cluster to get client properties for. Often will be
     *     {@link org.creekservice.api.kafka.metadata.KafkaTopicDescriptor#DEFAULT_CLUSTER_NAME}.
     * @return the Kafka properties.
     */
    public Properties properties(final String clusterName) {
        final Properties props = new Properties();
        props.putAll(properties.get(clusterName));
        return props;
    }

    /**
     * Get Kafka client properties.
     *
     * @param clusterName the name of the Kafka cluster to get client properties for. Often will be
     *     {@link org.creekservice.api.kafka.metadata.KafkaTopicDescriptor#DEFAULT_CLUSTER_NAME}.
     * @return the Kafka properties.
     */
    public Map<String, ?> propertyMap(final String clusterName) {
        return properties.get(clusterName);
    }

    /** @return the timeout used when closing the stream app. */
    public Duration streamsCloseTimeout() {
        return streamsCloseTimeout;
    }

    /** @return the observer that will be invoked as the stream app changes state. */
    public LifecycleObserver lifecycleObserver() {
        return lifecycleObserver;
    }

    /** @return the observer that will be invoked as the stream app restored its state. */
    public StateRestoreObserver restoreObserver() {
        return restoreObserver;
    }

    /** @return metrics publishing options */
    public KafkaMetricsPublisherOptions metricsPublishing() {
        return metricsPublishing;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KafkaStreamsExtensionOptions that = (KafkaStreamsExtensionOptions) o;
        return Objects.equals(properties, that.properties)
                && Objects.equals(streamsCloseTimeout, that.streamsCloseTimeout)
                && Objects.equals(lifecycleObserver, that.lifecycleObserver)
                && Objects.equals(restoreObserver, that.restoreObserver)
                && Objects.equals(metricsPublishing, that.metricsPublishing);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                properties,
                streamsCloseTimeout,
                lifecycleObserver,
                restoreObserver,
                metricsPublishing);
    }

    @Override
    public String toString() {
        return "KafkaStreamsExtensionOptions{"
                + "properties="
                + properties
                + ", streamsCloseTimeout="
                + streamsCloseTimeout
                + ", lifecycleObserver="
                + lifecycleObserver
                + ", restoreObserver="
                + restoreObserver
                + ", metricsPublishing="
                + metricsPublishing
                + '}';
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class Builder {

        private final ClustersProperties.Builder properties = propertiesBuilder();
        private Optional<KafkaPropertyOverrides> overridesProvider = Optional.empty();
        private Duration streamsCloseTimeout = DEFAULT_STREAMS_CLOSE_TIMEOUT;
        private Optional<LifecycleObserver> lifecycleObserver = Optional.empty();
        private Optional<StateRestoreObserver> restoreObserver = Optional.empty();
        private KafkaMetricsPublisherOptions metricsPublishing =
                KafkaMetricsPublisherOptions.builder().build();

        private Builder() {
            CLIENT_DEFAULTS.forEach(properties::putCommon);
            STREAMS_DEFAULTS.forEach(properties::putCommon);
        }

        /**
         * Set an alternate provider of Kafka property overrides.
         *
         * <p>The default overrides provider loads them from environment variables. See {@link
         * SystemEnvPropertyOverrides} for more info.
         *
         * <p>It is intended that the provider should return, among other things, properties such as
         * the bootstrap servers, so that these can be configured per-environment.
         *
         * <p>Note: the properties returned by the provider will <i>override</i> any properties set
         * via {@link #withKafkaProperty}.
         *
         * <p>Note: Any custom override provider implementation may want to consider if it needs to
         * be compatible with the system tests, as the system tests set properties via environment
         * variables.
         *
         * @param overridesProvider a custom provider of Kafka overrides.
         * @return self
         */
        public Builder withKafkaPropertiesOverrides(
                final KafkaPropertyOverrides overridesProvider) {
            this.overridesProvider = Optional.of(overridesProvider);
            return this;
        }

        /**
         * Set a common Kafka client property.
         *
         * <p>This property will be set for all clusters unless overridden either via {@link
         * #withKafkaProperty(String, String, Object)} or via {@link #withKafkaPropertiesOverrides}.
         *
         * @param name the name of the property
         * @param value the value of the property
         * @return self
         */
        public Builder withKafkaProperty(final String name, final Object value) {
            properties.putCommon(name, value);
            return this;
        }

        /**
         * Set a Kafka client property for a specific cluster.
         *
         * <p>Note: Any value set here can be overridden by the {@link #withKafkaPropertiesOverrides
         * overridesProvider}.
         *
         * @param cluster the name of the Kafka cluster this property should be scoped to.
         * @param name the name of the property
         * @param value the value of the property
         * @return self
         */
        public Builder withKafkaProperty(
                final String cluster, final String name, final Object value) {
            properties.put(cluster, name, value);
            return this;
        }

        /**
         * @param observer observer called as the Kafka Streams app changed state.
         * @return self
         */
        public Builder withLifecycleObserver(final LifecycleObserver observer) {
            this.lifecycleObserver = Optional.of(observer);
            return this;
        }

        /**
         * @param observer observer called as state stores are restored.
         * @return self
         */
        public Builder withStateRestoreObserver(final StateRestoreObserver observer) {
            this.restoreObserver = Optional.of(observer);
            return this;
        }

        /**
         * @param timeout time to give Kafka Streams to close down gracefully.
         * @return self
         */
        public Builder withStreamsCloseTimeout(final Duration timeout) {
            this.streamsCloseTimeout = requireNonNull(timeout, "timeout");
            return this;
        }

        /**
         * @param options options around metrics publishing.
         * @return self.
         */
        public Builder withMetricsPublishing(final KafkaMetricsPublisherOptions options) {
            this.metricsPublishing = requireNonNull(options, "options");
            return this;
        }

        /**
         * @param options options around metrics publishing.
         * @return self.
         */
        public Builder withMetricsPublishing(final KafkaMetricsPublisherOptions.Builder options) {
            return withMetricsPublishing(options.build());
        }

        /**
         * Build the immutable options.
         *
         * @return the built options.
         */
        public KafkaStreamsExtensionOptions build() {
            properties.putAll(
                    overridesProvider
                            .orElseGet(SystemEnvPropertyOverrides::systemEnvPropertyOverrides)
                            .get());
            return new KafkaStreamsExtensionOptions(
                    properties.build(),
                    streamsCloseTimeout,
                    lifecycleObserver.orElseGet(DefaultLifecycleObserver::new),
                    restoreObserver.orElseGet(DefaultStateRestoreObserver::new),
                    metricsPublishing);
        }
    }
}
