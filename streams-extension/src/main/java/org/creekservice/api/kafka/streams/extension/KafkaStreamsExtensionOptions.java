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

package org.creekservice.api.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.StreamsConfig;
import org.creekservice.api.kafka.extension.ClientsExtensionOptions;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides;
import org.creekservice.api.kafka.extension.config.TypeOverrides;
import org.creekservice.api.kafka.streams.extension.exception.StreamsExceptionHandlers;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creekservice.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creekservice.internal.kafka.streams.extension.StreamsVersions;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultLifecycleObserver;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultStateRestoreObserver;

/** Options for the Kafka client extension. */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class KafkaStreamsExtensionOptions implements ClientsExtensionOptions {

    /** Default stream close timeout. */
    public static final Duration DEFAULT_STREAMS_CLOSE_TIMEOUT = Duration.ofSeconds(30);

    /** Sensible Kafka Streams defaults: */
    private static final Map<String, ?> STREAMS_DEFAULTS =
            Map.of(
                    // Kafka default is only 1. This isn't very resilient: up to a more sensible 3:
                    StreamsConfig.REPLICATION_FACTOR_CONFIG,
                    3,
                    // Default to exactly once semantics:
                    StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                    StreamsVersions.EXACTLY_ONCE_V2,
                    // Reduce default commit interval from 30s to 1s:
                    // Reducing eos publishing delays and the number of messages replayed on
                    // failure.
                    StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                    1000,
                    // Configure an exception handler to log structured messages:
                    StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                    StreamsExceptionHandlers.LogAndFailProductionExceptionHandler.class);

    private final KafkaClientsExtensionOptions clientOptions;
    private final Duration streamsCloseTimeout;
    private final LifecycleObserver lifecycleObserver;
    private final StateRestoreObserver restoreObserver;
    private final KafkaMetricsPublisherOptions metricsPublishing;

    /**
     * @return new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    private KafkaStreamsExtensionOptions(
            final KafkaClientsExtensionOptions clientOptions,
            final Duration streamsCloseTimeout,
            final LifecycleObserver lifecycleObserver,
            final StateRestoreObserver restoreObserver,
            final KafkaMetricsPublisherOptions metricsPublishing) {
        this.clientOptions = requireNonNull(clientOptions, "clientOptions");
        this.streamsCloseTimeout = requireNonNull(streamsCloseTimeout, "streamsCloseTimeout");
        this.lifecycleObserver = requireNonNull(lifecycleObserver, "lifecycleObserver");
        this.restoreObserver = requireNonNull(restoreObserver, "restoreObserver");
        this.metricsPublishing = requireNonNull(metricsPublishing, "metricsPublishing");
    }

    @Override
    public ClustersProperties.Builder propertiesBuilder() {
        return clientOptions.propertiesBuilder();
    }

    @Override
    public TypeOverrides typeOverrides() {
        return clientOptions.typeOverrides();
    }

    /**
     * @return the timeout used when closing the stream app.
     */
    public Duration streamsCloseTimeout() {
        return streamsCloseTimeout;
    }

    /**
     * @return the observer that will be invoked as the stream app changes state.
     */
    public LifecycleObserver lifecycleObserver() {
        return lifecycleObserver;
    }

    /**
     * @return the observer that will be invoked as the stream app restored its state.
     */
    public StateRestoreObserver restoreObserver() {
        return restoreObserver;
    }

    /**
     * @return metrics publishing options
     */
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
        return Objects.equals(clientOptions, that.clientOptions)
                && Objects.equals(streamsCloseTimeout, that.streamsCloseTimeout)
                && Objects.equals(lifecycleObserver, that.lifecycleObserver)
                && Objects.equals(restoreObserver, that.restoreObserver)
                && Objects.equals(metricsPublishing, that.metricsPublishing);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                clientOptions,
                streamsCloseTimeout,
                lifecycleObserver,
                restoreObserver,
                metricsPublishing);
    }

    @Override
    public String toString() {
        return "KafkaStreamsExtensionOptions{"
                + "clientOptions="
                + clientOptions
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

    /** Builder of streams options. */
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class Builder implements ClientsExtensionOptions.Builder {

        private final KafkaClientsExtensionOptions.Builder clientOptionsBuilder =
                KafkaClientsExtensionOptions.builder();

        private Duration streamsCloseTimeout = DEFAULT_STREAMS_CLOSE_TIMEOUT;
        private Optional<LifecycleObserver> lifecycleObserver = Optional.empty();
        private Optional<StateRestoreObserver> restoreObserver = Optional.empty();
        private KafkaMetricsPublisherOptions metricsPublishing =
                KafkaMetricsPublisherOptions.builder().build();

        private Builder() {
            STREAMS_DEFAULTS.forEach(clientOptionsBuilder::withKafkaProperty);
        }

        @Override
        public Builder withKafkaPropertiesOverrides(
                final KafkaPropertyOverrides overridesProvider) {
            clientOptionsBuilder.withKafkaPropertiesOverrides(overridesProvider);
            return this;
        }

        @Override
        public Builder withKafkaProperty(final String name, final Object value) {
            clientOptionsBuilder.withKafkaProperty(name, value);
            return this;
        }

        @Override
        public Builder withKafkaProperty(
                final String cluster, final String name, final Object value) {
            clientOptionsBuilder.withKafkaProperty(cluster, name, value);
            return this;
        }

        @Override
        public <T> Builder withTypeOverride(final Class<T> type, final T instance) {
            clientOptionsBuilder.withTypeOverride(type, instance);
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
            return new KafkaStreamsExtensionOptions(
                    clientOptionsBuilder.build(),
                    streamsCloseTimeout,
                    lifecycleObserver.orElseGet(DefaultLifecycleObserver::new),
                    restoreObserver.orElseGet(DefaultStateRestoreObserver::new),
                    metricsPublishing);
        }
    }
}
