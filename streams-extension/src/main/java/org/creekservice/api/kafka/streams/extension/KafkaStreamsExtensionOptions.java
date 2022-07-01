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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.creekservice.api.kafka.common.config.KafkaPropertyOverrides;
import org.creekservice.api.kafka.common.config.SystemEnvPropertyOverrides;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creekservice.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creekservice.api.service.extension.CreekExtensionOptions;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultLifecycleObserver;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultStateRestoreObserver;

/** Options for the Kafka streams extension. */
public final class KafkaStreamsExtensionOptions implements CreekExtensionOptions {

    public static final Duration DEFAULT_STREAMS_CLOSE_TIMEOUT = Duration.ofSeconds(30);

    private final Map<String, Object> properties;
    private final Duration streamsCloseTimeout;
    private final LifecycleObserver lifecycleObserver;
    private final StateRestoreObserver restoreObserver;
    private final KafkaMetricsPublisherOptions metricsPublishing;

    public static Builder builder() {
        return new Builder();
    }

    private KafkaStreamsExtensionOptions(
            final Map<String, Object> properties,
            final Duration streamsCloseTimeout,
            final LifecycleObserver lifecycleObserver,
            final StateRestoreObserver restoreObserver,
            final KafkaMetricsPublisherOptions metricsPublishing) {
        this.properties = Map.copyOf(requireNonNull(properties, "properties"));
        this.streamsCloseTimeout = requireNonNull(streamsCloseTimeout, "streamsCloseTimeout");
        this.lifecycleObserver = requireNonNull(lifecycleObserver, "lifecycleObserver");
        this.restoreObserver = requireNonNull(restoreObserver, "restoreObserver");
        this.metricsPublishing = requireNonNull(metricsPublishing, "metricsPublishing");
    }

    /** @return the Kafka properties. */
    public Properties properties() {
        final Properties props = new Properties();
        props.putAll(properties);
        return props;
    }

    /** @return the Kafka properties. */
    public Map<String, ?> propertyMap() {
        return Map.copyOf(properties);
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

        private final Map<String, Object> properties = new HashMap<>();
        private Optional<KafkaPropertyOverrides> overridesProvider = Optional.empty();
        private Duration streamsCloseTimeout = DEFAULT_STREAMS_CLOSE_TIMEOUT;
        private Optional<LifecycleObserver> lifecycleObserver = Optional.empty();
        private Optional<StateRestoreObserver> restoreObserver = Optional.empty();
        private KafkaMetricsPublisherOptions metricsPublishing =
                KafkaMetricsPublisherOptions.builder().build();

        private Builder() {}

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
         * Set/overwrite a property that should be passed to the Kafka clients / streams app.
         *
         * @param name the name of the property
         * @param value the value of the property
         * @return self
         */
        public Builder withKafkaProperty(final String name, final Object value) {
            properties.put(name, value);
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
                    properties,
                    streamsCloseTimeout,
                    lifecycleObserver.orElseGet(DefaultLifecycleObserver::new),
                    restoreObserver.orElseGet(DefaultStateRestoreObserver::new),
                    metricsPublishing);
        }
    }
}
