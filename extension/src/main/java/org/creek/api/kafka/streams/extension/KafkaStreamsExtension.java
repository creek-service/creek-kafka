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

package org.creek.api.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.creek.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creek.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creek.api.service.extension.CreekExtension;
import org.creek.internal.kafka.streams.extension.KafkaStreamsBuilder;
import org.creek.internal.kafka.streams.extension.KafkaStreamsExecutor;
import org.creek.internal.kafka.streams.extension.KafkaStreamsExtensionOptions;

/**
 * Todo: java docs + tests
 *
 * <p>- also split into the bit the user cares about, i.e. the builder, and the bit we care about
 * internally only?
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class KafkaStreamsExtension implements CreekExtension {

    public static final Duration DEFAULT_STREAMS_CLOSE_TIMEOUT = Duration.ofSeconds(30);

    private final KafkaStreamsBuilder appBuilder;
    private final KafkaStreamsExecutor appExecutor;

    /** @return extension with default config. */
    public static KafkaStreamsExtension defaults() {
        return builder().build();
    }

    /** @return builder used to customise the extension. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Execute a Kafka Streams {@link Topology}.
     *
     * @param topology the topology to execute.
     * @return the exit code todo: link to exit codes as part of api? enum?
     */
    public void execute(final Topology topology) {
        final KafkaStreams app = appBuilder.build(topology);
        appExecutor.execute(app);
    }

    // Todo @VisibleForTesting
    KafkaStreamsExtension(
            final KafkaStreamsBuilder appBuilder, final KafkaStreamsExecutor appExecutor) {
        this.appBuilder = requireNonNull(appBuilder, "appBuilder");
        this.appExecutor = requireNonNull(appExecutor, "appExecutor");
    }

    public static final class Builder {

        private final Properties properties = new Properties();
        private Duration streamsCloseTimeout = DEFAULT_STREAMS_CLOSE_TIMEOUT;
        private Optional<LifecycleObserver> lifecycleObserver = Optional.empty();
        private Optional<StateRestoreObserver> restoreObserver = Optional.empty();

        // Todo(ac): Better as String value?
        public Builder withKafkaProperty(final String name, final Object value) {
            properties.put(name, value);
            return this;
        }

        /**
         * @param observer observer called as the Kafka Streams app changed state.
         * @return self
         */
        public Builder withLifecycleObserver(final LifecycleObserver observer) {
            this.lifecycleObserver = Optional.of(requireNonNull(observer, "observer"));
            return this;
        }

        /**
         * @param observer observer called as state stores are restored.
         * @return self
         */
        public Builder withStateRestoreObserver(final StateRestoreObserver observer) {
            this.restoreObserver = Optional.of(requireNonNull(observer, "observer"));
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

        public KafkaStreamsExtension build() {
            final KafkaStreamsExtensionOptions options =
                    new KafkaStreamsExtensionOptions(
                            properties, streamsCloseTimeout, lifecycleObserver, restoreObserver);
            return new KafkaStreamsExtension(
                    new KafkaStreamsBuilder(options), new KafkaStreamsExecutor(options));
        }
    }
}

// Todo(ac): Rename module to `streams-extension`
