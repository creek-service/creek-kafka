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

package org.creek.internal.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import org.creek.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creek.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creek.internal.kafka.streams.extension.observation.DefaultLifecycleObserver;
import org.creek.internal.kafka.streams.extension.observation.DefaultStateRestoreObserver;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class KafkaStreamsExtensionOptions {

    private final Properties properties;
    private final Duration streamsCloseTimeout;
    private final Optional<LifecycleObserver> lifecycleObserver;
    private final Optional<StateRestoreObserver> stateRestoreObserver;

    public KafkaStreamsExtensionOptions(
            final Properties properties,
            final Duration streamsCloseTimeout,
            final Optional<LifecycleObserver> lifecycleObserver,
            final Optional<StateRestoreObserver> stateRestoreObserver) {
        this.properties = requireNonNull(properties, "properties");
        this.streamsCloseTimeout = requireNonNull(streamsCloseTimeout, "streamsCloseTimeout");
        this.lifecycleObserver = requireNonNull(lifecycleObserver, "lifecycleObserver");
        this.stateRestoreObserver = requireNonNull(stateRestoreObserver, "stateRestoreObserver");
    }

    public Properties properties() {
        return properties;
    }

    public Duration streamsCloseTimeout() {
        return streamsCloseTimeout;
    }

    public LifecycleObserver lifecycleObserver() {
        return lifecycleObserver.orElseGet(DefaultLifecycleObserver::new);
    }

    public StateRestoreObserver stateRestoreObserver() {
        return stateRestoreObserver.orElseGet(DefaultStateRestoreObserver::new);
    }
}
