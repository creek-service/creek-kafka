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

package org.creekservice.internal.kafka.streams.test.extension;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides;
import org.creekservice.api.kafka.extension.config.SystemEnvPropertyOverrides;

public final class ClusterEndpointsProvider implements KafkaPropertyOverrides {

    private final KafkaPropertyOverrides delegate;
    private final Map<String, Map<String, ?>> configs = new HashMap<>();

    public ClusterEndpointsProvider() {
        this(SystemEnvPropertyOverrides.systemEnvPropertyOverrides());
    }

    @VisibleForTesting
    ClusterEndpointsProvider(final KafkaPropertyOverrides delegate) {
        this.delegate = requireNonNull(delegate, "delegate");
    }

    @Override
    public void init(final Set<String> clusterNames) {
        delegate.init(clusterNames);
    }

    @Override
    public Map<String, ?> get(final String clusterName) {
        final Map<String, Object> result = new HashMap<>(delegate.get(clusterName));
        result.putAll(configs.getOrDefault(clusterName, Map.of()));
        return result;
    }

    public void put(final String clusterName, final Map<String, ?> config) {
        configs.put(requireNonNull(clusterName, "clusterName"), requireNonNull(config, "config"));
    }
}
