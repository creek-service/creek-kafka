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

package org.creekservice.api.kafka.common.config;

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Holds Kafka client properties for connecting to multiple Kafka clusters.
 *
 * <p>Properties can be added for a specific cluster or common to all clusters. Specific properties
 * override common.
 *
 * <p>The class is immutable(ish) and thread-safe, as long as the values of the map are considered
 * immutable. The builder is not thread-safe.
 */
public final class ClustersProperties {

    private final Map<String, Object> common;
    private final Map<String, Map<String, ?>> clusters;

    private ClustersProperties(
            final Map<String, Object> common, final Map<String, Map<String, Object>> clusters) {
        final Map<String, Map<String, Object>> copy = new HashMap<>();
        clusters.forEach((k, v) -> copy.put(k, Map.copyOf(v)));
        this.common = Map.copyOf(common);
        this.clusters = Map.copyOf(copy);
    }

    public static Builder propertiesBuilder() {
        return new Builder();
    }

    /**
     * Get Kafka client properties for the supplied {@code clusterName}.
     *
     * @param clusterName the name of the Kafka cluster.
     * @return the properties, or an empty map if non are set.
     */
    public Map<String, ?> get(final String clusterName) {
        final Map<String, Object> props = new HashMap<>(common);
        props.putAll(clusterSpecific(clusterName));
        return props;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ClustersProperties that = (ClustersProperties) o;
        return Objects.equals(common, that.common) && Objects.equals(clusters, that.clusters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(common, clusters);
    }

    @Override
    public String toString() {
        return "ClustersProperties{" + "common=" + common + ", byCluster=" + clusters + '}';
    }

    private Map<String, ?> clusterSpecific(final String clusterName) {
        return clusters.getOrDefault(requireNonNull(clusterName, "clusterName"), Map.of());
    }

    public static final class Builder {

        private final Map<String, Object> common = new HashMap<>();
        private final Map<String, Map<String, Object>> clusters = new HashMap<>();

        private Builder() {}

        public Builder putCommon(final String name, final Object value) {
            common.put(requireNonBlank(name, "name"), requireNonNull(value, "value"));
            return this;
        }

        public Builder put(final String cluster, final String name, final Object value) {
            clusters.computeIfAbsent(requireNonBlank(cluster, "cluster"), k -> new HashMap<>())
                    .put(requireNonBlank(name, "name"), requireNonNull(value, "value"));
            return this;
        }

        public Builder putAll(final ClustersProperties other) {
            other.common.forEach(this::putCommon);
            other.clusters.forEach(
                    (clusterName, properties) ->
                            properties.forEach((key, value) -> put(clusterName, key, value)));
            return this;
        }

        public ClustersProperties build() {
            return new ClustersProperties(common, clusters);
        }
    }
}
