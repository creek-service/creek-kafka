/*
 * Copyright 2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.extension.resource;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.config.TypeOverrides;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider.SerdeProvider;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProviders;

/**
 * Responsible for initialising and tracking {@link
 * org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider} instances per-cluster. Thereby
 * allowing serde providers to initialise internal state and cache resources, per cluster.
 */
final class ClustersSerdeProviders {

    private final TypeOverrides typeOverrides;
    private final KafkaSerdeProviders serdeProviders;
    private final Map<ClusterAndFormat, SerdeProvider> providers = new HashMap<>();

    ClustersSerdeProviders(final TypeOverrides typeOverrides) {
        this(typeOverrides, KafkaSerdeProviders.create());
    }

    @VisibleForTesting
    ClustersSerdeProviders(
            final TypeOverrides typeOverrides, final KafkaSerdeProviders serdeProviders) {
        this.typeOverrides = requireNonNull(typeOverrides, "typeOverrides");
        this.serdeProviders = requireNonNull(serdeProviders, "serdeProviders");
    }

    public SerdeProvider get(final SerializationFormat format, final String clusterName) {
        return providers.computeIfAbsent(
                new ClusterAndFormat(clusterName, format), this::initialiseCluster);
    }

    private SerdeProvider initialiseCluster(final ClusterAndFormat key) {
        return serdeProviders.get(key.format).initialize(key.clusterName, typeOverrides::get);
    }

    private static final class ClusterAndFormat {
        private final String clusterName;
        private final SerializationFormat format;

        private ClusterAndFormat(final String clusterName, final SerializationFormat format) {
            this.clusterName = requireNonNull(clusterName, "clusterName");
            this.format = requireNonNull(format, "format");
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ClusterAndFormat that = (ClusterAndFormat) o;
            return Objects.equals(clusterName, that.clusterName)
                    && Objects.equals(format, that.format);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterName, format);
        }
    }
}
