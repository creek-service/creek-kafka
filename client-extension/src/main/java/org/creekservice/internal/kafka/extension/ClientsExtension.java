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

package org.creekservice.internal.kafka.extension;

import static java.util.Objects.requireNonNull;

import java.util.Properties;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.extension.resource.ResourceRegistry;

/** Kafka Client Creek extension. */
public final class ClientsExtension implements KafkaClientsExtension {

    static final String NAME = "org.creekservice.kafka.clients";

    private final ClustersProperties clustersProperties;
    private final ResourceRegistry resources;

    public ClientsExtension(
            final ClustersProperties clustersProperties, final ResourceRegistry resources) {
        this.clustersProperties = requireNonNull(clustersProperties, "clustersProperties");
        this.resources = requireNonNull(resources, "resources");
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Properties properties(final String clusterName) {
        return clustersProperties.properties(clusterName);
    }

    @Override
    public <K, V> KafkaTopic<K, V> topic(final KafkaTopicDescriptor<K, V> def) {
        return resources.topic(def);
    }
}
