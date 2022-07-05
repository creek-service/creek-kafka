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

package org.creekservice.internal.kafka.streams.extension;

import static java.util.Objects.requireNonNull;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.creekservice.api.kafka.common.config.ClustersProperties;
import org.creekservice.api.kafka.common.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.internal.kafka.streams.extension.resource.ResourceRegistry;

/** Kafka streams Creek extension. */
final class StreamsExtension implements KafkaStreamsExtension {

    static final String NAME = "Kafka-streams";

    private final ClustersProperties clustersProperties;
    private final ResourceRegistry resources;
    private final KafkaStreamsBuilder appBuilder;
    private final KafkaStreamsExecutor appExecutor;

    StreamsExtension(
            final ClustersProperties clustersProperties,
            final ResourceRegistry resources,
            final KafkaStreamsBuilder appBuilder,
            final KafkaStreamsExecutor appExecutor) {
        this.clustersProperties = requireNonNull(clustersProperties, "clustersProperties");
        this.resources = requireNonNull(resources, "resources");
        this.appBuilder = requireNonNull(appBuilder, "appBuilder");
        this.appExecutor = requireNonNull(appExecutor, "appExecutor");
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

    @Override
    public KafkaStreams build(final Topology topology, final String clusterName) {
        return appBuilder.build(topology, clusterName);
    }

    @Override
    public void execute(final KafkaStreams app) {
        appExecutor.execute(app);
    }
}
