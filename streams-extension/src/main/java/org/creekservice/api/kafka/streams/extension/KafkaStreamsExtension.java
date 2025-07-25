/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;

/** Kafka streams extension to Creek. */
public interface KafkaStreamsExtension extends KafkaClientsExtension {

    /**
     * Build a Kafka Streams app from the supplied {@code topology}.
     *
     * @param topology the topology to build.
     * @param clusterName the name of the Kafka cluster to get client properties for. Often will be
     *     {@link KafkaTopicDescriptor#DEFAULT_CLUSTER_NAME}. Note, topics used in streams
     *     topologies must all be in the same cluster.
     * @return the stream app.
     */
    KafkaStreams build(Topology topology, String clusterName);

    /**
     * Execute a Kafka Streams app.
     *
     * @param app the app to execute.
     */
    void execute(KafkaStreams app);

    /**
     * Convenience method to build and execute a {@code topology}.
     *
     * @param topology the topology to build and execute.
     */
    default void execute(final Topology topology) {
        execute(build(topology, KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME));
    }
}
