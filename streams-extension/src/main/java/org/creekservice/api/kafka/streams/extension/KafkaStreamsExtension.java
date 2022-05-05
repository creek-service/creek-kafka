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


import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.creekservice.api.kafka.common.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.service.extension.CreekExtension;

/** Kafka streams extension to Creek. */
public interface KafkaStreamsExtension extends CreekExtension {

    /**
     * Get the Kafka properties that should be used to build a topology.
     *
     * <p>Note: the properties should be considered immutable. Changing them may result in undefined
     * behaviour.
     *
     * @return the properties.
     */
    Properties properties();

    /**
     * Get a topic resource for the supplied {@code def}.
     *
     * @param def the topic descriptor
     * @return the topic resource.
     */
    <K, V> KafkaTopic<K, V> topic(KafkaTopicDescriptor<K, V> def);

    /**
     * Build a Kafka Streams app from the supplied {@code topology}.
     *
     * @param topology the topology to build.
     * @return the streams app.
     */
    KafkaStreams build(Topology topology);

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
        execute(build(topology));
    }
}
