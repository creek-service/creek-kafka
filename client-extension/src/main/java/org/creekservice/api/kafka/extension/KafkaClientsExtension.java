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

package org.creekservice.api.kafka.extension;


import java.util.Properties;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.service.extension.CreekExtension;

/** Kafka client extension to Creek. */
public interface KafkaClientsExtension extends CreekExtension {

    /**
     * Get the Kafka properties that should be used to build a topology.
     *
     * <p>Note: the properties should be considered immutable. Changing them may result in undefined
     * behaviour.
     *
     * @param clusterName the name of the Kafka cluster to get client properties for. Often will be
     *     {@link KafkaTopicDescriptor#DEFAULT_CLUSTER_NAME}.
     * @return the properties.
     */
    Properties properties(String clusterName);

    /**
     * Get a topic resource for the supplied {@code def}.
     *
     * @param def the topic descriptor
     * @return the topic resource.
     */
    <K, V> KafkaTopic<K, V> topic(KafkaTopicDescriptor<K, V> def);
}
