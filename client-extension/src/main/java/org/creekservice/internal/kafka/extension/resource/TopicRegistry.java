/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;

/** Type responsible for providing lookups of topic resources. */
public interface TopicRegistry {

    /**
     * Get the topic resource for the supplied {@code def}.
     *
     * @param def def of topic to look up.
     * @return the topic resource.
     * @param <K> the key type.
     * @param <V> the value type.
     * @throws RuntimeException on unknown topic.
     */
    <K, V> KafkaTopic<K, V> topic(KafkaTopicDescriptor<K, V> def);

    /**
     * Get the topic resource for the supplied {@code def}.
     *
     * @param cluster the cluster the topic belongs to.
     * @param topic the topic name.
     * @return the topic resource.
     * @throws RuntimeException on unknown topic.
     */
    KafkaTopic<?, ?> topic(String cluster, String topic);
}
