/*
 * Copyright 2023-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.extension.client;

import java.util.List;
import java.util.Map;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;

/** Client for working with Kafka topics. */
public interface TopicClient {

    /**
     * Ensure the supplied creatable topics exist, creating where necessary.
     *
     * @param topics the topics to ensure. all topics belong to a single Kafka cluster.
     */
    void ensureTopicsExist(List<? extends CreatableKafkaTopic<?, ?>> topics);

    /**
     * Type responsible for creating {@link TopicClient}'s.
     *
     * <p>Can be overridden via {@link
     * org.creekservice.api.kafka.extension.ClientsExtensionOptions.Builder#withTypeOverride}.
     */
    @FunctionalInterface
    interface Factory {

        /**
         * Create a {@link TopicClient}.
         *
         * @param clusterName name of the Kafka cluster.
         * @param kafkaProperties properties of the Kafka cluster.
         * @return an initialised {@link TopicClient}.
         */
        TopicClient create(String clusterName, Map<String, Object> kafkaProperties);
    }
}
