/*
 * Copyright 2021-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.metadata.topic;

import java.util.Map;

/** Type for defining a Kafka topic's config */
public interface KafkaTopicConfig {

    /**
     * @return topics partition count.
     */
    int partitions();

    /**
     * @return any specific configs. If not supplied, cluster/env defaults will be used.
     */
    default Map<String, String> config() {
        return Map.of();
    }
}
