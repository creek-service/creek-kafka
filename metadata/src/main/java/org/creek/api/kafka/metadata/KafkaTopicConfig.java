/*
 * Copyright 2021-2022 Creek Contributors (https://github.com/creek-service)
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

package org.creek.api.kafka.metadata;


import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/** Type for defining a Kafka topic's config */
public interface KafkaTopicConfig {
    /** @return topics partition count. */
    int getPartitions();

    /** @return any specific configs. If not supplied, cluster/env defaults will be used. */
    default Map<String, String> getConfig() {
        return Map.of();
    }

    /** @return true if left and right are equivalent. */
    static boolean matches(final KafkaTopicConfig left, final KafkaTopicConfig right) {
        return left.getPartitions() == right.getPartitions()
                && Objects.equals(left.getConfig(), right.getConfig());
    }

    /**
     * Convert topic details to string.
     *
     * <p>Used when logging topic details.
     * Avoids the need for every implementor of this type to define {@code toString).
     *
     * @param config the config to convert
     * @return string representation
     */
    static String asString(final KafkaTopicConfig config) {
        return new StringJoiner(", ", config.getClass().getSimpleName() + "[", "]")
                .add("partitions=" + config.getPartitions())
                .add("config=" + config.getConfig())
                .toString();
    }
}
