/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

import java.util.Objects;
import java.util.StringJoiner;
import org.creekservice.api.kafka.metadata.KafkaTopicConfig;

/** Util class for working with implementations of {@link KafkaTopicConfig}. */
public final class KafkaTopicConfigs {

    private KafkaTopicConfigs() {}

    /**
     * @param left one config
     * @param right other config
     * @return true if left and right are equivalent.
     */
    public static boolean matches(final KafkaTopicConfig left, final KafkaTopicConfig right) {
        return left.partitions() == right.partitions()
                && Objects.equals(left.config(), right.config());
    }

    /**
     * Convert topic details to string.
     *
     * <p>Used when logging topic details. Avoids the need for every implementor of this type to
     * define {@code toString}.
     *
     * @param config the config to convert
     * @return string representation
     */
    public static String asString(final KafkaTopicConfig config) {
        return new StringJoiner(", ", config.getClass().getSimpleName() + "[", "]")
                .add("partitions=" + config.partitions())
                .add("config=" + config.config())
                .toString();
    }

    /**
     * Compute hash code of config.
     *
     * <p>Avoids Creek being at the mercy of implementers of the interface.
     *
     * @param config the config.
     * @return the hash code.
     */
    public static int hashCode(final KafkaTopicConfig config) {
        return Objects.hash(config.partitions(), config.config());
    }
}
