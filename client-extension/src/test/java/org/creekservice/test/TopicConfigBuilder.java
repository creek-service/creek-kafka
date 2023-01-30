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

package org.creekservice.test;

import static java.util.Objects.requireNonNull;

import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.creekservice.api.kafka.metadata.KafkaTopicConfig;

public final class TopicConfigBuilder {

    private static final int MAX_PARTITIONS = 10_000;

    private final int partitions;
    private final Map<String, String> config = new HashMap<>();

    public static TopicConfigBuilder withPartitions(final int partitions) {
        return builder(partitions, false);
    }

    public static TopicConfigBuilder builder(final int partitions, final boolean allowCrazy) {
        if (partitions <= 0) {
            final NumberFormat format = NumberFormat.getInstance(Locale.ROOT);
            throw new IllegalArgumentException(
                    "partition count must be positive, but was " + format.format(partitions));
        }
        if (!allowCrazy && partitions > MAX_PARTITIONS) {
            final NumberFormat format = NumberFormat.getInstance(Locale.ROOT);
            throw new IllegalArgumentException(
                    "partition count should be less than "
                            + format.format(MAX_PARTITIONS)
                            + ", but was "
                            + format.format(partitions));
        }
        return new TopicConfigBuilder(partitions);
    }

    private TopicConfigBuilder(final int partitions) {
        this.partitions = partitions;
    }

    public TopicConfigBuilder withConfig(final String key, final String value) {
        this.config.put(requireNonNull(key, "key"), requireNonNull(value, "value"));
        return this;
    }

    public TopicConfigBuilder withInfiniteRetention() {
        return withConfig("retention.ms", "-1");
    }

    public KafkaTopicConfig build() {
        return new KafkaTopicConfigImpl(partitions, config);
    }

    private static final class KafkaTopicConfigImpl implements KafkaTopicConfig {

        private final int partitions;
        private final Map<String, String> config;

        KafkaTopicConfigImpl(final int partitions, final Map<String, String> config) {
            this.partitions = partitions;
            this.config = Map.copyOf(requireNonNull(config, "config"));
        }

        @Override
        public int partitions() {
            return partitions;
        }

        @Override
        public Map<String, String> config() {
            return config;
        }
    }
}
