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

package com.acme.examples.service;

import org.creekservice.api.kafka.metadata.topic.KafkaTopicConfig;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused") // What is unused today may be used tomorrow...
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

    public TopicConfigBuilder withConfigs(final Map<String, String> config) {
        config.forEach(this::withConfig);
        return this;
    }

    public TopicConfigBuilder withKeyCompaction() {
        return withConfig("cleanup.policy", "compact");
    }

    public TopicConfigBuilder withKeyCompactionAndDeletion() {
        return withConfig("cleanup.policy", "compact,delete");
    }

    public TopicConfigBuilder withRetentionTime(final Duration duration) {
        return withConfig("retention.ms", String.valueOf(duration.toMillis()));
    }

    public TopicConfigBuilder withInfiniteRetention() {
        return withConfig("retention.ms", "-1");
    }

    public TopicConfigBuilder withSegmentSize(final long segmentBytes) {
        return withConfig("segment.bytes", String.valueOf(segmentBytes));
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
