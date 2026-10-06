/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.creekservice.api.base.annotation.VisibleForTesting;

final class TopicConsumers {

    private final Consumer<byte[], byte[]> consumer;
    private final Map<String, TopicInfo> topics;
    private final TopicConsumerFactory consumerFactory;

    TopicConsumers(
            final Map<String, TestKafkaTopic> topics,
            final Consumer<byte[], byte[]> consumer,
            final SeekOffsetOverrides seekOverrides) {
        this(topics, consumer, seekOverrides, TopicConsumer::new);
    }

    @VisibleForTesting
    TopicConsumers(
            final Map<String, TestKafkaTopic> topics,
            final Consumer<byte[], byte[]> consumer,
            final SeekOffsetOverrides seekOverrides,
            final TopicConsumerFactory consumerFactory) {
        this.consumer = requireNonNull(consumer, "consumer");
        this.topics = buildTopics(topics, consumer, seekOverrides);
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory");
    }

    public TopicConsumer get(final String topicName) {
        final TopicInfo topicInfo = topics.get(topicName);
        final TopicConsumer topicConsumer = consumerFactory.create(topicInfo.topic, consumer);
        topicConsumer.assignAndSeek(topicInfo.seekOffsets);
        return topicConsumer;
    }

    private static Map<String, TopicInfo> buildTopics(
            final Map<String, TestKafkaTopic> topics,
            final Consumer<byte[], byte[]> consumer,
            final SeekOffsetOverrides seekOverrides) {
        final Map<String, Map<TopicPartition, Long>> endOffsets =
                seekOffsets(consumer, topics.keySet(), seekOverrides);

        return topics.entrySet().stream()
                .collect(
                        toUnmodifiableMap(
                                Map.Entry::getKey,
                                e -> new TopicInfo(e.getValue(), endOffsets.get(e.getKey()))));
    }

    private static Map<String, Map<TopicPartition, Long>> seekOffsets(
            final Consumer<byte[], byte[]> consumer,
            final Set<String> topicNames,
            final SeekOffsetOverrides seekOverrides) {

        final Map<Boolean, List<String>> partitioned =
                topicNames.stream().collect(Collectors.partitioningBy(seekOverrides::hasOverrides));

        final List<String> topicsWithOverrides = partitioned.getOrDefault(true, List.of());
        final List<String> topicsWithoutOverrides = partitioned.getOrDefault(false, List.of());

        final Map<String, Map<TopicPartition, Long>> offsets =
                endOffsets(consumer, topicsWithoutOverrides);

        final Map<String, Map<TopicPartition, Long>> overriddenOffsets =
                topicsWithOverrides.stream()
                        .collect(
                                toMap(
                                        Function.identity(),
                                        topic ->
                                                overriddenOffsets(topic, seekOverrides, consumer)));

        offsets.putAll(overriddenOffsets);

        return offsets;
    }

    private static Map<String, Map<TopicPartition, Long>> endOffsets(
            final Consumer<byte[], byte[]> consumer, final List<String> topicsWithOverrides) {
        final List<TopicPartition> endOffsetPartitions =
                topicsWithOverrides.stream()
                        .map(topic -> partitionsFor(topic, consumer))
                        .flatMap(List::stream)
                        .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                        .toList();

        return consumer.endOffsets(endOffsetPartitions).entrySet().stream()
                .collect(
                        groupingBy(
                                e -> e.getKey().topic(),
                                toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static Map<TopicPartition, Long> overriddenOffsets(
            final String topic,
            final SeekOffsetOverrides seekOverrides,
            final Consumer<byte[], byte[]> consumer) {
        return seekOverrides.get(topic, partitionsFor(topic, consumer).size());
    }

    private static List<PartitionInfo> partitionsFor(
            final String topic, final Consumer<?, ?> consumer) {
        final List<PartitionInfo> pis = consumer.partitionsFor(topic);
        if (pis == null) {
            throw new UnknownTopicOrPartitionException("Unknown topic: " + topic);
        }
        return pis;
    }

    private record TopicInfo(TestKafkaTopic topic, Map<TopicPartition, Long> seekOffsets) {
        private TopicInfo {
            seekOffsets = Map.copyOf(requireNonNull(seekOffsets, "seekOffsets"));
        }
    }

    @VisibleForTesting
    interface TopicConsumerFactory {
        TopicConsumer create(TestKafkaTopic testTopic, Consumer<byte[], byte[]> consumer);
    }
}
