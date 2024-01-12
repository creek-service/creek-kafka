/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;

final class TopicConsumers {

    private final Consumer<byte[], byte[]> consumer;
    private final Map<String, TopicInfo> topics;
    private final TopicConsumerFactory consumerFactory;

    TopicConsumers(
            final Map<String, KafkaTopic<?, ?>> topics, final Consumer<byte[], byte[]> consumer) {
        this(topics, consumer, TopicConsumer::new);
    }

    @VisibleForTesting
    TopicConsumers(
            final Map<String, KafkaTopic<?, ?>> topics,
            final Consumer<byte[], byte[]> consumer,
            final TopicConsumerFactory consumerFactory) {
        this.consumer = requireNonNull(consumer, "consumer");
        this.topics = buildTopics(topics, consumer);
        this.consumerFactory = requireNonNull(consumerFactory, "consumerFactory");
    }

    public TopicConsumer get(final String topicName) {
        final TopicInfo topicInfo = topics.get(topicName);
        final TopicConsumer consumer = consumerFactory.create(topicInfo.topic, this.consumer);
        consumer.assignAndSeek(topicInfo.endOffsets);
        return consumer;
    }

    private static Map<String, TopicInfo> buildTopics(
            final Map<String, KafkaTopic<?, ?>> topics, final Consumer<byte[], byte[]> consumer) {
        final Map<String, Map<TopicPartition, Long>> endOffsets =
                endOffsets(consumer, topics.keySet());

        return topics.entrySet().stream()
                .collect(
                        toUnmodifiableMap(
                                Map.Entry::getKey,
                                e -> new TopicInfo(e.getValue(), endOffsets.get(e.getKey()))));
    }

    private static Map<String, Map<TopicPartition, Long>> endOffsets(
            final Consumer<byte[], byte[]> consumer, final Set<String> topicNames) {

        final List<TopicPartition> partitions =
                topicNames.stream()
                        .flatMap(topic -> partitionsFor(topic, consumer))
                        .collect(toList());

        return consumer.endOffsets(partitions).entrySet().stream()
                .collect(
                        groupingBy(
                                e -> e.getKey().topic(),
                                toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private static Stream<TopicPartition> partitionsFor(
            final String topic, final Consumer<?, ?> consumer) {
        final List<PartitionInfo> pis = consumer.partitionsFor(topic);
        if (pis == null) {
            throw new UnknownTopicOrPartitionException("Unknown topic: " + topic);
        }
        return pis.stream().map(pi -> new TopicPartition(pi.topic(), pi.partition()));
    }

    private static final class TopicInfo {
        private final KafkaTopic<?, ?> topic;
        private final Map<TopicPartition, Long> endOffsets;

        TopicInfo(final KafkaTopic<?, ?> topic, final Map<TopicPartition, Long> endOffsets) {
            this.topic = requireNonNull(topic, "topic");
            this.endOffsets = Map.copyOf(requireNonNull(endOffsets, "endOffsets"));
        }
    }

    @VisibleForTesting
    interface TopicConsumerFactory {
        TopicConsumer create(KafkaTopic<?, ?> topic, Consumer<byte[], byte[]> consumer);
    }
}
