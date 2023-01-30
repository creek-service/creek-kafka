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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TopicConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicExpectationHandler.class);

    private final KafkaTopic<?, ?> topic;
    private final Consumer<byte[], byte[]> consumer;
    private final Clock clock;

    TopicConsumer(final KafkaTopic<?, ?> topic, final Consumer<byte[], byte[]> consumer) {
        this(topic, consumer, Clock.systemUTC());
    }

    @VisibleForTesting
    TopicConsumer(
            final KafkaTopic<?, ?> topic,
            final Consumer<byte[], byte[]> consumer,
            final Clock clock) {
        this.topic = requireNonNull(topic, "topic");
        this.consumer = requireNonNull(consumer, "consumer");
        this.clock = requireNonNull(clock, "clock");
    }

    void assignAndSeek(final Map<TopicPartition, Long> startOffsets) {
        try {
            consumer.assign(startOffsets.keySet());
        } catch (final Exception e) {
            throw new KafkaClientException("Failed to assign topic partitions", e);
        }

        try {
            startOffsets.forEach(consumer::seek);
        } catch (final Exception e) {
            throw new KafkaClientException("Failed to seek topic partition to starting offset", e);
        }

        LOGGER.info(
                "Consuming expected output from "
                        + topic.name()
                        + ", with starting offsets: "
                        + startOffsets);
    }

    List<ConsumedRecord> consume(final long minRecords, final Instant end) {
        final List<ConsumedRecord> consumed = new ArrayList<>();

        while (consumed.size() < minRecords && clock.instant().isBefore(end)) {
            final ConsumerRecords<byte[], byte[]> records = poll();

            records.forEach(
                    record -> {
                        final ConsumedRecord consumedRecord =
                                new ConsumedRecord(
                                        record, deserializeKey(record), deserializeValue(record));

                        LOGGER.debug("Consumed: " + consumedRecord);
                        consumed.add(consumedRecord);
                    });
        }
        return consumed;
    }

    private ConsumerRecords<byte[], byte[]> poll() {
        try {
            return consumer.poll(Duration.ofSeconds(1));
        } catch (final Exception e) {
            throw new KafkaClientException("Failed to consume records from Kafka", e);
        }
    }

    private Optional<?> deserializeKey(final ConsumerRecord<byte[], byte[]> record) {
        try {
            return Optional.ofNullable(topic.deserializeKey(record.key()));
        } catch (final Exception e) {
            throw new DeserializationException("key", record, e);
        }
    }

    private Optional<?> deserializeValue(final ConsumerRecord<byte[], byte[]> record) {
        try {
            return Optional.ofNullable(topic.deserializeValue(record.value()));
        } catch (final Exception e) {
            throw new DeserializationException("value", record, e);
        }
    }

    private static final class KafkaClientException extends RuntimeException {
        KafkaClientException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }

    private static final class DeserializationException extends RuntimeException {
        DeserializationException(
                final String fieldName, final ConsumerRecord<?, ?> record, final Throwable cause) {
            super(
                    "Failed to deserialize record "
                            + fieldName
                            + ". topic: "
                            + record.topic()
                            + ", partition: "
                            + record.partition()
                            + ", offset: "
                            + record.offset(),
                    cause);
        }
    }
}
