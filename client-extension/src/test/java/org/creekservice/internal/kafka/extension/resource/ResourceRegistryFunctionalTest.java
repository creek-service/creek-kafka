/*
 * Copyright 2022 Creek Contributors (https://github.com/creek-service)
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

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.api.kafka.test.service.TestServiceDescriptor.InputTopic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.test.service.TestServiceDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ResourceRegistryFunctionalTest {

    @Container
    private static final KafkaContainer KAFKA_CLUSTER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.4"))
                    .withStartupAttempts(3)
                    .withStartupTimeout(Duration.ofSeconds(90))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

    private KafkaTopic<String, Long> topic;

    @BeforeEach
    void setUp() {
        final ClustersProperties clustersProperties =
                ClustersProperties.propertiesBuilder()
                        .putCommon(BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.getBootstrapServers())
                        .putCommon(AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .putCommon(GROUP_ID_CONFIG, UUID.randomUUID().toString())
                        .build();

        final ResourceRegistry registry =
                new ResourceRegistryFactory()
                        .create(List.of(new TestServiceDescriptor()), clustersProperties);

        try (Admin admin = Admin.create(clustersProperties.get(DEFAULT_CLUSTER_NAME))) {
            ensureTopics(admin, InputTopic);
        }

        topic = registry.topic(InputTopic);
    }

    @Test
    void shouldProduceAndConsumeToKafkaTopic() {
        // When:
        produceToTopic();
        final ConsumerRecord<String, Long> record = consumeFromTopic();

        // Then:
        assertThat(record.key(), is("key"));
        assertThat(record.value(), is(100L));
    }

    private void produceToTopic() {
        try (Producer<String, Long> producer = topic.producer()) {
            producer.send(new ProducerRecord<>(InputTopic.name(), "key", 100L));
        }
    }

    private ConsumerRecord<String, Long> consumeFromTopic() {
        try (Consumer<String, Long> consumer = topic.consumer()) {
            final List<TopicPartition> tps =
                    IntStream.range(0, InputTopic.config().partitions())
                            .mapToObj(p -> new TopicPartition(InputTopic.name(), p))
                            .collect(Collectors.toList());

            consumer.assign(tps);

            for (int i = 0; i != 30; ++i) {
                final Iterator<ConsumerRecord<String, Long>> result =
                        consumer.poll(Duration.ofSeconds(1)).records(InputTopic.name()).iterator();

                if (result.hasNext()) {
                    return result.next();
                }
            }

            throw new AssertionError("Timed out waiting for record");
        }
    }

    private void ensureTopics(final Admin admin, final CreatableKafkaTopic<?, ?>... topics) {
        final List<NewTopic> newTopics =
                Arrays.stream(topics)
                        .map(
                                topic ->
                                        new NewTopic(
                                                        topic.name(),
                                                        Optional.of(topic.config().partitions()),
                                                        Optional.empty())
                                                .configs(topic.config().config()))
                        .collect(Collectors.toList());

        try {
            admin.createTopics(newTopics).all().get();
        } catch (Exception e) {
            throw new AssertionError("Failed to create topics", e);
        }
    }
}
