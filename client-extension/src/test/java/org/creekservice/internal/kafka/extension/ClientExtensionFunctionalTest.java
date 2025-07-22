/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.extension;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.test.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.creekservice.test.TopicDescriptors.outputTopic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.KafkaClientsExtensionOptions;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.platform.metadata.AggregateDescriptor;
import org.creekservice.api.platform.metadata.ComponentInput;
import org.creekservice.api.platform.metadata.ComponentInternal;
import org.creekservice.api.platform.metadata.ComponentOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SuppressWarnings("SameParameterValue")
@Testcontainers
@Tag("ContainerisedTest")
@Execution(ExecutionMode.SAME_THREAD) // Due to static state
class ClientExtensionFunctionalTest {

    @Container
    private static final KafkaContainer KAFKA_CLUSTER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"))
                    .withStartupAttempts(3)
                    .withStartupTimeout(Duration.ofSeconds(90))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private static CreekContext creek;
    private static KafkaClientsExtension ext;
    private static Admin admin;

    @BeforeAll
    static void beforeAll() {
        creek =
                CreekServices.builder(new KafkaServiceDescriptor())
                        .with(
                                KafkaClientsExtensionOptions.builder()
                                        .withKafkaProperty(
                                                DEFAULT_CLUSTER_NAME,
                                                BOOTSTRAP_SERVERS_CONFIG,
                                                KAFKA_CLUSTER.getBootstrapServers())
                                        .withKafkaProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
                                        .withKafkaProperty(
                                                GROUP_ID_CONFIG, UUID.randomUUID().toString())
                                        .build())
                        .build();

        ext = creek.extension(KafkaClientsExtension.class);

        admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER.getBootstrapServers()));
    }

    @AfterAll
    static void tearDown() {
        creek.close();
        admin.close();
    }

    @Test
    void shouldHaveCreatedOwnedTopics() {
        assertThat(KafkaServiceDescriptor.OutputTopic, topicExists());
    }

    @Test
    void shouldNotHaveCreatedUnownedTopics() {
        assertThat(KafkaServiceDescriptor.InputTopic, not(topicExists()));
    }

    @Test
    void shouldProduceAndConsumeToKafkaTopic() {
        // Given:
        final OwnedKafkaTopicOutput<Long, String> topicDef = KafkaServiceDescriptor.OutputTopic;
        final long key = 101L;
        final String value = "val";

        // When:
        produceToTopic(topicDef, key, value);
        final ConsumerRecord<byte[], byte[]> record = consumeFromTopic(topicDef);

        // Then:
        final KafkaTopic<Long, String> topic = ext.topic(topicDef);
        assertThat(topic.deserializeKey(record.key()), is(key));
        assertThat(topic.deserializeValue(record.value()), is(value));
    }

    private void produceToTopic(
            final KafkaTopicDescriptor<Long, String> topicDef, final long key, final String value) {
        final KafkaTopic<Long, String> topic = ext.topic(topicDef);
        final Producer<byte[], byte[]> producer = ext.producer();
        producer.send(
                new ProducerRecord<>(
                        topic.name(), topic.serializeKey(key), topic.serializeValue(value)));
        producer.flush();
    }

    private ConsumerRecord<byte[], byte[]> consumeFromTopic(
            final CreatableKafkaTopic<Long, String> topicDef) {
        final KafkaTopic<Long, String> topic = ext.topic(topicDef);

        final List<TopicPartition> tps =
                IntStream.range(0, topicDef.config().partitions())
                        .mapToObj(p -> new TopicPartition(topic.name(), p))
                        .collect(Collectors.toList());

        final Consumer<byte[], byte[]> consumer = ext.consumer();
        consumer.assign(tps);

        for (int i = 0; i != 30; ++i) {
            final Iterator<ConsumerRecord<byte[], byte[]>> result =
                    consumer.poll(Duration.ofSeconds(1)).records(topic.name()).iterator();

            if (result.hasNext()) {
                return result.next();
            }
        }

        throw new AssertionError("Timed out waiting for record");
    }

    private static Matcher<? super KafkaTopicDescriptor<?, ?>> topicExists() {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(
                    final KafkaTopicDescriptor<?, ?> item, final Description mismatchDescription) {
                if (!item.cluster().equals(DEFAULT_CLUSTER_NAME)) {
                    mismatchDescription
                            .appendText("wrong cluster. Expected: ")
                            .appendValue(DEFAULT_CLUSTER_NAME)
                            .appendText(", but was ")
                            .appendValue(item.cluster());
                    return false;
                }

                try {
                    final Set<String> topics = admin.listTopics().names().get();
                    final boolean exists = topics.contains(item.name());
                    if (!exists) {
                        mismatchDescription
                                .appendText("topic does not exist. Existing topics are: ")
                                .appendValue(topics);
                        return false;
                    }
                    return true;
                } catch (final Exception e) {
                    mismatchDescription
                            .appendText("An exception was thrown querying the Kafka cluster")
                            .appendValue(e);
                    return false;
                }
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("topic exists");
            }
        };
    }

    private static final class UpstreamDescriptor implements AggregateDescriptor {

        private static final List<ComponentOutput> OUTPUTS = new ArrayList<>();

        public static final OwnedKafkaTopicOutput<String, Long> Output =
                register(outputTopic("input", String.class, Long.class, withPartitions(3)));

        UpstreamDescriptor() {}

        @Override
        public Collection<ComponentOutput> outputs() {
            return List.copyOf(OUTPUTS);
        }

        private static <T extends ComponentOutput> T register(final T output) {
            OUTPUTS.add(output);
            return output;
        }
    }

    private static final class KafkaServiceDescriptor implements ServiceDescriptor {

        private static final List<ComponentInput> INPUTS = new ArrayList<>();
        private static final List<ComponentInternal> INTERNALS = new ArrayList<>();
        private static final List<ComponentOutput> OUTPUTS = new ArrayList<>();

        public static final KafkaTopicInput<String, Long> InputTopic =
                register(UpstreamDescriptor.Output.toInput());

        public static final OwnedKafkaTopicOutput<Long, String> OutputTopic =
                register(outputTopic("output", Long.class, String.class, withPartitions(1)));

        @Override
        public String dockerImage() {
            return "service never started";
        }

        @Override
        public Collection<ComponentInput> inputs() {
            return List.copyOf(INPUTS);
        }

        @Override
        public Collection<ComponentInternal> internals() {
            return List.copyOf(INTERNALS);
        }

        @Override
        public Collection<ComponentOutput> outputs() {
            return List.copyOf(OUTPUTS);
        }

        private static <T extends ComponentInput> T register(final T input) {
            INPUTS.add(input);
            return input;
        }

        private static <T extends ComponentOutput> T register(final T output) {
            OUTPUTS.add(output);
            return output;
        }
    }
}
