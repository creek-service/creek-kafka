/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.serde.test;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.creekservice.api.platform.metadata.ComponentInput;
import org.creekservice.api.platform.metadata.ComponentInternal;
import org.creekservice.api.platform.metadata.ComponentOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/**
 * Test fixture for use by serde implementations perform functional testing.
 *
 * <p>The fixture:
 *
 * <ul>
 *   <li>will create the necessary Kafka Docker instance(s).
 *   <li>initialize Creek, which will initialize any owned topics.
 *   <li>provides functionality to test producing and consuming.
 *   <li>provides functionality to test schema evolution.
 * </ul>
 *
 * <p>This is also used internally, so take a look at the code in this repo to see examples of how
 * this class is used.
 */
public final class KafkaSerdeProviderFunctionalFixture {

    private static final DockerImageName KAFKA_IMAGE_NAME =
            DockerImageName.parse("confluentinc/cp-kafka:7.3.1");
    private static final int CONTAINER_STARTUP_ATTEMPTS = 3;
    private static final Duration CONTAINER_STARTUP_TIMEOUT = Duration.ofSeconds(90);

    private final Network network = Network.newNetwork();
    private final Map<String, KafkaContainer> brokerByCluster;
    private final List<CreatableKafkaTopic<?, ?>> topics;
    private final Map<Class<?>, Object> typeOverrides = new HashMap<>();
    private final List<Tester> testers = new ArrayList<>();

    /**
     * Create a functional tester that will initialize the supplied {@code topics}.
     *
     * @param topics the topics to initialise.
     * @return the tester.
     */
    public static KafkaSerdeProviderFunctionalFixture tester(
            final Collection<? extends CreatableKafkaTopic<?, ?>> topics) {
        return new KafkaSerdeProviderFunctionalFixture(topics);
    }

    private KafkaSerdeProviderFunctionalFixture(
            final Collection<? extends CreatableKafkaTopic<?, ?>> topicDescriptors) {
        this.topics = List.copyOf(requireNonNull(topicDescriptors, "topicDescriptors"));
        this.brokerByCluster =
                topics.stream()
                        .map(CreatableKafkaTopic::cluster)
                        .distinct()
                        .collect(
                                toMap(
                                        Function.identity(),
                                        clusterName ->
                                                new KafkaContainer(KAFKA_IMAGE_NAME)
                                                        .withNetwork(network)
                                                        .withNetworkAliases(
                                                                clusterName + "-kafka-broker")
                                                        .withStartupAttempts(
                                                                CONTAINER_STARTUP_ATTEMPTS)
                                                        .withStartupTimeout(
                                                                CONTAINER_STARTUP_TIMEOUT)
                                                        .withEnv(
                                                                "KAFKA_AUTO_CREATE_TOPICS_ENABLE",
                                                                "false")));
    }

    public <T> KafkaSerdeProviderFunctionalFixture withTypeOverride(
            final Class<T> type, final T instance) {
        this.typeOverrides.put(type, instance);
        return this;
    }

    /**
     * Bring up the necessary Docker containers for Kafka clusters and initialise
     *
     * @return {@link Tester} instance that can be used to test serde operations.
     */
    public Tester start() {
        brokerByCluster.values().forEach(KafkaContainer::start);
        return new Tester(topics);
    }

    /** Release all resources help, and stop all containers started, by the fixture. */
    public void stop() {
        testers.forEach(Tester::close);
        testers.clear();
        brokerByCluster.values().forEach(KafkaContainer::stop);
        brokerByCluster.clear();
    }

    public KafkaContainer kafkaContainer(final String clusterName) {
        final KafkaContainer found = brokerByCluster.get(clusterName);
        if (found == null) {
            throw new IllegalArgumentException("Unknown cluster: " + clusterName);
        }
        return found;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaClientsExtensionOptions options() {
        final KafkaClientsExtensionOptions.Builder options =
                KafkaClientsExtensionOptions.builder()
                        .withKafkaProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
                        .withKafkaProperty(GROUP_ID_CONFIG, UUID.randomUUID().toString());

        typeOverrides.forEach((k, v) -> options.withTypeOverride((Class) k, v));

        brokerByCluster.forEach(
                (cluster, broker) ->
                        options.withKafkaProperty(
                                cluster, BOOTSTRAP_SERVERS_CONFIG, broker.getBootstrapServers()));

        return options.build();
    }

    private static final class TestServiceDescriptor implements ServiceDescriptor {

        private final List<CreatableKafkaTopic<?, ?>> topics;

        TestServiceDescriptor(final Collection<? extends CreatableKafkaTopic<?, ?>> topics) {
            this.topics = List.copyOf(topics);
        }

        @Override
        public String dockerImage() {
            return "not image - test descriptor";
        }

        @Override
        public Collection<ComponentInput> inputs() {
            return topics(ComponentInput.class);
        }

        @Override
        public Collection<ComponentInternal> internals() {
            return topics(ComponentInternal.class);
        }

        @Override
        public Collection<ComponentOutput> outputs() {
            return topics(ComponentOutput.class);
        }

        private <T> Collection<T> topics(final Class<T> type) {
            return topics.stream()
                    .filter(type::isInstance)
                    .map(type::cast)
                    .collect(Collectors.toList());
        }
    }

    public final class Tester implements Closeable {

        private final CreekContext creek;
        private final KafkaClientsExtension extension;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private Tester(final Collection<? extends CreatableKafkaTopic<?, ?>> topics) {
            this.creek =
                    CreekServices.builder(new TestServiceDescriptor(topics))
                            .with(options())
                            .build();
            this.extension = creek.extension(KafkaClientsExtension.class);
            testers.add(this);
        }

        public <K, V> KafkaTopic<K, V> topic(final KafkaTopicDescriptor<K, V> descriptor) {
            if (extension == null) {
                throw new IllegalStateException("Not started");
            }
            return extension.topic(descriptor);
        }

        /**
         * Produce a single record to a topic
         *
         * @param descriptor the descriptor of the topic to produce to
         * @param key the key to produce
         * @param value the value to produce
         * @param <K> the key type
         * @param <V> the value type
         */
        public <K, V> void produce(
                final CreatableKafkaTopic<K, V> descriptor, final K key, final V value) {

            final KafkaTopic<K, V> topic = topic(descriptor);

            produceToTopic(topic, key, value);
        }

        /**
         * Consume a single record from a topic
         *
         * @param descriptor the descriptor of the topic to consume from
         * @param key the expected key
         * @param value the expected value
         * @param <K> the key type
         * @param <V> the value type
         */
        public <K, V> void consume(
                final CreatableKafkaTopic<K, V> descriptor, final K key, final V value) {

            final KafkaTopic<K, V> topic = topic(descriptor);
            final ConsumerRecord<byte[], byte[]> record = consumeFromTopic(topic);

            assertEqual(topic.deserializeKey(record.key()), key);
            assertEqual(topic.deserializeValue(record.value()), value);
        }

        /**
         * Test the serde is set up correctly to produce and consume from the supplied topic {@code
         * descriptor}.
         *
         * <p>The {@code key} and {@code value} types must implement a meaningful {@link
         * Object#equals} method, as this method is used to check equality on the consumed key and
         * value with those supplied.
         *
         * @param descriptor the topic descriptor to produce to, and consume from.
         * @param key the key to produce, and the expected key on consume.
         * @param value the value to produce, and the expected value on consume.
         * @param <K> the type of the key. Type must implement a meaningful {@link Object#equals}
         *     method.
         * @param <V> the type of the value. Type must implement a meaningful {@link Object#equals}
         *     method.
         */
        public <K, V> void testProduceConsume(
                final CreatableKafkaTopic<K, V> descriptor, final K key, final V value) {

            produce(descriptor, key, value);
            consume(descriptor, key, value);
        }

        /**
         * Test evolving one of the topics used to create the fixture.
         *
         * @param evolvedTopic the evolved topic descriptor
         * @return {@link Tester} instance that can be used to test serde operations using the
         *     evolved topic.
         */
        public Tester testEvolution(final CreatableKafkaTopic<?, ?> evolvedTopic) {
            return new Tester(List.of(evolvedTopic));
        }

        public void close() {
            if (closed.compareAndSet(false, true)) {
                creek.close();
            }
        }

        private <V, K> void produceToTopic(
                final KafkaTopic<K, V> topic, final K key, final V value) {
            final Producer<byte[], byte[]> producer =
                    extension.producer(topic.descriptor().cluster());
            producer.send(
                    new ProducerRecord<>(
                            topic.name(), topic.serializeKey(key), topic.serializeValue(value)));
            producer.flush();
        }

        private ConsumerRecord<byte[], byte[]> consumeFromTopic(final KafkaTopic<?, ?> topic) {
            final Consumer<byte[], byte[]> consumer =
                    extension.consumer(topic.descriptor().cluster());

            final List<TopicPartition> tps =
                    IntStream.range(
                                    0,
                                    ((CreatableKafkaTopic<?, ?>) topic.descriptor())
                                            .config()
                                            .partitions())
                            .mapToObj(p -> new TopicPartition(topic.name(), p))
                            .collect(Collectors.toList());

            consumer.assign(tps);

            for (int i = 0; i != 30; ++i) {
                final Iterator<ConsumerRecord<byte[], byte[]>> result =
                        consumer.poll(Duration.ofSeconds(1)).records(topic.name()).iterator();

                if (result.hasNext()) {
                    return result.next();
                }
            }

            throw new AssertionError("Timed out waiting for record in " + topic.name());
        }

        private <T> void assertEqual(final T actual, final T expected) {
            if (!expected.equals(actual)) {
                throw new AssertionError(
                        "Key mismatch."
                                + System.lineSeparator()
                                + "Expected: "
                                + expected
                                + System.lineSeparator()
                                + "Got: "
                                + actual);
            }
        }
    }
}
