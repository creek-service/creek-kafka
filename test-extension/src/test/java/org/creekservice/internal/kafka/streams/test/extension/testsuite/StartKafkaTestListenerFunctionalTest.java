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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.api.kafka.test.service.inbuilt.NativeServiceDescriptor.InputTopic;
import static org.creekservice.api.kafka.test.service.inbuilt.NativeServiceDescriptor.OutputTopic;
import static org.creekservice.api.system.test.test.util.CreekSystemTestExtensionTester.tester;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstanceContainer;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.api.system.test.test.util.CreekSystemTestExtensionTester;
import org.creekservice.internal.kafka.streams.test.extension.ClusterEndpointsProvider;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.internal.stubbing.defaultanswers.ReturnsDeepStubs;
import org.testcontainers.DockerClientFactory;

@Tag("ContainerisedTest")
@Execution(ExecutionMode.SAME_THREAD) // Due to static state
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StartKafkaTestListenerFunctionalTest {

    private static final CreekSystemTestExtensionTester EXT_TESTER =
            tester()
                    // Uncomment the line below to debug the test service using attach me plugin:
                    // .withDebugServices("native-service")
                    .build();

    private static StartKafkaTestListener listener;
    private static ConfigurableServiceInstance testService;
    private static final ClusterEndpointsProvider clusterEndpointsProvider =
            new ClusterEndpointsProvider();

    private final DockerClient dockerClient = DockerClientFactory.lazyClient();

    @BeforeAll
    static void beforeAll() {
        final ServiceInstanceContainer services = EXT_TESTER.dockerServicesContainer();

        final CreekSystemTest api =
                mock(CreekSystemTest.class, withSettings().defaultAnswer(new ReturnsDeepStubs()));

        when(api.tests().env().currentSuite().services().add(any()))
                .thenAnswer(inv -> services.add(inv.getArgument(0)));

        testService = services.add(EXT_TESTER.serviceDefinitions().get("native-service"));

        when(api.tests().env().currentSuite().services().stream())
                .thenReturn(Stream.of(testService));

        listener = new StartKafkaTestListener(api, clusterEndpointsProvider);
    }

    @AfterAll
    static void afterAll() {
        EXT_TESTER.dockerServicesContainer().forEach(ServiceInstance::stop);
        EXT_TESTER.clearServices();
    }

    @Test
    @Order(1)
    void shouldStartKafka() {
        // Given:
        final CreekTestSuite suite = mock(CreekTestSuite.class);
        final KafkaOptions options = mock(KafkaOptions.class);
        when(suite.options(KafkaOptions.class)).thenReturn(List.of(options));
        when(options.kafkaDockerImage()).thenReturn("confluentinc/cp-kafka:7.1.4");

        // When:
        listener.beforeSuite(suite);

        // Then:
        assertThat(serviceInstance("kafka-default-0").running(), is(true));
        assertThat(EXT_TESTER.runningContainerIds().get("kafka-default-0"), is(running()));
        assertThat(
                dockerImageName(EXT_TESTER.runningContainerIds().get("kafka-default-0")),
                is("confluentinc/cp-kafka:7.1.4"));
    }

    @Test
    void shouldPopulateClusterEndPoints() {
        // Given:
        clusterEndpointsProvider.init(Set.of());
        final String bootstrapServers = (String) baseProps().get(BOOTSTRAP_SERVERS_CONFIG);
        assertThat(bootstrapServers, is(notNullValue()));

        // Then:
        assertThat(
                clusterEndpointsProvider.get(DEFAULT_CLUSTER_NAME),
                is(Map.of(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)));
    }

    @Test
    void shouldBeAbleToProduceAndConsumeFromTestNetwork() {
        // Given:
        givenTopic("test-topic");

        try (KafkaConsumer<String, String> consumer = kafkaConsumer("test-topic", String.class)) {

            produce("test-topic", "key", "value");

            // Then:
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records.count(), is(1));
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertThat(record.key(), is("key"));
            assertThat(record.value(), is("value"));
        }
    }

    @Test
    void shouldBeAbleToProduceAndConsumeFromServiceNetwork() {
        // Given:
        givenTopic(InputTopic.name());

        try (KafkaConsumer<Long, String> consumer = kafkaConsumer(OutputTopic)) {

            testService.start();

            // When:
            produce(InputTopic.name(), "k", 100L);

            // Then:
            final ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(60));

            assertThat(records.count(), is(1));
            final ConsumerRecord<Long, String> record = records.iterator().next();
            assertThat(record.key(), is(100L));
            assertThat(record.value(), is("k"));
        }
    }

    @Test
    @Order(Integer.MAX_VALUE)
    void shouldShutdownKafkaAfterSuite() {
        // Given:
        final String kafkaContainerId = EXT_TESTER.runningContainerIds().get("kafka-default-0");

        // When:
        listener.afterSuite(null, null);

        // Then:
        assertThat(serviceInstance("kafka-default-0").running(), is(false));
        assertThat(kafkaContainerId, is(not(running())));
    }

    private Map<String, Object> baseProps() {
        final ServiceInstance instance =
                EXT_TESTER.dockerServicesContainer().get("kafka-default-0");
        final String testNetworkBootstrap =
                instance.testNetworkHostname()
                        + ":"
                        + instance.testNetworkPort(KafkaContainerDef.TEST_NETWORK_PORT);

        final Map<String, Object> baseProps = new HashMap<>();
        baseProps.put(BOOTSTRAP_SERVERS_CONFIG, testNetworkBootstrap);
        return baseProps;
    }

    @SuppressFBWarnings(
            value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "https://github.com/spotbugs/spotbugs/issues/756")
    private void givenTopic(final String name) {
        try (Admin adminClient = Admin.create(baseProps())) {
            adminClient
                    .createTopics(List.of(new NewTopic(name, 1, (short) 1)))
                    .all()
                    .get(1, TimeUnit.HOURS);
        } catch (ExecutionException | TimeoutException | InterruptedException e) {
            throw new AssertionError("Failed to create topic", e);
        }
    }

    private <V> void produce(final String topic, final String key, final V value) {
        try (KafkaProducer<String, V> producer = kafkaProducer(value)) {
            producer.send(new ProducerRecord<>(topic, key, value));
            producer.flush();
        } catch (final Exception e) {
            throw new AssertionError("Failed to produce", e);
        }
    }

    private <V> KafkaProducer<String, V> kafkaProducer(final V value) {
        final Map<String, Object> producerProps = baseProps();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer(value));

        return new KafkaProducer<>(producerProps);
    }

    private Class<? extends Serializer<?>> serializer(final Object value) {
        if (value instanceof String) {
            return StringSerializer.class;
        }
        if (value instanceof Long) {
            return LongSerializer.class;
        }
        throw new IllegalArgumentException("Unsupported: " + value);
    }

    @SuppressWarnings("SameParameterValue")
    private <K> KafkaConsumer<K, String> kafkaConsumer(
            final KafkaTopicDescriptor<K, String> topic) {
        return kafkaConsumer(topic.name(), topic.key().type());
    }

    private <K> KafkaConsumer<K, String> kafkaConsumer(
            final String topicName, final Class<K> keyType) {
        final Map<String, Object> consumerProps = baseProps();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer(keyType));
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        final KafkaConsumer<K, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(topicName));
        return consumer;
    }

    private Class<? extends Deserializer<?>> deserializer(final Class<?> valueType) {
        if (valueType.equals(String.class)) {
            return StringDeserializer.class;
        }
        if (valueType.equals(Long.class)) {
            return LongDeserializer.class;
        }
        throw new IllegalArgumentException("Unsupported type: " + valueType);
    }

    @SuppressWarnings("SameParameterValue")
    private static ServiceInstance serviceInstance(final String instanceName) {
        return EXT_TESTER.dockerServicesContainer().stream()
                .filter(i -> Objects.equals(i.name(), instanceName))
                .findAny()
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "No instance with name: "
                                                + instanceName
                                                + ", only: "
                                                + EXT_TESTER.dockerServicesContainer().stream()
                                                        .map(ServiceInstance::name)
                                                        .collect(Collectors.toList())));
    }

    private String dockerImageName(final String containerId) {
        final InspectContainerResponse response =
                dockerClient.inspectContainerCmd(containerId).exec();

        return response.getConfig().getImage();
    }

    private Matcher<String> running() {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(
                    final String containerId, final Description mismatchDescription) {
                try {
                    final InspectContainerResponse response =
                            dockerClient.inspectContainerCmd(containerId).exec();
                    if (Boolean.FALSE.equals(response.getState().getRunning())) {
                        mismatchDescription
                                .appendText("Container with id ")
                                .appendValue(containerId)
                                .appendText(" is not running");
                        return false;
                    }
                    return true;
                } catch (final NotFoundException e) {
                    mismatchDescription
                            .appendText("Container with id ")
                            .appendValue(containerId)
                            .appendText(" no longer exists");
                    return false;
                }
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("a running container");
            }
        };
    }
}
