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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static org.creekservice.api.system.test.test.util.CreekSystemTestExtensionTester.extensionTester;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.service.ServiceInstance;
import org.creekservice.api.system.test.test.util.CreekSystemTestExtensionTester;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.internal.stubbing.defaultanswers.ReturnsDeepStubs;
import org.testcontainers.DockerClientFactory;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StreamsTestLifecycleListenerFunctionalTest {

    private static final CreekSystemTestExtensionTester EXT_TESTER = extensionTester();
    private static StreamsTestLifecycleListener listener;

    private final DockerClient dockerClient = DockerClientFactory.lazyClient();

    @BeforeAll
    static void beforeAll() {
        final KafkaTopicDescriptor<?, ?> kafkaResource0 = mock(KafkaTopicDescriptor.class);
        when(kafkaResource0.cluster()).thenReturn("default");

        final ServiceDescriptor serviceDescriptor = mock(ServiceDescriptor.class);
        when(serviceDescriptor.resources()).thenReturn(Stream.of(kafkaResource0));

        final CreekSystemTest api =
                mock(CreekSystemTest.class, withSettings().defaultAnswer(new ReturnsDeepStubs()));

        when(api.testSuite().services().add(any()))
                .thenAnswer(inv -> EXT_TESTER.dockerServicesContainer().add(inv.getArgument(0)));

        final ConfigurableServiceInstance kafkaServiceUnderTest =
                mock(ConfigurableServiceInstance.class);
        when(kafkaServiceUnderTest.descriptor()).thenReturn(Optional.of(serviceDescriptor));

        when(api.testSuite().services().stream()).thenReturn(Stream.of(kafkaServiceUnderTest));

        listener = new StreamsTestLifecycleListener(api);
    }

    @AfterAll
    static void afterAll() {
        EXT_TESTER.dockerServicesContainer().forEach(ServiceInstance::stop);
        EXT_TESTER.clearServices();
    }

    @Test
    @Order(1)
    void shouldStartKafka() {
        // When:
        listener.beforeSuite(null);

        // Then:
        assertThat(serviceInstance("kafka-default-0").running(), is(true));
        assertThat(EXT_TESTER.runningContainerIds().get("kafka-default-0"), is(running()));
    }

    @Test
    void shouldBeAbleToProduceAndConsumeFromTestNetwork() throws Exception {
        // Given:
        givenTopic();

        try (KafkaConsumer<String, String> consumer = kafkaConsumer()) {

            consumer.subscribe(List.of("test-topic"));
            consumer.poll(Duration.ofSeconds(5));

            try (KafkaProducer<String, String> producer = kafkaProducer()) {

                // When:
                producer.send(new ProducerRecord<>("test-topic", "key", "value"))
                        .get(1, TimeUnit.HOURS);
            }

            // Then:
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));

            assertThat(records.count(), is(1));
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertThat(record.key(), is("key"));
            assertThat(record.value(), is("value"));
        }
    }

    @Test
    @Order(Integer.MAX_VALUE)
    void shouldShutdownKafkaAfterSuite() {
        // Given:
        final String kafkaContainerId = EXT_TESTER.runningContainerIds().get("kafka-default-0");

        // When:
        listener.afterSuite(null);

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
        baseProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, testNetworkBootstrap);
        return baseProps;
    }

    @SuppressFBWarnings(
            value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "https://github.com/spotbugs/spotbugs/issues/756")
    private void givenTopic() {
        try (Admin adminClient = Admin.create(baseProps())) {
            adminClient
                    .createTopics(List.of(new NewTopic("test-topic", 1, (short) 1)))
                    .all()
                    .get(1, TimeUnit.HOURS);
        } catch (ExecutionException | TimeoutException | InterruptedException e) {
            throw new AssertionError("Failed to create topic");
        }
    }

    private KafkaProducer<String, String> kafkaProducer() {
        final Map<String, Object> producerProps = baseProps();
        producerProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(producerProps);
    }

    private KafkaConsumer<String, String> kafkaConsumer() {
        final Map<String, Object> consumerProps = baseProps();
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "Bob");
        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaConsumer<>(consumerProps);
    }

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
