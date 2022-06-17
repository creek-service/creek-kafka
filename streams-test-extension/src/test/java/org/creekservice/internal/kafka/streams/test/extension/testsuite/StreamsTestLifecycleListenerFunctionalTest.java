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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.github.dockerjava.api.DockerClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
import org.apache.kafka.common.serialization.StringSerializer;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.executor.api.testsuite.service.LocalServiceInstances;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.service.ServiceInstance;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.internal.stubbing.defaultanswers.ReturnsDeepStubs;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.DockerClientFactory;

@ExtendWith(MockitoExtension.class)
class StreamsTestLifecycleListenerFunctionalTest {

    // Todo: can system test repo provide easy way to test this?
    private static final LocalServiceInstances SERVICES = new LocalServiceInstances();
    private static StreamsTestLifecycleListener listener;
    private final DockerClient dockerClient = DockerClientFactory.lazyClient();

    @BeforeAll
    static void beforeAll() {
        final KafkaTopicDescriptor<?, ?> kafkaResource = mock(KafkaTopicDescriptor.class);
        when(kafkaResource.cluster()).thenReturn("default");
        final ServiceDescriptor serviceDescriptor = mock(ServiceDescriptor.class);
        when(serviceDescriptor.resources()).thenReturn(Stream.of(kafkaResource));

        final CreekSystemTest api =
                mock(CreekSystemTest.class, withSettings().defaultAnswer(new ReturnsDeepStubs()));

        when(api.testSuite().services().add(any(), any()))
                .thenAnswer(inv -> SERVICES.add(inv.getArgument(0), inv.getArgument(1)));

        final ServiceInstance serviceInstance = mock(ServiceInstance.class);
        when(serviceInstance.descriptor()).thenReturn(Optional.of(serviceDescriptor));

        when(api.testSuite().services().stream()).thenReturn(Stream.of(serviceInstance));

        listener = new StreamsTestLifecycleListener(api);
        listener.beforeSuite(null);
    }

    @AfterAll
    static void afterAll() {
        SERVICES.forEach(ServiceInstance::stop);
        SERVICES.clear();
    }

    @Test
    void shouldBeAbleToProduceAndConsumeFromKafka() throws Exception {
        // Given:
        final Admin adminClient =
                Admin.create(
                        Map.of(
                                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                                listener.hostEndpoint()
                                ));

        adminClient
                .createTopics(List.of(new NewTopic("test-topic", 1, (short) 1)))
                .all()
                .get(1, TimeUnit.HOURS);

        final KafkaProducer<String, String> producer =
                new KafkaProducer<>(
                        Map.of(
                                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, listener.hostEndpoint(),
                                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
                        ));

        producer.send(new ProducerRecord<>("test-topic", "key", "value")).get(1, TimeUnit.HOURS);

        final KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(
                        Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, listener.hostEndpoint(),
                                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));

        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));

        assertThat(records.count(), is(1));
        assertThat(
                records.iterator().next(),
                is(new ConsumerRecord<>("test-topic", 1, 0, "key", "value")));

        // Todo: test actually running
        // Todo: test can connect, produce, consume
        // Todo: test a test-service has its env set accordingly and can connnect, produce &
        // consume.

        try {
            Thread.sleep(10_000_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void shouldSupportMultipleClusters() {
        // Todo:
    }

    private Matcher<? super ServiceInstance> serviceWithName(final String instanceName) {
        return new FeatureMatcher<>(is(instanceName), "service instance name", "instance-name") {
            @Override
            protected String featureValueOf(final ServiceInstance actual) {
                return actual.name();
            }
        };
    }
}

// Todo: issues with docker image pull rate  - can we login to docker hub?
