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

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
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
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

// Todo: remove after trying on Apple silicon
class HackGenericContainerTest {

    private static final Network network = Network.newNetwork();
    private static final GenericContainer<?> kafka =
            //new GenericContainer<>(DockerImageName.parse("confluentinc/cp-kafka:6.1.2"))
    new KafkaContainerHack()
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
                    .withStartupAttempts(3)
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                    .withStartupTimeout(Duration.ofSeconds(90));

    private final DockerClient dockerClient = DockerClientFactory.lazyClient();
    private Map<String, Object> baseProps;

    @BeforeAll
    static void beforeAll() {
        new KafkaContainerHack().modify(kafka);
        kafka.start();
    }

    @AfterAll
    static void afterAll() {
        kafka.stop();
    }

    @BeforeEach
    void setUp() {
        baseProps = Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getHost() + ":" + kafka.getMappedPort(9093)
        );
    }

    @Test
    @Order(1)
    void shouldBringUpKafka() throws Exception {
        assertThat(kafka.getContainerId(), is(running()));
        assertThat(topics(), is(empty()));
    }

    @Test
    @Order(2)
    void shouldBeAbleToProduceAndConsumeFromKafka() throws Exception {
        // Given:
        try(Admin adminClient = Admin.create(baseProps)) {

            adminClient
                    .createTopics(List.of(new NewTopic("test-topic", 1, (short) 1)))
                    .all()
                    .get(1, TimeUnit.HOURS);
        }

        final Map<String, Object> consumerProps = new HashMap<>(baseProps);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                "Bob");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(
                        consumerProps)) {

            consumer.subscribe(List.of("test-topic"));
            consumer.poll(Duration.ofSeconds(1));

            final HashMap<String, Object> producerProps = new HashMap<>(baseProps);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());

            try (KafkaProducer<String, String> producer =
                         new KafkaProducer<>(producerProps)) {

                producer.send(new ProducerRecord<>("test-topic", "key", "value")).get(1, TimeUnit.HOURS);
            }


            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));

            assertThat(records.count(), is(1));
            final ConsumerRecord<String, String> record = records.iterator().next();
            assertThat(record.key(), is("key"));
            assertThat(record.value(), is("value"));
        }

        // Todo: test actually running
        // Todo: test can connect, produce, consume
        // Todo: test a test-service has its env set accordingly and can connnect, produce &
        // consume.
    }

    private Set<String> topics() throws Exception {
        try (Admin admin = Admin.create(baseProps)) {
            return admin.listTopics().names().get(30, TimeUnit.SECONDS);
        }
    }

    private Matcher<String> running() {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(final String containerId, final Description mismatchDescription) {
                try {
                    final InspectContainerResponse response = dockerClient.inspectContainerCmd(containerId).exec();
                    if (Boolean.FALSE.equals(response.getState().getRunning())) {
                        mismatchDescription.appendText("Container with id ").appendValue(containerId).appendText(" is not running");
                        return false;
                    }
                    return true;
                } catch (final NotFoundException e) {
                    mismatchDescription.appendText("Container with id ").appendValue(containerId).appendText(" no longer exists");
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
