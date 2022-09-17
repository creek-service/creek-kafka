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

package org.creekservice.internal.kafka.extension.client;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicOutput;
import org.creekservice.test.TopicConfigBuilder;
import org.creekservice.test.TopicDescriptors;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaTopicClientFunctionalTest {

    @Container
    private static final KafkaContainer DEFAULT_CLUSTER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.4"))
                    .withStartupAttempts(3)
                    .withStartupTimeout(Duration.ofSeconds(90))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @Container
    private static final KafkaContainer OTHER_CLUSTER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.4"))
                    .withStartupAttempts(3)
                    .withStartupTimeout(Duration.ofSeconds(90))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private static final String OTHER_CLUSTER_NAME = "other";

    @Mock private ClustersProperties clustersProperties;

    private final CreatableKafkaTopic<Long, String> defaultTopic =
            createTopicDescriptor(DEFAULT_CLUSTER_NAME);
    private final CreatableKafkaTopic<Long, String> otherTopic =
            createTopicDescriptor(OTHER_CLUSTER_NAME);
    private final Map<String, Admin> admins = new HashMap<>();

    private KafkaTopicClient client;

    @BeforeEach
    void setUp() {
        client = new KafkaTopicClient(clustersProperties);

        final Map<String, Object> defaultClusterProps =
                Map.of(BOOTSTRAP_SERVERS_CONFIG, DEFAULT_CLUSTER.getBootstrapServers());
        when(clustersProperties.get(DEFAULT_CLUSTER_NAME)).thenReturn(defaultClusterProps);
        admins.put(DEFAULT_CLUSTER_NAME, Admin.create(defaultClusterProps));

        final Map<String, Object> otherClusterProps =
                Map.of(BOOTSTRAP_SERVERS_CONFIG, OTHER_CLUSTER.getBootstrapServers());

        when(clustersProperties.get(OTHER_CLUSTER_NAME)).thenReturn(otherClusterProps);
        admins.put(OTHER_CLUSTER_NAME, Admin.create(otherClusterProps));
    }

    @AfterEach
    void tearDown() {
        admins.values().forEach(Admin::close);
    }

    @Test
    void shouldThrowIfKafkaDown() {
        // Given:
        when(clustersProperties.get(DEFAULT_CLUSTER_NAME))
                .thenReturn(Map.of(BOOTSTRAP_SERVERS_CONFIG, "host_down:81"));

        // Then:
        assertThrows(KafkaException.class, () -> client.ensure(List.of(defaultTopic)));
    }

    @Test
    void shouldCreateTopicsThatDoNotExist() {
        // When:
        client.ensure(List.of(defaultTopic));

        // Then:
        assertThat(defaultTopic, exists());
    }

    @Test
    void shouldHandleTopicExisting() {
        // Given:
        givenTopicExists(defaultTopic);

        // When:
        client.ensure(List.of(defaultTopic));

        // Then:
        assertThat(defaultTopic, exists());
    }

    @Test
    void shouldHandleTopicsForMultipleClusters() {
        // When:
        client.ensure(List.of(defaultTopic, otherTopic));

        // Then:
        assertThat(defaultTopic, exists());
        assertThat(otherTopic, exists());
    }

    private void givenTopicExists(final CreatableKafkaTopic<?, ?> topic) {
        final NewTopic newTopic =
                new NewTopic(
                                topic.name(),
                                Optional.of(topic.config().partitions()),
                                Optional.empty())
                        .configs(topic.config().config());

        try {
            admins.get(topic.cluster()).createTopics(List.of(newTopic)).all().get();
        } catch (Exception e) {
            throw new AssertionError(
                    "Failed to create topic " + topic.name() + " om cluster " + topic.cluster(), e);
        }
    }

    private OwnedKafkaTopicOutput<Long, String> createTopicDescriptor(final String cluster) {
        return TopicDescriptors.outputTopic(
                cluster,
                UUID.randomUUID().toString(),
                Long.class,
                String.class,
                TopicConfigBuilder.withPartitions(3).withInfiniteRetention());
    }

    private Matcher<? super CreatableKafkaTopic<?, ?>> exists() {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(
                    final CreatableKafkaTopic<?, ?> topic, final Description mismatchDescription) {
                final String topicName = topic.name();
                final String clusterName = topic.cluster();

                try {
                    final Admin admin = admins.get(topic.cluster());

                    final TopicDescription description =
                            admin.describeTopics(List.of(topicName)).all().get().get(topic.name());
                    if (description.partitions().size() != topic.config().partitions()) {
                        mismatchDescription
                                .appendText("Topic has wrong partition count. Expected: ")
                                .appendValue(topic.config().partitions())
                                .appendText(", got: ")
                                .appendValue(description.partitions().size())
                                .appendText(", topic: ")
                                .appendValue(topicName)
                                .appendText(", cluster: ")
                                .appendValue(clusterName);
                        return false;
                    }

                    final ConfigResource configResource = new ConfigResource(TOPIC, topicName);
                    final Map<String, String> actualConfig =
                            admin
                                    .describeConfigs(List.of(configResource))
                                    .all()
                                    .get()
                                    .get(configResource)
                                    .entries()
                                    .stream()
                                    .filter(
                                            e ->
                                                    e.source()
                                                            == ConfigEntry.ConfigSource
                                                                    .DYNAMIC_TOPIC_CONFIG)
                                    .collect(
                                            Collectors.toMap(
                                                    ConfigEntry::name, ConfigEntry::value));
                    if (!actualConfig.equals(topic.config().config())) {
                        mismatchDescription
                                .appendText("Topic has wrong config. Expected: ")
                                .appendValue(topic.config().config())
                                .appendText(", got: ")
                                .appendValue(actualConfig)
                                .appendText(", topic: ")
                                .appendValue(topicName)
                                .appendText(", cluster: ")
                                .appendValue(clusterName);
                        return false;
                    }
                    return true;
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        mismatchDescription
                                .appendText("Topic does not exist. topic: ")
                                .appendValue(topicName)
                                .appendText(", cluster: ")
                                .appendValue(clusterName);
                        return false;
                    }
                    throw new AssertionError(e);
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }

            @Override
            public void describeTo(final Description description) {
                description.appendText("Topic exists");
            }
        };
    }
}
