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

package org.creekservice.internal.kafka.extension.client;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.creekservice.test.TopicConfigBuilder.withPartitions;
import static org.creekservice.test.TopicDescriptors.outputTopic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProviders;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.creekservice.test.TopicDescriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaTopicClientTest {

    private static final String CLUSTER = "c";
    private static final CreatableKafkaTopic<Long, Object> TOPIC_A =
            outputTopic(CLUSTER, "t", Long.class, Object.class, withPartitions(1));
    private static final Map<String, Object> A_CLUSTER_PROPS = Map.of("a", 1);

    @Mock private ClustersProperties clusterProps;
    @Mock private KafkaSerdeProviders serdeProviders;
    @Mock private KafkaSerdeProvider kafkaSerdeProvider;
    @Mock private KafkaSerdeProvider otherSerdeProvider;
    @Mock private Function<Map<String, Object>, Admin> adminFactory;
    @Mock private Admin admin;
    @Mock private CreateTopicsResult createTopicsResult;
    private final TestStructuredLogger logger = TestStructuredLogger.create();
    private KafkaTopicClient client;

    @BeforeEach
    void setUp() {
        client = new KafkaTopicClient(clusterProps, serdeProviders, adminFactory, logger);

        when(adminFactory.apply(any())).thenReturn(admin);
        when(admin.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.values()).thenReturn(Map.of(TOPIC_A.name(), completedFuture(null)));
        when(createTopicsResult.numPartitions(TOPIC_A.name())).thenReturn(completedFuture(1));
        when(createTopicsResult.config(TOPIC_A.name()))
                .thenReturn(completedFuture(new Config(List.of())));

        when(clusterProps.get(CLUSTER)).thenReturn(A_CLUSTER_PROPS);

        when(serdeProviders.get(TopicDescriptors.KAFKA_FORMAT)).thenReturn(kafkaSerdeProvider);
        when(serdeProviders.get(TopicDescriptors.OTHER_FORMAT)).thenReturn(otherSerdeProvider);
    }

    @Test
    void shouldCreateAdmin() {
        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        verify(adminFactory).apply(A_CLUSTER_PROPS);
    }

    @Test
    void shouldCloseAdmin() {
        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        verify(admin).close();
    }

    @Test
    void shouldCloseAdminOnException() {
        // Given:
        when(admin.createTopics(any())).thenThrow(new RuntimeException("Boom"));

        // When:
        assertThrows(RuntimeException.class, () -> client.ensure(List.of(TOPIC_A)));

        // Then:
        verify(admin).close();
    }

    @Test
    void shouldNotThrowOnExistingTopic() {
        // Given:
        givenTopicExists();

        // When:
        client.ensure(List.of(TOPIC_A));

        // Then: did not throw.
    }

    @Test
    void shouldThrowOnOtherTopicCreationErrors() {
        // Given:
        final BrokerNotAvailableException cause = new BrokerNotAvailableException("");
        final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
        f.completeExceptionally(cause);
        when(createTopicsResult.values()).thenReturn(Map.of(TOPIC_A.name(), f));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> client.ensure(List.of(TOPIC_A)));

        // Then:
        assertThat(e.getMessage(), is("Failed to create topic. topicId: kafka-topic://c/t"));
        assertThat(e.getCause(), is(cause));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldThrowIfTopicCreationInterrupted() throws Exception {
        // Given:
        final InterruptedException cause = new InterruptedException();
        final KafkaFuture<Void> f = mock(KafkaFuture.class);
        when(f.get()).thenThrow(cause);
        when(createTopicsResult.values()).thenReturn(Map.of(TOPIC_A.name(), f));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> client.ensure(List.of(TOPIC_A)));

        // Then:
        assertThat(e.getMessage(), is("Failed to create topic. topicId: kafka-topic://c/t"));
        assertThat(e.getCause(), is(cause));
    }

    @Test
    void shouldLogTopicsOnEnsure() {
        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        assertThat(
                logger.textEntries(),
                hasItem("DEBUG: {message=Ensuring topics, topicIds=[kafka-topic://c/t]}"));
    }

    @Test
    void shouldLogOnTopicCreation() {
        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        assertThat(
                logger.textEntries(),
                hasItem("INFO: {message=Created topic, partitions=1, topicId=kafka-topic://c/t}"));
    }

    @Test
    void shouldLogOnTopicsPreExisting() {
        // Given:
        givenTopicExists();

        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        assertThat(
                logger.textEntries(),
                hasItem("DEBUG: {message=Topic already exists, topicId=kafka-topic://c/t}"));
    }

    @Test
    void shouldThrowOnUnknownFormat() {
        // Given:
        final RuntimeException expected = new IllegalArgumentException("Unknown format bro");
        when(serdeProviders.get(TopicDescriptors.KAFKA_FORMAT)).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> client.ensure(List.of(TOPIC_A)));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldEnsureKeyResource() {
        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        verify(kafkaSerdeProvider).ensureTopicPartResources(TOPIC_A.key(), A_CLUSTER_PROPS);
    }

    @Test
    void shouldEnsureValueResource() {
        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        verify(otherSerdeProvider).ensureTopicPartResources(TOPIC_A.value(), A_CLUSTER_PROPS);
    }

    @Test
    void shouldLogOnEnsureTopicResources() {
        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        assertThat(
                logger.textEntries(),
                hasItem("DEBUG: {message=Ensuring topic resources, topicId=kafka-topic://c/t}"));
    }

    private void givenTopicExists() {
        final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new TopicExistsException(""));
        when(createTopicsResult.values()).thenReturn(Map.of(TOPIC_A.name(), f));
    }
}
