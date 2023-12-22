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

package org.creekservice.internal.kafka.extension.resource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicOutput;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeProvider.SerdeProvider;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.creekservice.internal.kafka.extension.client.TopicClient;
import org.creekservice.test.TopicConfigBuilder;
import org.creekservice.test.TopicDescriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicResourceHandlerTest {

    private static final String CLUSTER_A = "Anna";
    private static final String CLUSTER_B = "Bob";
    private static final OwnedKafkaTopicOutput<?, ?> TOPIC_1_A =
            TopicDescriptors.outputTopic(
                    CLUSTER_A,
                    "topic-1",
                    long.class,
                    Object.class,
                    TopicConfigBuilder.withPartitions(1));
    public static final OwnedKafkaTopicOutput<?, ?> TOPIC_2_B =
            TopicDescriptors.outputTopic(
                    CLUSTER_B,
                    "topic-2",
                    Object.class,
                    long.class,
                    TopicConfigBuilder.withPartitions(1));
    public static final OwnedKafkaTopicInput<?, ?> TOPIC_1_B =
            TopicDescriptors.inputTopic(
                    CLUSTER_B,
                    "topic-1",
                    long.class,
                    Object.class,
                    TopicConfigBuilder.withPartitions(1));

    @Mock private TopicClient.Factory topicClientFactory;
    @Mock private ClustersProperties properties;
    @Mock private ClustersSerdeProviders serdeProviders;
    @Mock private TopicRegistrar resources;
    @Mock private TopicClient topicClient;
    @Mock private TopicResourceFactory topicResourceFactory;
    @Mock private KafkaTopicInput<?, ?> notCreatable;
    @Mock private Map<String, Object> kafkaProperties4A;
    @Mock private Map<String, Object> kafkaProperties4B;
    @Mock private SerdeProvider kafkaSerdeProvider;
    @Mock private SerdeProvider otherSerdeProvider;
    @Mock private KafkaTopic<?, ?> topic;

    private final TestStructuredLogger logger = TestStructuredLogger.create();
    private TopicResourceHandler handler;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @BeforeEach
    void setUp() {
        handler =
                new TopicResourceHandler(
                        topicClientFactory,
                        properties,
                        serdeProviders,
                        resources,
                        topicResourceFactory,
                        logger);

        when(properties.get(CLUSTER_A)).thenReturn(kafkaProperties4A);
        when(properties.get(CLUSTER_B)).thenReturn(kafkaProperties4B);

        when(topicClientFactory.create(any(), anyMap())).thenReturn(topicClient);

        when(serdeProviders.get(eq(TopicDescriptors.KAFKA_FORMAT), any()))
                .thenReturn(kafkaSerdeProvider);
        when(serdeProviders.get(eq(TopicDescriptors.OTHER_FORMAT), any()))
                .thenReturn(otherSerdeProvider);

        when(topicResourceFactory.create(any(), any())).thenReturn((KafkaTopic) topic);
    }

    @Test
    void shouldThrowOnEnsureIfNotCreatable() {
        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> handler.ensure(List.of(notCreatable)));

        // Then:
        assertThat(e.getMessage(), startsWith("Topic descriptor is not creatable"));
    }

    @Test
    void shouldThrowOnEnsureOnUnknownCluster() {
        // Given:
        final RuntimeException expected = new RuntimeException("boom");
        when(properties.get(any())).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.ensure(List.of(TOPIC_1_A)));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldThrowOnEnsureOnFailureToCreateTopics() {
        // Given:
        final RuntimeException expected = new RuntimeException("boom");
        doThrow(expected).when(topicClient).ensureExternalResources(any());

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.ensure(List.of(TOPIC_1_A)));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldCreateTopicClientWithCorrectParams() {
        // When:
        handler.ensure(List.of(TOPIC_1_A));

        // Then:
        verify(topicClientFactory).create(CLUSTER_A, kafkaProperties4A);
    }

    @Test
    void shouldGetSerdeProviderByClusterAndFormat() {
        // When:
        handler.ensure(List.of(TOPIC_1_A));

        // Then:
        verify(serdeProviders).get(TopicDescriptors.KAFKA_FORMAT, CLUSTER_A);
    }

    @Test
    void shouldEnsureCreatableTopicsExist() {
        // When:
        handler.ensure(List.of(TOPIC_1_A));

        // Then:
        verify(topicClient).ensureExternalResources(List.of(TOPIC_1_A));
    }

    @Test
    void shouldCreateTopicsPerCluster() {
        // Given:
        final TopicClient topicClientB = mock(TopicClient.class);
        when(topicClientFactory.create(CLUSTER_A, kafkaProperties4A)).thenReturn(topicClient);
        when(topicClientFactory.create(CLUSTER_B, kafkaProperties4B)).thenReturn(topicClientB);

        // When:
        handler.ensure(List.of(TOPIC_1_B, TOPIC_1_A, TOPIC_2_B));

        // Then:
        verify(topicClient).ensureExternalResources(List.of(TOPIC_1_A));
        verify(topicClientB).ensureExternalResources(List.of(TOPIC_1_B, TOPIC_2_B));
    }

    @Test
    void shouldThrowOnEnsureOnUnknownFormatOrCluster() {
        // Given:
        final RuntimeException expected = new RuntimeException("boom");
        when(serdeProviders.get(any(), any())).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.ensure(List.of(TOPIC_1_A)));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldThrowOnEnsureOnFailureToEnsureSerdeResources() {
        // Given:
        final RuntimeException expected = new RuntimeException("boom");
        doThrow(expected).when(kafkaSerdeProvider).ensureExternalResources(any());

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.ensure(List.of(TOPIC_1_A)));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldLogSerdeOnEnsure() {
        // When:
        handler.ensure(List.of(TOPIC_1_A));

        // Then:
        assertThat(
                logger.textEntries(),
                hasItem(
                        "DEBUG: {message=Ensuring topic resources,"
                                + " topicIds=[kafka-topic://Anna/topic-1]}"));
    }

    @Test
    void shouldPrepareTopicsPerClusterAndFormat() {
        // Given:
        final SerdeProvider kafkaSerdeProviderB = mock(SerdeProvider.class);
        final SerdeProvider otherSerdeProviderB = mock(SerdeProvider.class);
        when(serdeProviders.get(TopicDescriptors.KAFKA_FORMAT, CLUSTER_A))
                .thenReturn(kafkaSerdeProvider);
        when(serdeProviders.get(TopicDescriptors.OTHER_FORMAT, CLUSTER_A))
                .thenReturn(otherSerdeProvider);
        when(serdeProviders.get(TopicDescriptors.KAFKA_FORMAT, CLUSTER_B))
                .thenReturn(kafkaSerdeProviderB);
        when(serdeProviders.get(TopicDescriptors.OTHER_FORMAT, CLUSTER_B))
                .thenReturn(otherSerdeProviderB);

        // When:
        handler.ensure(List.of(TOPIC_1_B, TOPIC_1_A, TOPIC_2_B));

        // Then:
        verify(kafkaSerdeProvider).ensureExternalResources(List.of(TOPIC_1_A.key()));
        verify(otherSerdeProvider).ensureExternalResources(List.of(TOPIC_1_A.value()));
        verify(kafkaSerdeProviderB)
                .ensureExternalResources(List.of(TOPIC_1_B.key(), TOPIC_2_B.value()));
        verify(otherSerdeProviderB)
                .ensureExternalResources(List.of(TOPIC_1_B.value(), TOPIC_2_B.key()));
    }

    @Test
    void shouldThrowOnPrepareOnFailureToCreateTopicResource() {
        // Given:
        final RuntimeException expected = new RuntimeException("boom");
        when(topicResourceFactory.create(any(), any())).thenThrow(expected);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.prepare(List.of(TOPIC_1_A)));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldCreateTopicResourceOnPrepare() {
        // When:
        handler.prepare(List.of(TOPIC_1_A));

        // Then:
        verify(topicResourceFactory).create(TOPIC_1_A, kafkaProperties4A);
    }

    @Test
    void shouldThrowOnPrepareOnFailureToRegisterTopicResource() {
        // Given:
        final RuntimeException expected = new RuntimeException("boom");
        doThrow(expected).when(resources).register(any());

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> handler.prepare(List.of(TOPIC_1_A)));

        // Then:
        assertThat(e, is(expected));
    }

    @Test
    void shouldRegisterTopicResourceOnPrepare() {
        // When:
        handler.prepare(List.of(TOPIC_1_A));

        // Then:
        verify(resources).register(topic);
    }

    @Test
    void shouldLogSerdeOnPrepare() {
        // When:
        handler.prepare(List.of(TOPIC_1_A));

        // Then:
        assertThat(
                logger.textEntries(),
                hasItem(
                        "DEBUG: {message=Preparing topics,"
                                + " topicIds=[kafka-topic://Anna/topic-1]}"));
    }
}
