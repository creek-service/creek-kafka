/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

import static org.creekservice.test.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.creekservice.test.TopicDescriptors.outputTopic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.creekservice.internal.kafka.extension.client.TopicClient;
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
            outputTopic(
                    CLUSTER_A, "ignored", "topic-1", long.class, Object.class, withPartitions(1));
    public static final OwnedKafkaTopicOutput<?, ?> TOPIC_2_B =
            outputTopic(
                    CLUSTER_B, "ignored", "topic-2", Object.class, long.class, withPartitions(1));
    public static final OwnedKafkaTopicInput<?, ?> TOPIC_1_B =
            TopicDescriptors.inputTopic(
                    CLUSTER_B, "ignored", "topic-1", long.class, Object.class, withPartitions(1));

    @Mock private TopicClient.Factory topicClientFactory;
    @Mock private ClustersProperties properties;
    @Mock private TopicRegistrar resources;
    @Mock private TopicClient topicClient;
    @Mock private TopicResourceFactory topicResourceFactory;
    @Mock private KafkaTopicInput<?, ?> notCreatable;
    @Mock private Map<String, Object> kafkaProperties4A;
    @Mock private Map<String, Object> kafkaProperties4B;
    @Mock private KafkaTopic<?, ?> topic;
    @Mock private KafkaResourceValidator validator;

    private final TestStructuredLogger logger = TestStructuredLogger.create();
    private TopicResourceHandler handler;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @BeforeEach
    void setUp() {
        handler =
                new TopicResourceHandler(
                        topicClientFactory,
                        properties,
                        resources,
                        topicResourceFactory,
                        validator,
                        logger);

        when(properties.get(CLUSTER_A)).thenReturn(kafkaProperties4A);
        when(properties.get(CLUSTER_B)).thenReturn(kafkaProperties4B);

        when(topicClientFactory.create(any(), anyMap())).thenReturn(topicClient);

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
        doThrow(expected).when(topicClient).ensureTopicsExist(any());

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
    void shouldEnsureCreatableTopicsExist() {
        // When:
        handler.ensure(List.of(TOPIC_1_A));

        // Then:
        verify(topicClient).ensureTopicsExist(List.of(TOPIC_1_A));
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
        verify(topicClient).ensureTopicsExist(List.of(TOPIC_1_A));
        verify(topicClientB).ensureTopicsExist(List.of(TOPIC_1_B, TOPIC_2_B));
    }

    @Test
    void shouldLogTopicsOnEnsure() {
        // When:
        handler.ensure(List.of(TOPIC_1_A));

        // Then:
        assertThat(
                logger.textEntries(),
                hasItem("DEBUG: {message=Ensuring topics, topicIds=[kafka-topic://Anna/topic-1]}"));
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

    @Test
    void shouldCallValidatorOnValidate() {
        // When:
        handler.validate(List.of(TOPIC_1_A));

        // Then:
        verify(validator).validateGroup(List.of(TOPIC_1_A));
    }

    @Test
    void shouldThrowIfValidatorThrows() {
        // Given:
        final RuntimeException expected = new RuntimeException("BOOM");
        doThrow(expected).when(validator).validateGroup(any());

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> handler.validate(List.of()));

        // Then:
        assertThat(e, is(expected));
    }
}
