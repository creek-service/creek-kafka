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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicInternal;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicOutput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.internal.kafka.extension.resource.TopicCollector;
import org.creekservice.internal.kafka.extension.resource.TopicCollector.CollectedTopics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicValidatingListenerTest {

    private static final URI TOPIC_ID = URI.create("some:///id");

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekSystemTest api;

    private TopicValidatingListener validator;
    @Mock private TopicCollector topicCollector;
    @Mock private CollectedTopics collectedTopics;
    @Mock private KafkaTopic<?, ?> topic;
    @Mock private KafkaTopicDescriptor<?, ?> topicDescriptor;
    @Mock private ConfigurableServiceInstance serviceInstance0;
    @Mock private ConfigurableServiceInstance serviceInstance1;
    @Mock private ServiceDescriptor serviceDescriptor0;
    @Mock private ServiceDescriptor serviceDescriptor1;

    @Mock(extraInterfaces = KafkaTopicInput.class)
    private KafkaTopicDescriptor<?, ?> inputDescriptor;

    @Mock(extraInterfaces = OwnedKafkaTopicInput.class)
    private KafkaTopicDescriptor<?, ?> ownedInputDescriptor;

    @Mock(extraInterfaces = KafkaTopicInternal.class)
    private KafkaTopicDescriptor<?, ?> sharedDescriptor;

    @Mock(extraInterfaces = KafkaTopicOutput.class)
    private KafkaTopicDescriptor<?, ?> outputDescriptor;

    @Mock(extraInterfaces = OwnedKafkaTopicOutput.class)
    private KafkaTopicDescriptor<?, ?> ownedOutputDescriptor;

    @BeforeEach
    void setUp() {
        validator = new TopicValidatingListener(api, topicCollector);

        when(topicCollector.collectTopics(any())).thenReturn(collectedTopics);
        doReturn(topicDescriptor).when(topic).descriptor();

        when(topicDescriptor.id()).thenReturn(TOPIC_ID);

        when(api.tests().env().currentSuite().services().stream())
                .thenReturn(Stream.of(serviceInstance0, serviceInstance1));

        doReturn(Optional.of(serviceDescriptor0)).when(serviceInstance0).descriptor();
        doReturn(Optional.of(serviceDescriptor1)).when(serviceInstance1).descriptor();
    }

    @Test
    void shouldPassAllDescriptorsToTopicCollector() {
        // When:
        validator.beforeSuite(null);

        // Then:
        verify(topicCollector).collectTopics(List.of(serviceDescriptor0, serviceDescriptor1));
    }

    @Test
    void shouldIgnoreServicesWithoutDescriptor() {
        // Given:
        when(serviceInstance0.descriptor()).thenReturn(Optional.empty());

        // When:
        validator.beforeSuite(null);

        // Then:
        verify(topicCollector).collectTopics(List.of(serviceDescriptor1));
    }

    @Test
    void shouldAllowConsumeFromInternalTopic() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(inputDescriptor, sharedDescriptor));

        validator.beforeSuite(null);

        // When:
        validator.validateCanConsume(topic);

        // Then: did not throw
    }

    @Test
    void shouldAllowConsumeFromOutputTopic() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(inputDescriptor, outputDescriptor));

        validator.beforeSuite(null);

        // When:
        validator.validateCanConsume(topic);

        // Then: did not throw
    }

    @Test
    void shouldAllowConsumeFromOwnedOutputTopic() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(inputDescriptor, ownedOutputDescriptor));

        validator.beforeSuite(null);

        // When:
        validator.validateCanConsume(topic);

        // Then: did not throw
    }

    @Test
    void shouldThrowOnConsumeFromPureInputTopic() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(inputDescriptor, ownedInputDescriptor));

        validator.beforeSuite(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateCanConsume(topic));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Tests can not consume from topic some:///id "
                                + "as the services-under-test do not produce to it. "
                                + "Please check the topic name."));
    }

    @Test
    void shouldAllowProduceToInternalTopic() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(outputDescriptor, sharedDescriptor));

        validator.beforeSuite(null);

        // When:
        validator.validateCanProduce(topic);

        // Then: did not throw
    }

    @Test
    void shouldAllowProduceToInputTopic() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(inputDescriptor, outputDescriptor));

        validator.beforeSuite(null);

        // When:
        validator.validateCanProduce(topic);

        // Then: did not throw
    }

    @Test
    void shouldAllowProduceToOwnedInputTopic() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(ownedInputDescriptor, outputDescriptor));

        validator.beforeSuite(null);

        // When:
        validator.validateCanProduce(topic);

        // Then: did not throw
    }

    @Test
    void shouldThrowOnProduceToPureOutputTopic() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(outputDescriptor, ownedOutputDescriptor));

        validator.beforeSuite(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateCanProduce(topic));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Tests can not produce to topic some:///id "
                                + "as the services-under-test do not consume from it. "
                                + "Please check the topic name."));
    }

    @Test
    void shouldThrowIfTopicCollectorThrows() {
        // Given:
        final RuntimeException cause = new RuntimeException("boom");
        when(topicCollector.collectTopics(anyCollection())).thenThrow(cause);

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> validator.beforeSuite(null));

        // Then:
        assertThat(e, is(cause));
    }

    @Test
    void shouldThrowIfBeforeSuiteNotCalled() {
        // Given:
        when(collectedTopics.getAll(TOPIC_ID))
                .thenReturn(List.of(inputDescriptor, sharedDescriptor));

        validator.beforeSuite(null);

        // When:
        validator.afterSuite(null, null);

        // Then:
        assertThrows(IllegalStateException.class, () -> validator.validateCanConsume(topic));
        assertThrows(IllegalStateException.class, () -> validator.validateCanProduce(topic));
    }
}
