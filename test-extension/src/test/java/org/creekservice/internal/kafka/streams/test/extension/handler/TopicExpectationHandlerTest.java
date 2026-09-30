/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.test.model.Expectation;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler.ExpectationOptions;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicExpectationHandlerTest {

    private static final String CLUSTER_1 = "default";
    private static final String CLUSTER_2 = "other-cluster";
    private static final String TOPIC = "output-topic";
    private static final TopicPartition PARTITION_0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition PARTITION_1 = new TopicPartition(TOPIC, 1);

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekTestSuite suite;

    @Mock private ClientsExtension clientsExt;
    @Mock private TopicExpectation expectation1;
    @Mock private TopicExpectation expectation2;
    @Mock private Consumer<byte[], byte[]> consumer1;
    @Mock private Consumer<byte[], byte[]> consumer2;
    @Mock private KafkaTopic<Object, Object> topic1;
    @Mock private KafkaTopic<Object, Object> topic2;
    @Mock private KafkaTopicDescriptor<Object, Object> descriptor1;
    @Mock private KafkaTopicDescriptor<Object, Object> descriptor2;
    @Mock private SystemTestSerdeProviders testSerdeProviders;
    @Mock private TestKafkaTopic testTopic1;
    @Mock private TestKafkaTopic testTopic2;
    @Mock private TopicValidator topicValidator;
    @Mock private TopicExpectationHandler.TopicConsumersFactory topicConsumersFactory;
    @Mock private TopicConsumers topicConsumers1;
    @Mock private TopicConsumers topicConsumers2;
    @Mock private ExpectationOptions options;
    @Captor private ArgumentCaptor<SeekOffsetOverrides> overridesCapture;

    private TopicExpectationHandler handler;

    @BeforeEach
    void setUp() {
        handler =
                new TopicExpectationHandler(
                        clientsExt, testSerdeProviders, topicValidator, topicConsumersFactory);

        when(suite.tests().get(0).expectations())
                .thenReturn(
                        List.of(mock(Expectation.class), expectation1, mock(Expectation.class)));

        when(expectation1.records()).thenReturn(List.of(record(CLUSTER_1)));
        lenient().when(expectation2.records()).thenReturn(List.of(record(CLUSTER_2)));

        when(clientsExt.consumer(CLUSTER_1)).thenReturn(consumer1);
        lenient().when(clientsExt.consumer(CLUSTER_2)).thenReturn(consumer2);

        doReturn(topic1).when(clientsExt).topic(CLUSTER_1, TOPIC);
        lenient().doReturn(topic2).when(clientsExt).topic(CLUSTER_2, TOPIC);

        doReturn(descriptor1).when(topic1).descriptor();
        lenient().doReturn(descriptor2).when(topic2).descriptor();

        when(testSerdeProviders.get(descriptor1)).thenReturn(testTopic1);
        lenient().when(testSerdeProviders.get(descriptor2)).thenReturn(testTopic2);

        when(topicConsumersFactory.create(any(), eq(consumer1), any())).thenReturn(topicConsumers1);
        lenient()
                .when(topicConsumersFactory.create(any(), eq(consumer2), any()))
                .thenReturn(topicConsumers2);

        when(options.get(KafkaOptions.class)).thenReturn(List.of());
        when(options.timeout()).thenReturn(Duration.ofSeconds(1));
    }

    @Test
    void shouldNotCaptureOffsetsWithoutSeedData() {
        // Given:
        when(suite.seedData().isEmpty()).thenReturn(true);

        // When:
        handler.afterSeeding(suite);

        // Then:
        verifyNoInteractions(consumer1, consumer2);
    }

    @Test
    void shouldNotCaptureOffsetsWithoutTests() {
        // Given:
        when(suite.tests().isEmpty()).thenReturn(true);

        // When:
        handler.afterSeeding(suite);

        // Then:
        verifyNoInteractions(consumer1, consumer2);
    }

    @Test
    void shouldNotCaptureOffsetsWithoutKafkaExpectations() {
        // Given:
        when(suite.tests().get(0).expectations()).thenReturn(List.of(mock(Expectation.class)));

        // When:
        handler.afterSeeding(suite);

        // Then:
        verifyNoInteractions(consumer1, consumer2);
    }

    @Test
    void shouldUseEndOffsetsForTopicCreatedBeforeSeeding() {
        // Given:
        givenExistingTopicAtOffsets(consumer1, 4L, 9L);

        handler.afterSeeding(suite);

        // When:
        handler.prepare(List.of(expectation1), options);

        // Then:
        final SeekOffsetOverrides passedOverrides = captureOverrides();
        assertThat(passedOverrides.get(TOPIC, 1), is(Map.of(PARTITION_0, 4L, PARTITION_1, 9L)));
    }

    @Test
    void shouldUseZeroOffsetsForTopicCreatedAfterSeeding() {
        // Given:
        when(consumer1.listTopics()).thenReturn(Map.of());

        handler.afterSeeding(suite);

        // When:
        handler.prepare(List.of(expectation1), options);

        // Then:
        final SeekOffsetOverrides passedOverrides = captureOverrides();
        assertThat(passedOverrides.get(TOPIC, 2), is(Map.of(PARTITION_0, 0L, PARTITION_1, 0L)));
    }

    @Test
    void shouldHaveNoSeekOverridesAfterFirstTest() {
        // Given:
        givenExistingTopicAtOffsets(consumer1, 4L, 9L);

        handler.afterSeeding(suite);
        handler.afterTest(mock(), mock());

        // When:
        handler.prepare(List.of(expectation1), options);

        // Then:
        final SeekOffsetOverrides passedOverrides = captureOverrides();
        assertThat(passedOverrides.hasOverrides(TOPIC), is(false));
    }

    @Test
    void shouldHandleSameTopicOnDifferentClusters() {
        // Given:
        givenExistingTopicAtOffsets(consumer1, 4L, 9L);
        givenExistingTopicAtOffsets(consumer2, 3L);

        when(suite.tests().get(0).expectations()).thenReturn(List.of(expectation1, expectation2));

        handler.afterSeeding(suite);

        // When:
        handler.prepare(List.of(expectation1, expectation2), options);

        // Then:
        verify(topicConsumersFactory, times(2)).create(any(), any(), overridesCapture.capture());
        final List<SeekOffsetOverrides> passedOverrides = overridesCapture.getAllValues();

        assertThat(
                passedOverrides.get(0).get(TOPIC, 1), is(Map.of(PARTITION_0, 4L, PARTITION_1, 9L)));
        assertThat(passedOverrides.get(1).get(TOPIC, 1), is(Map.of(PARTITION_0, 3L)));
    }

    private SeekOffsetOverrides captureOverrides() {
        verify(topicConsumersFactory).create(any(), any(), overridesCapture.capture());
        return overridesCapture.getValue();
    }

    private static void givenExistingTopicAtOffsets(
            final Consumer<byte[], byte[]> consumer, final long... offsets) {
        final Map<TopicPartition, Long> partitionOffsets =
                IntStream.range(0, offsets.length)
                        .mapToObj(i -> new TopicPartition(TOPIC, i))
                        .collect(toMap(Function.identity(), tp -> offsets[tp.partition()]));

        final List<PartitionInfo> pis =
                partitionOffsets.keySet().stream()
                        .map(tp -> new PartitionInfo(tp.topic(), tp.partition(), null, null, null))
                        .toList();

        when(consumer.listTopics()).thenReturn(Map.of(TOPIC, pis));
        when(consumer.endOffsets(partitionOffsets.keySet())).thenReturn(partitionOffsets);
    }

    private static TopicRecord record(final String cluster) {
        return new TopicRecord(
                URI.create("file:///expectation.yml"),
                cluster,
                TopicExpectationHandlerTest.TOPIC,
                Optional3.notProvided(),
                Optional3.notProvided());
    }
}
