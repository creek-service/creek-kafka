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

import static java.util.stream.Collectors.toMap;
import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.internal.kafka.extension.resource.TopicCollector.CollectedTopics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TopicCollectorTest {

    private static final SerializationFormat SOME_FORMAT = serializationFormat("something");

    @Mock private ComponentDescriptor componentA;
    @Mock private ComponentDescriptor componentB;
    @Mock private PartDescriptor<Long> topicKey;
    @Mock private PartDescriptor<Long> creatableTopicKey;
    @Mock private PartDescriptor<String> topicValue;
    @Mock private PartDescriptor<String> creatableTopicValue;
    @Mock private KafkaTopicDescriptor<Long, String> topic;
    @Mock private CreatableKafkaTopic<Long, String> creatableTopic;
    @Mock private CreatableKafkaTopic<Long, String> creatableTopic2;
    private TopicCollector collector;

    @BeforeEach
    void setUp() {
        when(topicKey.type()).thenReturn(long.class);
        when(topicKey.format()).thenReturn(SOME_FORMAT);

        when(creatableTopicKey.type()).thenReturn(long.class);
        when(creatableTopicKey.format()).thenReturn(SOME_FORMAT);

        when(topicValue.type()).thenReturn(String.class);
        when(topicValue.format()).thenReturn(SOME_FORMAT);

        when(creatableTopicValue.type()).thenReturn(String.class);
        when(creatableTopicValue.format()).thenReturn(SOME_FORMAT);

        when(topic.id()).thenCallRealMethod();
        when(topic.name()).thenReturn("topicDef");
        when(topic.cluster()).thenReturn(DEFAULT_CLUSTER_NAME);
        when(topic.key()).thenReturn(topicKey);
        when(topic.value()).thenReturn(topicValue);

        when(creatableTopic.id()).thenCallRealMethod();
        when(creatableTopic.name()).thenReturn("creatableTopicDef");
        when(creatableTopic.cluster()).thenReturn(DEFAULT_CLUSTER_NAME);
        when(creatableTopic.key()).thenReturn(creatableTopicKey);
        when(creatableTopic.value()).thenReturn(creatableTopicValue);
        when(creatableTopic.config()).thenReturn(() -> 0);

        when(creatableTopic2.id()).thenCallRealMethod();
        when(creatableTopic2.name()).thenReturn("creatableTopicDef");
        when(creatableTopic2.cluster()).thenReturn(DEFAULT_CLUSTER_NAME);
        when(creatableTopic2.key()).thenReturn(creatableTopicKey);
        when(creatableTopic2.value()).thenReturn(creatableTopicValue);
        when(creatableTopic2.config()).thenReturn(() -> 0);

        when(componentA.resources()).thenReturn(Stream.of(topic));
        when(componentB.resources()).thenReturn(Stream.of(creatableTopic));

        collector = new TopicCollector();
    }

    @Test
    void shouldNotBlowUpIfNoKafkaResources() {
        // Given:
        final ResourceDescriptor otherResource = mock(ResourceDescriptor.class);
        when(componentA.resources()).thenReturn(Stream.of(otherResource));

        // When:
        final CollectedTopics result = collector.collectTopics(List.of(componentA));

        // Then:
        assertThat(result.clusters(), is(empty()));
        assertThat(result.stream().collect(Collectors.toList()), is(empty()));
    }

    @Test
    void shouldCollectTopics() {
        // When:
        final CollectedTopics result = collector.collectTopics(List.of(componentA, componentB));

        // Then:
        assertThat(result.clusters(), is(Set.of(DEFAULT_CLUSTER_NAME)));
        assertThat(
                result.stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)),
                is(
                        Map.of(
                                topic.id(),
                                List.of(topic),
                                creatableTopic.id(),
                                List.of(creatableTopic))));
    }

    @Test
    void shouldCollectNestedTopics() {
        // Given:
        final ResourceDescriptor resource = mock();
        when(resource.resources()).thenReturn(Stream.of(topic));
        when(componentA.resources()).thenReturn(Stream.of(resource));

        // When:
        final CollectedTopics result = collector.collectTopics(List.of(componentA));

        // Then:
        assertThat(result.clusters(), is(Set.of(DEFAULT_CLUSTER_NAME)));
        assertThat(
                result.stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)),
                is(Map.of(topic.id(), List.of(topic))));
    }

    @Test
    void shouldHandleTopicsWithSameNameOnDifferentClusters() {
        // Given:
        when(topic.name()).thenReturn("topicDef");
        when(topic.cluster()).thenReturn(DEFAULT_CLUSTER_NAME);

        when(creatableTopic.name()).thenReturn("topicDef");
        when(creatableTopic.cluster()).thenReturn("diffCluster");

        // When:
        final CollectedTopics result = collector.collectTopics(List.of(componentA, componentB));

        // Then:
        assertThat(result.clusters(), is(Set.of(DEFAULT_CLUSTER_NAME, "diffCluster")));
        assertThat(
                result.stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)),
                is(
                        Map.of(
                                topic.id(),
                                List.of(topic),
                                creatableTopic.id(),
                                List.of(creatableTopic))));
    }

    @Test
    void shouldHandleDuplicates() {
        // Given:
        when(componentB.resources()).thenReturn(Stream.of(topic));

        // When:
        final CollectedTopics result = collector.collectTopics(List.of(componentA, componentB));

        // Then:
        final Map<URI, List<KafkaTopicDescriptor<?, ?>>> topics =
                result.stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThat(topics.get(topic.id()), is(List.of(topic, topic)));
        assertThat(topics.entrySet(), hasSize(1));
    }

    @Test
    void shouldHandleDuplicateCreatable() {
        // Given:
        when(componentA.resources()).thenReturn(Stream.of(creatableTopic, creatableTopic2));

        // When:
        final CollectedTopics result = collector.collectTopics(List.of(componentA));

        // Then:
        final Map<URI, List<KafkaTopicDescriptor<?, ?>>> topics =
                result.stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThat(topics.get(creatableTopic.id()), is(List.of(creatableTopic, creatableTopic2)));
        assertThat(topics.entrySet(), hasSize(1));
    }

    @Test
    void shouldThrowOnUnknownTopicId() {
        // Given:
        final URI id = URI.create("some:///id");
        final CollectedTopics collectedTopics = new CollectedTopics(Map.of());

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> collectedTopics.getAll(id));

        // Then:
        assertThat(e.getMessage(), is("Unknown topic id: " + id));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldGetByTopicId() {
        // Given:
        final URI id = URI.create("some:///id");
        final List<KafkaTopicDescriptor<?, ?>> descriptors =
                List.of(mock(KafkaTopicDescriptor.class));
        final CollectedTopics collectedTopics = new CollectedTopics(Map.of(id, descriptors));

        // When:
        final List<KafkaTopicDescriptor<?, ?>> result = collectedTopics.getAll(id);

        // Then:
        assertThat(result, is(descriptors));
    }
}
