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

package org.creekservice.internal.kafka.common.resource;

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.creekservice.internal.kafka.common.resource.TopicCollector.collectTopics;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.metadata.SerializationFormat;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
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
        when(topic.cluster()).thenReturn(KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME);
        when(topic.key()).thenReturn(topicKey);
        when(topic.value()).thenReturn(topicValue);

        when(creatableTopic.id()).thenCallRealMethod();
        when(creatableTopic.name()).thenReturn("creatableTopicDef");
        when(creatableTopic.cluster()).thenReturn(KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME);
        when(creatableTopic.key()).thenReturn(creatableTopicKey);
        when(creatableTopic.value()).thenReturn(creatableTopicValue);
        when(creatableTopic.config()).thenReturn(() -> 0);

        when(componentA.resources()).thenReturn(Stream.of(topic));
        when(componentB.resources()).thenReturn(Stream.of(creatableTopic));
    }

    @Test
    void shouldNotBlowUpIfNoKafkaResources() {
        // Given:
        final ResourceDescriptor otherResource = mock(ResourceDescriptor.class);
        when(componentA.resources()).thenReturn(Stream.of(otherResource));

        // When:
        final Map<?, ?> result = collectTopics(List.of(componentA));

        // Then:
        assertThat(result.entrySet(), is(empty()));
    }

    @Test
    void shouldCollectTopics() {
        // When:
        final Map<URI, KafkaTopicDescriptor<?, ?>> result =
                collectTopics(List.of(componentA, componentB));

        // Then:
        assertThat(result, is(Map.of(topic.id(), topic, creatableTopic.id(), creatableTopic)));
    }

    @Test
    void shouldHandleTopicsWithSameNameOnDifferentClusters() {
        // Given:
        when(topic.name()).thenReturn("topicDef");
        when(topic.cluster()).thenReturn(KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME);

        when(creatableTopic.name()).thenReturn("topicDef");
        when(creatableTopic.cluster()).thenReturn("diffCluster");

        // When:
        final Map<URI, KafkaTopicDescriptor<?, ?>> result =
                collectTopics(List.of(componentA, componentB));

        // Then:
        assertThat(result, is(Map.of(topic.id(), topic, creatableTopic.id(), creatableTopic)));
    }

    @Test
    void shouldDeduplicateDefs() {
        // Given:
        when(componentB.resources()).thenReturn(Stream.of(topic));

        // When:
        final Map<URI, KafkaTopicDescriptor<?, ?>> result =
                collectTopics(List.of(componentA, componentB));

        // Then:
        assertThat(result, hasEntry(topic.id(), topic));
        assertThat(result.entrySet(), hasSize(1));
    }

    @Test
    void shouldPreferDescriptorsWithConfig() {
        // Given:
        when(creatableTopic.name()).thenReturn("topicDef");
        when(componentA.resources()).thenReturn(Stream.of(topic, creatableTopic, topic));

        // When:
        final Map<URI, KafkaTopicDescriptor<?, ?>> result = collectTopics(List.of(componentA));

        // Then:
        assertThat(result, hasEntry(topic.id(), creatableTopic));
        assertThat(result.entrySet(), hasSize(1));
    }

    @Test
    void shouldThrowOnDuplicateResourceMismatch() {
        // Given:
        when(creatableTopic.name()).thenReturn("topicDef");
        when(creatableTopicValue.format()).thenReturn(serializationFormat("different"));

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> collectTopics(List.of(componentA, componentB)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Topic descriptor mismatch: "
                                + "multiple topic descriptors share the same topic name, "
                                + "but have different attributes."
                                + System.lineSeparator()));

        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(creatableTopic)));
    }
}
