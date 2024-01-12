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

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class ResourceRegistryTest {

    @Mock private KafkaResourceValidator validator;
    @Mock private Topic<Long, String> topicA;
    @Mock private Topic<String, Long> topicB;
    @Mock private KafkaTopicDescriptor<Long, String> topicDefA;
    @Mock private KafkaTopicDescriptor<String, Long> topicDefB;
    @Mock private KafkaTopicDescriptor<Long, String> def;
    private ResourceRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ResourceRegistry(validator);

        when(topicA.name()).thenCallRealMethod();
        when(topicB.name()).thenCallRealMethod();
        when(topicA.descriptor()).thenReturn(topicDefA);
        when(topicB.descriptor()).thenReturn(topicDefB);

        setUpTopicDef(topicDefA, "topic-A", long.class, String.class);
        setUpTopicDef(topicDefB, "topic-B", String.class, Long.class);
    }

    @Test
    void shouldThrowOnDuplicateRegistration() {
        // Given:
        final String sameName = topicDefA.name();
        when(topicDefB.name()).thenReturn(sameName);

        registry.register(topicA);

        // When:
        final Exception e =
                assertThrows(IllegalStateException.class, () -> registry.register(topicB));

        // Then:
        assertThat(
                e.getMessage(),
                is("Resource already registered with id=kafka-topic://default/topic-A"));
    }

    @Test
    void shouldGetTopicByDef() {
        // When:
        registry.register(topicA);
        registry.register(topicB);

        // Then:
        assertThat(registry.topic(topicDefA), is(topicA));
        assertThat(registry.topic(topicDefB), is(topicB));
    }

    @Test
    void shouldGetTopicByName() {
        // When:
        registry.register(topicA);
        registry.register(topicB);

        // Then:
        assertThat(registry.topic(topicDefA.cluster(), topicDefA.name()), is(topicA));
        assertThat(registry.topic(topicDefB.cluster(), topicDefB.name()), is(topicB));
    }

    @Test
    void shouldBeClusterAware() {
        // Given:
        final String sameName = topicDefA.name();
        when(topicDefB.name()).thenReturn(sameName);
        when(topicDefB.cluster()).thenReturn("different-cluster");

        // When:
        registry.register(topicA);
        registry.register(topicB);

        // Then (did not throw):
        assertThat(registry.topic(topicDefA), is(topicA));
        assertThat(registry.topic(topicDefB), is(topicB));
    }

    @Test
    void shouldGetTopicWithEquivalentDef() {
        // Given:
        registry.register(topicA);

        setUpTopicDef(def, "topic-A", long.class, String.class);

        // Then:
        assertThat(registry.topic(def), is(topicA));
    }

    @Test
    void shouldThrowOnDefMismatch() {
        // Given:
        registry.register(topicA);

        setUpTopicDef(def, "topic-A", long.class, String.class);

        final Exception exception = new RuntimeException("boom");
        doThrow(exception).when(validator).validateGroup(any());

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> registry.topic(def));

        // Then:
        verify(validator).validateGroup(List.of(def, topicA.descriptor()));
        assertThat(e, is(sameInstance(exception)));
    }

    @Test
    void shouldThrowOnUnknownTopic() {
        // Given:
        when(topicDefA.cluster()).thenReturn("unknown");

        // When:
        final Exception e =
                assertThrows(IllegalArgumentException.class, () -> registry.topic(topicDefA));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Unknown topic. No topic has the supplied id."
                                + " id=kafka-topic://unknown/topic-A"));
    }

    private <K, V> void setUpTopicDef(
            final KafkaTopicDescriptor<K, V> def,
            final String name,
            final Class<K> keyType,
            final Class<V> valueType) {
        final PartDescriptor<K> key = part(keyType);
        final PartDescriptor<V> value = part(valueType);

        when(def.id()).thenCallRealMethod();
        when(def.cluster()).thenCallRealMethod();
        when(def.name()).thenReturn(name);
        when(def.key()).thenReturn(key);
        when(def.value()).thenReturn(value);
    }

    @SuppressWarnings("unchecked")
    private <V> PartDescriptor<V> part(final Class<V> type) {
        final PartDescriptor<V> part = mock(PartDescriptor.class);
        when(part.type()).thenReturn(type);
        when(part.format()).thenReturn(serializationFormat("format"));
        return part;
    }
}
