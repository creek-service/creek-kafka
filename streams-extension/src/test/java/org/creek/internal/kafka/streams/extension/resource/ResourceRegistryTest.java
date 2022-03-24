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

package org.creek.internal.kafka.streams.extension.resource;

import static org.creek.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.creek.api.kafka.metadata.KafkaTopicDescriptor;
import org.creek.api.kafka.metadata.KafkaTopicDescriptor.PartDescriptor;
import org.creek.internal.kafka.common.resource.KafkaTopicDescriptors;
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

    @Mock private Topic<Long, String> topicA;
    @Mock private Topic<String, Long> topicB;
    @Mock private KafkaTopicDescriptor<Long, String> topicDefA;
    @Mock private KafkaTopicDescriptor<String, Long> topicDefB;
    @Mock private KafkaTopicDescriptor<Long, String> def;
    private ResourceRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ResourceRegistry(Map.of("topic-A", topicA, "topic-B", topicB));

        when(topicA.descriptor()).thenReturn(topicDefA);
        when(topicB.descriptor()).thenReturn(topicDefB);

        setUpTopicDef(topicDefA, "topic-A", long.class, String.class);
        setUpTopicDef(topicDefB, "topic-B", String.class, Long.class);
    }

    @Test
    void shouldGetTopic() {
        assertThat(registry.topic(topicDefA), is(topicA));
        assertThat(registry.topic(topicDefB), is(topicB));
    }

    @Test
    void shouldGetTopicWithEquivalentDef() {
        // Given:
        setUpTopicDef(def, "topic-A", long.class, String.class);

        // Then:
        assertThat(registry.topic(def), is(topicA));
    }

    @Test
    void shouldThrowOnDefMismatch() {
        // Given:
        setUpTopicDef(def, "topic-A", Long.class, String.class);

        // When:
        final Exception e = assertThrows(IllegalArgumentException.class, () -> registry.topic(def));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "The supplied topic descriptor does not match the topic descriptor found when inspecting components."));
        assertThat(
                e.getMessage(), containsString("supplied=" + KafkaTopicDescriptors.asString(def)));
        assertThat(
                e.getMessage(),
                containsString("actual=" + KafkaTopicDescriptors.asString(topicDefA)));
    }

    @Test
    void shouldThrowOnUnknownTopic() {
        // Given:
        when(topicDefA.name()).thenReturn("unknown");

        // When:
        final Exception e =
                assertThrows(IllegalArgumentException.class, () -> registry.topic(topicDefA));

        // Then:
        assertThat(
                e.getMessage(),
                is("Unknown topic. No component has a topic of the supplied name. topic=unknown"));
    }

    private <K, V> void setUpTopicDef(
            final KafkaTopicDescriptor<K, V> def,
            final String name,
            final Class<K> keyType,
            final Class<V> valueType) {
        final PartDescriptor<K> key = part(keyType);
        final PartDescriptor<V> value = part(valueType);

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
