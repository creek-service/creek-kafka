/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.serde.provider.KafkaSystemTestSerdeProvider.SystemTestSerde;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestKafkaTopicTest {

    private static final String TOPIC_NAME = "test-topic";

    @Mock private KafkaTopicDescriptor<String, Long> descriptor;
    @Mock private KafkaTopicDescriptor.PartDescriptor<String> keyPart;
    @Mock private KafkaTopicDescriptor.PartDescriptor<Long> valuePart;
    @Mock private SystemTestSerde keyFactory;
    @Mock private SystemTestSerde valueFactory;

    private TestKafkaTopic testTopic;

    @BeforeEach
    void setUp() {
        when(descriptor.key()).thenReturn(keyPart);
        when(descriptor.value()).thenReturn(valuePart);
        when(descriptor.name()).thenReturn(TOPIC_NAME);

        testTopic = new TestKafkaTopic(descriptor, keyFactory, valueFactory);
    }

    @Test
    void shouldReturnDescriptor() {
        assertThat(testTopic.descriptor(), is(sameInstance(descriptor)));
    }

    @Test
    void shouldSerializeKey() {
        // Given:
        final Object key = "some-key";
        final byte[] expected = new byte[] {1, 2, 3};
        when(keyFactory.serialize(key, keyPart, TOPIC_NAME)).thenReturn(expected);

        // When:
        final byte[] result = testTopic.serializeKey(key);

        // Then:
        assertThat(result, is(expected));
        verify(keyFactory).serialize(key, keyPart, TOPIC_NAME);
    }

    @Test
    void shouldSerializeValue() {
        // Given:
        final Object value = 42L;
        final byte[] expected = new byte[] {4, 5, 6};
        when(valueFactory.serialize(value, valuePart, TOPIC_NAME)).thenReturn(expected);

        // When:
        final byte[] result = testTopic.serializeValue(value);

        // Then:
        assertThat(result, is(expected));
        verify(valueFactory).serialize(value, valuePart, TOPIC_NAME);
    }

    @Test
    void shouldDeserializeKey() {
        // Given:
        final byte[] key = new byte[] {1, 2, 3};
        final Object expected = "deserialized-key";
        when(keyFactory.deserialize(key, keyPart, TOPIC_NAME)).thenReturn(expected);

        // When:
        final Object result = testTopic.deserializeKey(key);

        // Then:
        assertThat(result, is(expected));
        verify(keyFactory).deserialize(key, keyPart, TOPIC_NAME);
    }

    @Test
    void shouldDeserializeValue() {
        // Given:
        final byte[] value = new byte[] {4, 5, 6};
        final Object expected = "deserialized-value";
        when(valueFactory.deserialize(value, valuePart, TOPIC_NAME)).thenReturn(expected);

        // When:
        final Object result = testTopic.deserializeValue(value);

        // Then:
        assertThat(result, is(expected));
        verify(valueFactory).deserialize(value, valuePart, TOPIC_NAME);
    }

    @Test
    void shouldNormaliseKey() {
        // Given:
        final Object key = "raw-key";
        final Object expected = "normalised-key";
        when(keyFactory.normalise(key, keyPart, TOPIC_NAME)).thenReturn(expected);

        // When:
        final Object result = testTopic.normaliseKey(key);

        // Then:
        assertThat(result, is(expected));
        verify(keyFactory).normalise(key, keyPart, TOPIC_NAME);
    }

    @Test
    void shouldNormaliseValue() {
        // Given:
        final Object value = "raw-value";
        final Object expected = "normalised-value";
        when(valueFactory.normalise(value, valuePart, TOPIC_NAME)).thenReturn(expected);

        // When:
        final Object result = testTopic.normaliseValue(value);

        // Then:
        assertThat(result, is(expected));
        verify(valueFactory).normalise(value, valuePart, TOPIC_NAME);
    }

    @Test
    void shouldUseDifferentFactoriesForKeyAndValue() {
        // Given:
        final Object key = "k";
        final Object value = "v";
        final byte[] keyBytes = new byte[] {10};
        final byte[] valueBytes = new byte[] {20};
        when(keyFactory.serialize(key, keyPart, TOPIC_NAME)).thenReturn(keyBytes);
        when(valueFactory.serialize(value, valuePart, TOPIC_NAME)).thenReturn(valueBytes);

        // When:
        final byte[] serializedKey = testTopic.serializeKey(key);
        final byte[] serializedValue = testTopic.serializeValue(value);

        // Then:
        assertThat(serializedKey, is(keyBytes));
        assertThat(serializedValue, is(valueBytes));
        verify(keyFactory).serialize(key, keyPart, TOPIC_NAME);
        verify(valueFactory).serialize(value, valuePart, TOPIC_NAME);
    }
}
