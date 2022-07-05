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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
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
class KafkaResourceValidatorTest {

    private static final SerializationFormat SOME_FORMAT = serializationFormat("something");

    @Mock private ComponentDescriptor componentA;
    @Mock private ComponentDescriptor componentB;
    @Mock private PartDescriptor<Long> topicKey;
    @Mock private PartDescriptor<String> topicValue;
    @Mock private KafkaTopicDescriptor<Long, String> topic;
    private KafkaResourceValidator validator;

    @BeforeEach
    void setUp() {
        when(topicKey.type()).thenReturn(long.class);
        when(topicKey.format()).thenReturn(SOME_FORMAT);

        when(topicValue.type()).thenReturn(String.class);
        when(topicValue.format()).thenReturn(SOME_FORMAT);

        when(topic.name()).thenReturn("some-topic");
        when(topic.cluster()).thenReturn(KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME);
        when(topic.key()).thenReturn(topicKey);
        when(topic.value()).thenReturn(topicValue);

        when(componentA.resources()).thenReturn(Stream.of(topic));

        validator = new KafkaResourceValidator();
    }

    @Test
    void shouldNotBlowUpIfNoKafkaResources() {
        // Given:
        final ResourceDescriptor otherResource = mock(ResourceDescriptor.class);
        when(componentA.resources()).thenReturn(Stream.of(otherResource));

        // When:
        validator.validate(Stream.of(componentA));

        // Then: did not blow up
    }

    @Test
    void shouldThrowOnNullTopicName() {
        // Given:
        when(topic.name()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: name() is null"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldThrowOnBlankTopicName() {
        // Given:
        when(topic.name()).thenReturn(" ");

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: name() is blank"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldThrowOnNullClusterName() {
        // Given:
        when(topic.cluster()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: cluster() is null"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldNotThrowOnBlankClusterName() {
        // Given:
        when(topic.cluster()).thenReturn("");

        // When:
        validator.validate(Stream.of(componentA));

        // Then: did not throw.
    }

    @Test
    void shouldThrowOnInvalidClusterName() {
        // Given:
        when(topic.cluster()).thenReturn("invalid_name");

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Invalid topic descriptor: cluster() is invalid: illegal character '_'. Only alpha-numerics and '-' are supported."));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldThrowOnNullKey() {
        // Given:
        when(topic.key()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: key() is null"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldThrowOnNullKeyType() {
        // Given:
        when(topicKey.type()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: key().type() is null"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldThrowOnNullKeyFormat() {
        // Given:
        when(topicKey.format()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: key().format() is null"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldThrowOnNullValue() {
        // Given:
        when(topic.value()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: value() is null"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldThrowOnNullValueType() {
        // Given:
        when(topicValue.type()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: value().type() is null"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldThrowOnNullValueFormat() {
        // Given:
        when(topicValue.format()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> validator.validate(Stream.of(componentA)));

        // Then:
        assertThat(
                e.getMessage(), startsWith("Invalid topic descriptor: value().format() is null"));
        assertThat(e.getMessage(), containsString(KafkaTopicDescriptors.asString(topic)));
    }

    @Test
    void shouldCheckEachComponent() {
        // Given:
        when(componentA.resources()).thenReturn(Stream.of());
        when(componentB.resources()).thenReturn(Stream.of(topic));
        when(topicValue.type()).thenReturn(null);

        // Then:
        assertThrows(
                RuntimeException.class,
                () -> validator.validate(Stream.of(componentA, componentB)));
    }
}
