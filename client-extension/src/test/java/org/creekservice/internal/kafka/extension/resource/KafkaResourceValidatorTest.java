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

import static org.creekservice.api.kafka.metadata.SerializationFormat.serializationFormat;
import static org.creekservice.test.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicConfig;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.test.TopicDescriptors;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class KafkaResourceValidatorTest {

    private static final OwnedKafkaTopicInput<Long, String> VALID_TOPIC =
            TopicDescriptors.inputTopic(
                    "bob", "john", "fred", long.class, String.class, withPartitions(1));

    private KafkaTopicDescriptor<Long, String> topic;
    private CreatableKafkaTopic<Long, String> creatableTopic;
    private KafkaResourceValidator validator;

    @BeforeEach
    void setUp() {
        topic = createMockTopic(VALID_TOPIC.toOutput());
        creatableTopic = createMockTopic(VALID_TOPIC);

        validator = new KafkaResourceValidator();
    }

    @Test
    void shouldThrowOnNullId() {
        // Given:
        when(topic.id()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: id() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @Test
    void shouldThrowOnNullTopicName() {
        // Given:
        when(topic.name()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: name() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @Test
    void shouldThrowOnBlankTopicName() {
        // Given:
        when(topic.name()).thenReturn(" ");

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: name() is blank"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @Test
    void shouldThrowOnNullClusterName() {
        // Given:
        when(topic.cluster()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: cluster() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @Test
    void shouldThrowOnBlankClusterName() {
        // Given:
        when(topic.cluster()).thenReturn("");

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: cluster() is blank"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @Test
    void shouldThrowOnInvalidClusterName() {
        // Given:
        when(topic.cluster()).thenReturn("invalid-123!x");

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Invalid topic descriptor: cluster() is invalid: illegal character '!'."
                                + " Only alpha-numerics and '-' are supported."));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @Test
    void shouldThrowOnNullConfig() {
        // Given:
        when(creatableTopic.config()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(creatableTopic)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid topic descriptor: config() is null"));
        assertThat(e.getMessage(), containsTopicDetails(creatableTopic));
    }

    @Test
    void shouldThrowIfPartitionCountDifferent() {
        // Given:
        final CreatableKafkaTopic<Long, String> creatableTopic2 = createMockTopic(VALID_TOPIC);
        final KafkaTopicConfig config = spy(creatableTopic2.config());
        when(creatableTopic2.config()).thenReturn(config);
        when(config.partitions()).thenReturn(35);

        // Then:
        assertValidateGroupThrows(creatableTopic, creatableTopic2);
    }

    @Test
    void shouldThrowIfConfigDifferent() {
        // Given:
        final CreatableKafkaTopic<Long, String> creatableTopic2 = createMockTopic(VALID_TOPIC);
        final KafkaTopicConfig config = spy(creatableTopic2.config());
        when(creatableTopic2.config()).thenReturn(config);
        when(config.config()).thenReturn(Map.of("this", "that"));

        // Then:
        assertValidateGroupThrows(creatableTopic, creatableTopic2);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowOnNullPart(final PartSelector partSelector, final String partName) {
        // Given:
        when(partSelector.apply(topic)).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(), startsWith("Invalid topic descriptor: " + partName + "() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowOnNullPartName(final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        when(part.name()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Invalid topic descriptor: " + partName + "().name() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowOnNullPartFormat(final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        when(part.format()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Invalid topic descriptor: " + partName + "().format() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowOnNullPartType(final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        when(part.type()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Invalid topic descriptor: " + partName + "().type() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowOnNullPartResources(final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        when(part.resources()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Invalid topic descriptor: " + partName + "().resources() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowOnNullPartTopic(final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        when(part.topic()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Invalid topic descriptor: " + partName + "().topic() is null"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowIfPartReferencesWrongTopic(
            final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        when(part.topic()).thenReturn((KafkaTopicDescriptor) VALID_TOPIC);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Invalid topic descriptor: "
                                + partName
                                + "().topic() does not return the owning topic"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowIfTopicDoesNotIncPartResources(
            final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        final ResourceDescriptor missing = mock();
        when(missing.id()).thenReturn(URI.create("res://missing"));
        final ResourceDescriptor matched = mock();
        when(matched.id()).thenReturn(URI.create("res://matched"));
        final ResourceDescriptor additional = mock();
        when(additional.id()).thenReturn(URI.create("res://additional"));
        when(part.resources()).thenAnswer(inv -> Stream.of(missing, matched));
        when(topic.resources()).thenAnswer(inv -> Stream.of(matched, additional));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> validator.validateGroup(List.of(topic)));

        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Invalid topic descriptor: "
                                + partName
                                + "().topic().resources() does not include all "
                                + partName
                                + "() resources, missing: res://missing"));
        assertThat(e.getMessage(), containsTopicDetails(topic));
    }

    @Test
    void shouldThrowIfNameDifferent() {
        // Given:
        when(topic.name()).thenReturn("different");

        // Then:
        assertValidateGroupThrows(topic, creatableTopic);
    }

    @Test
    void shouldThrowIfClusterDifferent() {
        // Given:
        when(topic.cluster()).thenReturn("different");

        // Then:
        assertValidateGroupThrows(topic, creatableTopic);
    }

    @Test
    void shouldThrowIfResourcesDifferent() {
        // Given:
        final ResourceDescriptor additional = mock();
        when(additional.id()).thenReturn(URI.create("res://additional"));
        when(topic.resources()).thenAnswer(inv -> Stream.of(additional));

        // Then:
        assertValidateGroupThrows(creatableTopic, topic);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowIfPartNameDifferent(final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        final Part different = part.name() == Part.key ? Part.value : Part.key;
        when(part.name()).thenReturn(different);

        // Then:
        assertValidateGroupThrows(topic, creatableTopic);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowIfPartFormatDifferent(final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        when(part.format()).thenReturn(serializationFormat("different"));

        // Then:
        assertValidateGroupThrows(topic, creatableTopic);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @ParameterizedTest(name = "{1}")
    @MethodSource("topicParts")
    void shouldThrowIfPartTypeDifferent(final PartSelector partSelector, final String partName) {
        // Given:
        final PartDescriptor<?> part = partSelector.apply(topic);
        when(part.type()).thenReturn((Class) Boolean.class);

        // Then:
        assertValidateGroupThrows(topic, creatableTopic);
    }

    @Test
    void shouldNotThrowOnConsistentGroupWithMixOfCreatableAndNonCreatable() {
        // When:
        validator.validateGroup(List.of(topic, creatableTopic));

        // Then: did not throw.
    }

    @Test
    void shouldNotThrowOnConsistentGroupWithJustNonCreatable() {
        // Given:
        final KafkaTopicDescriptor<Long, String> topic2 = createMockTopic(VALID_TOPIC.toOutput());

        // When:
        validator.validateGroup(List.of(topic, topic2));

        // Then: did not throw.
    }

    @Test
    void shouldNotThrowOnConsistentGroupWithJustCreatable() {
        // Given:
        final CreatableKafkaTopic<Long, String> creatableTopic2 = createMockTopic(VALID_TOPIC);

        // When:
        validator.validateGroup(List.of(creatableTopic, creatableTopic2));

        // Then: did not throw.
    }

    private void assertValidateGroupThrows(
            final KafkaTopicDescriptor<Long, String> topic1,
            final KafkaTopicDescriptor<Long, String> topic2) {
        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(topic1, topic2, topic1, topic2)));

        assertThat(
                e.getMessage(),
                startsWith(
                        "Resource descriptors for the same resource disagree on the details."
                                + " id: "
                                + topic1.id()
                                + ", descriptors: ["));

        assertThat(e.getMessage(), containsTopicDetails(topic1));
        assertThat(e.getMessage(), containsTopicDetails(topic2));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T extends KafkaTopicDescriptor<Long, String>> T createMockTopic(
            final T actual) {
        final T topic = spy(actual);
        final PartDescriptor<Long> key = spy(actual.key());
        final PartDescriptor<String> value = spy(actual.value());

        when(topic.key()).thenReturn(key);
        when(key.topic()).thenReturn((KafkaTopicDescriptor) topic);

        when(topic.value()).thenReturn(value);
        when(value.topic()).thenReturn((KafkaTopicDescriptor) topic);

        return topic;
    }

    private static Matcher<String> containsTopicDetails(
            final KafkaTopicDescriptor<Long, String> topic) {
        final List<Matcher<? super String>> matchers = new ArrayList<>();
        matchers.add(containsString(topic.getClass().getSimpleName()));
        matchers.add(containsString("[id: "));
        matchers.add(containsString(", name: " + topic.name()));
        matchers.add(containsString(", cluster: " + topic.cluster()));

        matchers.add(
                containsString(
                        ", key: "
                                + Optional.ofNullable(topic.key())
                                        .map(
                                                part ->
                                                        "TopicPart[format: "
                                                                + part.format()
                                                                + ", type: "
                                                                + Optional.ofNullable(part.type())
                                                                        .map(Class::getName)
                                                                        .orElse("null")
                                                                + "]")
                                        .orElse("null")));

        matchers.add(
                containsString(
                        ", value: "
                                + Optional.ofNullable(topic.value())
                                        .map(
                                                part ->
                                                        "TopicPart[format: "
                                                                + part.format()
                                                                + ", type: "
                                                                + Optional.ofNullable(part.type())
                                                                        .map(Class::getName)
                                                                        .orElse("null")
                                                                + "]")
                                        .orElse("null")));

        if (topic instanceof CreatableKafkaTopic) {
            matchers.add(
                    containsString(
                            ", config: "
                                    + Optional.of((CreatableKafkaTopic<?, ?>) topic)
                                            .map(CreatableKafkaTopic::config)
                                            .map(
                                                    config ->
                                                            "TopicConfig[partitions: "
                                                                    + config.partitions()
                                                                    + ", config: "
                                                                    + config.config()
                                                                    + "]")
                                            .orElse("null")));
        }

        return allOf(matchers);
    }

    public static Stream<Arguments> topicParts() {
        return Stream.of(
                Arguments.of((PartSelector) KafkaTopicDescriptor::key, "key"),
                Arguments.of((PartSelector) KafkaTopicDescriptor::value, "value"));
    }

    @FunctionalInterface
    private interface PartSelector
            extends Function<KafkaTopicDescriptor<?, ?>, PartDescriptor<?>> {}
}
