/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.schema.resource;

import static org.creekservice.api.base.type.CodeLocation.codeLocation;
import static org.creekservice.internal.kafka.serde.json.util.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.schema.JsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.schema.OwnedJsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.schema.UnownedJsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.internal.kafka.serde.json.model.TestValueV0;
import org.creekservice.internal.kafka.serde.json.util.TopicDescriptors;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaResourceValidatorTest {

    private static final OwnedKafkaTopicInput<?, TestValueV0> VALID_TOPIC =
            TopicDescriptors.inputTopic(
                    "bob", "john", "fred", int.class, TestValueV0.class, withPartitions(1));

    private SchemaResourceValidator validator;
    private OwnedJsonSchemaDescriptor<?> owned;
    private UnownedJsonSchemaDescriptor<?> unowned;

    @BeforeEach
    void setUp() {
        owned = (OwnedJsonSchemaDescriptor<?>) createSpy(true);
        unowned = (UnownedJsonSchemaDescriptor<?>) createSpy(false);
        validator = new SchemaResourceValidator();
    }

    @Test
    void shouldThrowOnNullId() {
        // Given:
        when(owned.id()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(owned, unowned)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid schema descriptor: id() is null"));
        assertThat(e.getMessage(), containsString(", id: unknown"));
        assertThat(e.getMessage(), containsString(", topic: kafka-topic://bob/fred"));
        assertThat(e.getMessage(), containsString(", part: value"));
        assertThat(e.getMessage(), containsString(", location: " + codeLocation(owned)));
    }

    @Test
    void shouldThrowOnNullSchemaRegistryName() {
        // Given:
        when(unowned.schemaRegistryName()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(owned, unowned)));
        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Invalid schema descriptor: schemaRegistryName() is null"));
        assertThat(e.getMessage(), containsString(", id: schema:/fred/value"));
        assertThat(e.getMessage(), containsString(", topic: kafka-topic://bob/fred"));
        assertThat(e.getMessage(), containsString(", part: value"));
        assertThat(e.getMessage(), containsString(", location: " + codeLocation(owned)));
    }

    @Test
    void shouldThrowOnNullResources() {
        // Given:
        when(owned.resources()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(owned, unowned)));

        // Then:
        assertThat(e.getMessage(), startsWith("Invalid schema descriptor: resources() is null"));
        assertThat(e.getMessage(), containsLookUpInfo(owned));
    }

    @Test
    void shouldThrowOnNullPart() {
        // Given:
        when(owned.part()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(owned, unowned)));
        // Then:
        assertThat(e.getMessage(), startsWith("Invalid schema descriptor: part() is null"));
        assertThat(e.getMessage(), containsString(", id: unknown"));
        assertThat(e.getMessage(), containsString(", topic: unknown"));
        assertThat(e.getMessage(), containsString(", part: null"));
        assertThat(e.getMessage(), containsString(", location: " + codeLocation(owned)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldThrowOnNullPartTopic() {
        // Given:
        final PartDescriptor<TestValueV0> part = spy(VALID_TOPIC.value());
        when(owned.part()).thenReturn((PartDescriptor) part);
        when(part.topic()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(owned, unowned)));
        // Then:
        assertThat(e.getMessage(), startsWith("Invalid schema descriptor: part().topic() is null"));
        assertThat(e.getMessage(), containsString(", id: unknown"));
        assertThat(e.getMessage(), containsString(", topic: unknown"));
        assertThat(e.getMessage(), containsString(", part: value"));
        assertThat(e.getMessage(), containsString(", location: " + codeLocation(owned)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldThrowOnNullPartTopicId() {
        // Given:
        final PartDescriptor<TestValueV0> part = spy(VALID_TOPIC.value());
        when(owned.part()).thenReturn((PartDescriptor) part);
        final OwnedKafkaTopicInput<?, ?> topic = spy(VALID_TOPIC);
        when(part.topic()).thenReturn((KafkaTopicDescriptor) topic);
        when(topic.id()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(owned, unowned)));
        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Invalid schema descriptor: part().topic().id() is null"));
        assertThat(e.getMessage(), containsString(", id: schema://john/fred/value"));
        assertThat(e.getMessage(), containsString(", topic: unknown"));
        assertThat(e.getMessage(), containsString(", part: value"));
        assertThat(e.getMessage(), containsString(", location: " + codeLocation(owned)));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldThrowOnNullPartResources() {
        // Given:
        final PartDescriptor<TestValueV0> part = spy(VALID_TOPIC.value());
        when(owned.part()).thenReturn((PartDescriptor) part);
        when(part.resources()).thenReturn(null);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(owned, unowned)));
        // Then:
        assertThat(
                e.getMessage(),
                startsWith("Invalid schema descriptor: part().resources() is null"));
        assertThat(e.getMessage(), containsLookUpInfo(owned));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldThrowIfPartDoesNotHaveSchemaAsResource() {
        // Given:
        final PartDescriptor<TestValueV0> part = spy(VALID_TOPIC.value());
        when(owned.part()).thenReturn((PartDescriptor) part);
        when(part.resources()).thenAnswer(inv -> Stream.of());

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(owned, unowned)));
        // Then:
        assertThat(
                e.getMessage(),
                startsWith(
                        "Invalid schema descriptor: part().resources() does not include this schema"
                                + " resource"));
        assertThat(e.getMessage(), containsLookUpInfo(owned));
    }

    @Test
    void shouldThrowIfRegistryNameDifferent() {
        // Given:
        when(unowned.schemaRegistryName()).thenReturn("different");

        // Then:
        assertValidateGroupThrows(owned, unowned);
    }

    @Test
    void shouldThrowIfPartNameDifferent() {
        // Given:
        final PartDescriptor<?> part = ensureSchemaIncludedTypePart(VALID_TOPIC, unowned);
        final PartDescriptor.Part different =
                part.name() == PartDescriptor.Part.key
                        ? PartDescriptor.Part.value
                        : PartDescriptor.Part.key;
        when(part.name()).thenReturn(different);

        // Then:
        assertValidateGroupThrows(owned, unowned);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldThrowIfTopicIdDifferent() {
        // Given:
        final PartDescriptor<?> part = ensureSchemaIncludedTypePart(VALID_TOPIC, unowned);
        final OwnedKafkaTopicInput<?, TestValueV0> topic = spy(VALID_TOPIC);
        when(part.topic()).thenReturn((KafkaTopicDescriptor) topic);
        when(topic.id()).thenReturn(URI.create("different"));

        // Then:
        assertValidateGroupThrows(owned, unowned);
    }

    @Test
    void shouldThrowIfResourcesAreDifferent() {
        // Given:
        final ResourceDescriptor different = mock();
        when(different.id()).thenReturn(URI.create("different"));
        when(owned.resources()).thenAnswer(inv -> Stream.of(different));

        // Then:
        assertValidateGroupThrows(owned, unowned);
    }

    @Test
    void shouldNotThrowIfEverythingIsValid() {
        validator.validateGroup(List.of(owned, unowned, owned, unowned));
    }

    private void assertValidateGroupThrows(
            final JsonSchemaDescriptor<?> schema1, final JsonSchemaDescriptor<?> schema2) {
        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> validator.validateGroup(List.of(schema1, schema2, schema2, schema1)));

        assertThat(
                e.getMessage(),
                startsWith(
                        "Resource descriptors for the same resource disagree on the details"
                                + ", id: "
                                + schema1.id()
                                + ", descriptors: ["));

        assertThat(e.getMessage(), containsLookUpInfo(schema1));
        assertThat(e.getMessage(), containsLookUpInfo(schema2));
    }

    private Matcher<String> containsLookUpInfo(final JsonSchemaDescriptor<?> schema) {
        return allOf(
                containsString(", id: schema://john/fred/value"),
                containsString(", topic: kafka-topic://bob/fred"),
                containsString(", part: value"),
                containsString(", location: " + codeLocation(schema)));
    }

    private static JsonSchemaDescriptor<?> createSpy(final boolean owned) {
        final KafkaTopicDescriptor<?, ?> actualTopic = owned ? VALID_TOPIC : VALID_TOPIC.toOutput();

        final JsonSchemaDescriptor<?> actualSchema =
                actualTopic
                        .resources()
                        .filter(JsonSchemaDescriptor.class::isInstance)
                        .map(res -> (JsonSchemaDescriptor<?>) res)
                        .findAny()
                        .orElseThrow();

        final JsonSchemaDescriptor<?> spy = spy(actualSchema);
        ensureSchemaIncludedTypePart(actualTopic, spy);
        return spy;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static PartDescriptor<?> ensureSchemaIncludedTypePart(
            final KafkaTopicDescriptor<?, ?> topic, final JsonSchemaDescriptor<?> schema) {
        final PartDescriptor<?> part = spy(topic.value());
        when(schema.part()).thenReturn((PartDescriptor) part);
        when(part.resources()).thenAnswer(inv -> Stream.of(schema));
        return part;
    }
}
