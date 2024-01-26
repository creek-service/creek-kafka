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

package org.creekservice.internal.kafka.serde.json;

import static org.creekservice.api.base.type.CodeLocation.codeLocation;
import static org.creekservice.internal.kafka.serde.json.util.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.schema.JsonSchemaDescriptor;
import org.creekservice.api.kafka.metadata.schema.SchemaDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaStoreEndpoints;
import org.creekservice.api.kafka.serde.test.KafkaSerdeProviderFunctionalFixture;
import org.creekservice.internal.kafka.serde.json.model.IncompatibleValue;
import org.creekservice.internal.kafka.serde.json.model.TestKeyAddingMandatory;
import org.creekservice.internal.kafka.serde.json.model.TestKeyV0;
import org.creekservice.internal.kafka.serde.json.model.TestValueV0;
import org.creekservice.internal.kafka.serde.json.model.TestValueV1;
import org.creekservice.internal.kafka.serde.json.model.TypeWithEnum;
import org.creekservice.internal.kafka.serde.json.model.TypeWithExplicitPolymorphism;
import org.creekservice.internal.kafka.serde.json.model.TypeWithImplicitPolymorphism;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;
import org.creekservice.internal.kafka.serde.json.schema.validation.SchemaFriendValidator.JsonSchemaValidationFailed;
import org.creekservice.internal.kafka.serde.json.util.TopicDescriptors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Tag("ContainerisedTest")
@Execution(ExecutionMode.SAME_THREAD) // Due to static state
@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED")
class JsonSchemaSerdeFunctionalTest {

    private static final String CLUSTER_NAME_1 = KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
    private static final String SCHEMA_REGISTRY_NAME_1 =
            SchemaDescriptor.DEFAULT_SCHEMA_REGISTRY_NAME;
    private static final String CLUSTER_NAME_2 = "cluster-2";
    private static final String SCHEMA_REGISTRY_NAME_2 = "sr-2";

    private static final OwnedKafkaTopicInput<TestKeyV0, TestValueV0> TEST_TOPIC_1 =
            TopicDescriptors.inputTopic(
                    CLUSTER_NAME_1,
                    SCHEMA_REGISTRY_NAME_1,
                    "Bob",
                    TestKeyV0.class,
                    TestValueV0.class,
                    withPartitions(1));

    private static final OwnedKafkaTopicInput<TestKeyV0, TestValueV1> TEST_TOPIC_2 =
            TopicDescriptors.inputTopic(
                    CLUSTER_NAME_2,
                    SCHEMA_REGISTRY_NAME_2,
                    "Bob",
                    TestKeyV0.class,
                    TestValueV1.class,
                    withPartitions(1));
    private static final OwnedKafkaTopicInput<TestKeyV0, TestValueV0> EVOLVING_TOPIC =
            TopicDescriptors.inputTopic(
                    CLUSTER_NAME_1,
                    SCHEMA_REGISTRY_NAME_1,
                    "evolving",
                    TestKeyV0.class,
                    TestValueV0.class,
                    withPartitions(1));

    private static final OwnedKafkaTopicInput<TestKeyV0, TypeWithEnum> ENUM_TOPIC =
            TopicDescriptors.inputTopic(
                    CLUSTER_NAME_1,
                    SCHEMA_REGISTRY_NAME_1,
                    "with-enums",
                    TestKeyV0.class,
                    TypeWithEnum.class,
                    withPartitions(1));

    private static final OwnedKafkaTopicInput<
                    TypeWithExplicitPolymorphism, TypeWithExplicitPolymorphism>
            EXPLICIT_POLY_TOPIC =
                    TopicDescriptors.inputTopic(
                            CLUSTER_NAME_1,
                            SCHEMA_REGISTRY_NAME_1,
                            "explicit-polymorphic",
                            TypeWithExplicitPolymorphism.class,
                            TypeWithExplicitPolymorphism.class,
                            withPartitions(1));

    private static final OwnedKafkaTopicInput<
                    TypeWithImplicitPolymorphism, TypeWithImplicitPolymorphism>
            IMPLICIT_POLY_TOPIC =
                    TopicDescriptors.inputTopic(
                            CLUSTER_NAME_1,
                            SCHEMA_REGISTRY_NAME_1,
                            "implicit-polymorphic",
                            TypeWithImplicitPolymorphism.class,
                            TypeWithImplicitPolymorphism.class,
                            withPartitions(1));

    private static final KafkaSerdeProviderFunctionalFixture TEST_FIXTURE =
            KafkaSerdeProviderFunctionalFixture.tester(
                    List.of(
                            TEST_TOPIC_1,
                            TEST_TOPIC_2,
                            EVOLVING_TOPIC,
                            ENUM_TOPIC,
                            EXPLICIT_POLY_TOPIC,
                            IMPLICIT_POLY_TOPIC));

    @Container
    private static final SchemaRegistryContainer SCHEMA_REGISTRY_1 =
            new SchemaRegistryContainer(
                            DockerImageName.parse("confluentinc/cp-schema-registry:7.3.1"),
                            TEST_FIXTURE.kafkaContainer(CLUSTER_NAME_1))
                    .withStartupAttempts(3)
                    .withStartupTimeout(Duration.ofSeconds(90));

    @Container
    private static final SchemaRegistryContainer SCHEMA_REGISTRY_2 =
            new SchemaRegistryContainer(
                            DockerImageName.parse("confluentinc/cp-schema-registry:7.3.1"),
                            TEST_FIXTURE.kafkaContainer(CLUSTER_NAME_2))
                    .withStartupAttempts(3)
                    .withStartupTimeout(Duration.ofSeconds(90));

    private static Map<String, CachedSchemaRegistryClient> srClients;
    private static KafkaSerdeProviderFunctionalFixture.Tester tester;

    @BeforeAll
    static void classSetUp() {
        final Map<String, SchemaRegistryContainer> srInstances =
                Map.of(
                        SCHEMA_REGISTRY_NAME_1,
                        SCHEMA_REGISTRY_1,
                        SCHEMA_REGISTRY_NAME_2,
                        SCHEMA_REGISTRY_2);

        tester =
                TEST_FIXTURE
                        .withExtensionOption(
                                JsonSerdeExtensionOptions.builder()
                                        .withTypeOverride(
                                                SchemaStoreEndpoints.Loader.class,
                                                clusterName ->
                                                        srInstances
                                                                .get(clusterName)
                                                                .clientEndpoint())
                                        .withSubtypes(
                                                TypeWithImplicitPolymorphism.ImplicitlyNamed.class,
                                                TypeWithImplicitPolymorphism.ExplicitlyNamed.class)
                                        .build())
                        .start();

        srClients =
                srInstances.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e ->
                                                new CachedSchemaRegistryClient(
                                                        e
                                                                .getValue()
                                                                .clientEndpoint()
                                                                .endpoints()
                                                                .stream()
                                                                .map(URI::toString)
                                                                .collect(Collectors.toList()),
                                                        10,
                                                        List.of(new JsonSchemaProvider()),
                                                        Map.of())));
    }

    @AfterAll
    static void classTearDown() {
        TEST_FIXTURE.stop();
    }

    @Test
    void shouldRegisterSchema() throws Exception {
        final KafkaTopic<TestKeyV0, TestValueV0> topic = tester.topic(TEST_TOPIC_1);
        assertThat(numSchemaVersions(topic.descriptor().key()), is(1));
        assertThat(numSchemaVersions(topic.descriptor().value()), is(1));
    }

    @Test
    void shouldRegisterSchemaOnDifferentInstance() throws Exception {
        final KafkaTopic<TestKeyV0, TestValueV1> topic = tester.topic(TEST_TOPIC_2);
        assertThat(numSchemaVersions(topic.descriptor().key()), is(1));
        assertThat(numSchemaVersions(topic.descriptor().value()), is(1));
    }

    @Test
    void shouldFailRegisteringAnIncompatibleKeySchema() {
        // Given:
        final OwnedKafkaTopicInput<TestKeyAddingMandatory, TestValueV0> evolvedTopic =
                TopicDescriptors.inputTopic(
                        TEST_TOPIC_1.cluster(),
                        SCHEMA_REGISTRY_NAME_1,
                        TEST_TOPIC_1.name(),
                        TestKeyAddingMandatory.class,
                        TEST_TOPIC_1.value().type(),
                        withPartitions(1));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> tester.testEvolution(evolvedTopic));

        // Then:
        assertThat(e.getMessage(), containsString("Schema store operation failed, "));
        assertThat(e.getMessage(), containsString(", topic: Bob"));
        assertThat(e.getMessage(), containsString(", part: key"));
        assertThat(
                e.getMessage(),
                containsString(", type: " + TestKeyAddingMandatory.class.getName()));
        assertThat(
                e.getCause().getMessage(),
                containsString(
                        "proposed consumer schema is not compatible with existing producer"
                                + " schema"));
        assertThat(
                e.getCause().getMessage(),
                containsString("REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL"));
    }

    @Test
    void shouldFailRegisteringAnIncompatibleValueSchema() {
        // Given:
        final OwnedKafkaTopicInput<TestKeyV0, IncompatibleValue> evolvedTopic =
                TopicDescriptors.inputTopic(
                        TEST_TOPIC_1.cluster(),
                        SCHEMA_REGISTRY_NAME_1,
                        TEST_TOPIC_1.name(),
                        TEST_TOPIC_1.key().type(),
                        IncompatibleValue.class,
                        withPartitions(1));

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> tester.testEvolution(evolvedTopic));

        // Then:
        assertThat(e.getMessage(), containsString("Schema store operation failed, "));
        assertThat(e.getMessage(), containsString(", topic: Bob"));
        assertThat(e.getMessage(), containsString(", part: value"));
        assertThat(e.getMessage(), containsString(", type: " + IncompatibleValue.class.getName()));
        assertThat(
                e.getCause().getMessage(),
                containsString(
                        "proposed consumer schema is not compatible with existing producer"
                                + " schema"));
        assertThat(e.getCause().getMessage(), containsString("TYPE_CHANGED"));
    }

    @Test
    void shouldProduceAndConsumeToKafkaTopic() {
        tester.testProduceConsume(
                TEST_TOPIC_1,
                new TestKeyV0(24, OptionalInt.of(2)),
                new TestValueV0(Optional.of("Jane"), 24));
    }

    @Test
    void shouldProduceAndConsumeToDifferentCluster() {
        tester.testProduceConsume(
                TEST_TOPIC_2,
                new TestKeyV0(24, OptionalInt.of(2)),
                new TestValueV1(Optional.of("Jane"), 24));
    }

    @Test
    void shouldFailToProduceWithInvalidKey() {
        assertThrows(
                JsonSchemaValidationFailed.class,
                () ->
                        tester.testProduceConsume(
                                TEST_TOPIC_1,
                                new TestKeyV0(-1, OptionalInt.of(2)),
                                new TestValueV0(Optional.of("Jane"), 24)));
    }

    @Test
    void shouldFailToProduceWithInvalidValue() {
        assertThrows(
                JsonSchemaValidationFailed.class,
                () ->
                        tester.testProduceConsume(
                                TEST_TOPIC_1,
                                new TestKeyV0(1, OptionalInt.empty()),
                                new TestValueV0(Optional.of("Jane"), -1)));
    }

    @Test
    void shouldThrowIfTypeHasNoSchema() {
        // Given:
        final OwnedKafkaTopicInput<?, ?> invalid =
                TopicDescriptors.inputTopic(
                        CLUSTER_NAME_1,
                        SCHEMA_REGISTRY_NAME_1,
                        "invalid",
                        TestKeyV0.class,
                        JsonSchemaSerdeFunctionalTest.class,
                        withPartitions(1));

        // When:
        final Exception e =
                assertThrows(SchemaException.class, () -> tester.testEvolution(invalid));

        // Then:
        assertThat(
                e.getMessage(),
                containsString(
                        "Failed to load schema resource:"
                            + " /schema/json/org.creekservice.internal.kafka.serde.json.json_schema_serde_functional_test.yml."
                            + " Resource not found."));
    }

    @Test
    void shouldProduceAndConsumeEnums() {
        tester.testProduceConsume(
                ENUM_TOPIC,
                new TestKeyV0(24, OptionalInt.empty()),
                new TypeWithEnum(TypeWithEnum.Colour.blue));
    }

    @Test
    void shouldProduceAndConsumeExplicitPolymorphicExplicitNaming() {
        tester.testProduceConsume(
                EXPLICIT_POLY_TOPIC,
                new TypeWithExplicitPolymorphism(
                        new TypeWithExplicitPolymorphism.ExplicitlyNamed("something")),
                new TypeWithExplicitPolymorphism(
                        new TypeWithExplicitPolymorphism.ImplicitlyNamed(12)));
    }

    @Test
    void shouldProduceAndConsumeImplicitPolymorphic() {
        tester.testProduceConsume(
                IMPLICIT_POLY_TOPIC,
                new TypeWithImplicitPolymorphism(
                        new TypeWithImplicitPolymorphism.ExplicitlyNamed("something")),
                new TypeWithImplicitPolymorphism(
                        new TypeWithImplicitPolymorphism.ImplicitlyNamed(12)));
    }

    private int numSchemaVersions(final KafkaTopicDescriptor.PartDescriptor<?> part)
            throws Exception {
        final String subject = part.topic().name() + "." + part.name();
        final CachedSchemaRegistryClient client = srClients.get(schemaRegistryName(part));
        return client.getAllVersions(subject).size();
    }

    private static String schemaRegistryName(final KafkaTopicDescriptor.PartDescriptor<?> part) {
        return part.resources()
                .filter(JsonSchemaDescriptor.class::isInstance)
                .map(JsonSchemaDescriptor.class::cast)
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Part is not associated with a JSON schema. Part: "
                                                + part
                                                + "("
                                                + codeLocation(part)
                                                + ")"))
                .schemaRegistryName();
    }
}
