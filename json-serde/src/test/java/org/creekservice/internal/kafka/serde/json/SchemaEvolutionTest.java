/*
 * Copyright 2023-2025 Creek Contributors (https://github.com/creek-service)
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

import static org.creekservice.internal.kafka.serde.json.util.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.creekservice.api.kafka.metadata.schema.SchemaDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaStoreEndpoints;
import org.creekservice.api.kafka.serde.test.KafkaSerdeProviderFunctionalFixture;
import org.creekservice.api.kafka.serde.test.KafkaSerdeProviderFunctionalFixture.Tester;
import org.creekservice.internal.kafka.serde.json.model.TestKeyAddingMandatory;
import org.creekservice.internal.kafka.serde.json.model.TestKeyReAddNewIdDifferentType;
import org.creekservice.internal.kafka.serde.json.model.TestKeyReAddNewIdSameType;
import org.creekservice.internal.kafka.serde.json.model.TestKeyRemovingMandatory;
import org.creekservice.internal.kafka.serde.json.model.TestKeyV0;
import org.creekservice.internal.kafka.serde.json.model.TestKeyV1;
import org.creekservice.internal.kafka.serde.json.model.TestValueV0;
import org.creekservice.internal.kafka.serde.json.util.TopicDescriptors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
@Testcontainers
@Tag("ContainerisedTest")
public class SchemaEvolutionTest {

    private static final String CLUSTER_NAME = KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
    private static final String SCHEMA_REGISTRY_NAME =
            SchemaDescriptor.DEFAULT_SCHEMA_REGISTRY_NAME;

    private static final OwnedKafkaTopicInput<TestKeyV0, TestValueV0> TEST_TOPIC_1 =
            topicWithKeyType(TestKeyV0.class);

    private static final KafkaSerdeProviderFunctionalFixture TEST_FIXTURE =
            KafkaSerdeProviderFunctionalFixture.tester(List.of(TEST_TOPIC_1));

    private static final TestValueV0 VALUE = new TestValueV0(Optional.empty(), 22);

    @Container
    private static final SchemaRegistryContainer SCHEMA_REGISTRY =
            new SchemaRegistryContainer(
                            DockerImageName.parse("confluentinc/cp-schema-registry:7.3.1"),
                            TEST_FIXTURE.kafkaContainer(CLUSTER_NAME))
                    .withStartupAttempts(3)
                    .withStartupTimeout(Duration.ofSeconds(90));

    private static Tester tester;

    @BeforeAll
    static void classSetUp() {
        tester =
                TEST_FIXTURE
                        .withExtensionOption(
                                JsonSerdeExtensionOptions.builder()
                                        .withTypeOverride(
                                                SchemaStoreEndpoints.Loader.class,
                                                clusterName -> SCHEMA_REGISTRY.clientEndpoint())
                                        .build())
                        .start();
    }

    @AfterAll
    static void classTearDown() {
        TEST_FIXTURE.stop();
    }

    @Test
    void shouldNotBeAbleToAddMandatoryProperty() {
        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> tester.testEvolution(topicWithKeyType(TestKeyAddingMandatory.class)));

        // Then:
        assertThat(e.getClass().getSimpleName(), is("SchemaStoreException"));
        assertThat(e.getCause().getClass().getSimpleName(), is("IncompatibleSchemaException"));
    }

    @Test
    void shouldNotBeAbleToRemoveMandatoryProperty() {
        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                tester.testEvolution(
                                        topicWithKeyType(TestKeyRemovingMandatory.class)));

        // Then:
        assertThat(e.getClass().getSimpleName(), is("SchemaStoreException"));
        assertThat(e.getCause().getClass().getSimpleName(), is("IncompatibleSchemaException"));
    }

    @Test
    void shouldBeAbleToAddAndRemoveOptionalProperties() {
        // Given:
        tester.produce(TEST_TOPIC_1, new TestKeyV0(10, OptionalInt.of(11)), VALUE);
        final OwnedKafkaTopicInput<TestKeyV1, TestValueV0> evolvedTopic =
                topicWithKeyType(TestKeyV1.class);

        // When:
        try (Tester t = tester.testEvolution(evolvedTopic)) {

            // Then: did not throw.
            t.consume(evolvedTopic, new TestKeyV1(10, Optional.empty()), VALUE);
            tester.consume(TEST_TOPIC_1, new TestKeyV0(10, OptionalInt.of(11)), VALUE);

            // When:
            t.produce(evolvedTopic, new TestKeyV1(12, Optional.of("bob")), VALUE);

            // Then:
            t.consume(evolvedTopic, new TestKeyV1(12, Optional.of("bob")), VALUE);
            tester.consume(TEST_TOPIC_1, new TestKeyV0(12, OptionalInt.empty()), VALUE);
        }
    }

    @Test
    void shouldNotBeAbleToReAddAnOptionalPropertyWithADifferentType() {
        // Given:
        try (Tester t = tester.testEvolution(topicWithKeyType(TestKeyV1.class))) {

            // When:
            final Exception e =
                    assertThrows(
                            RuntimeException.class,
                            () ->
                                    t.testEvolution(
                                            topicWithKeyType(
                                                    TestKeyReAddNewIdDifferentType.class)));

            // Then:
            assertThat(e.getClass().getSimpleName(), is("SchemaStoreException"));
            assertThat(e.getCause().getClass().getSimpleName(), is("IncompatibleSchemaException"));
        }
    }

    @Test
    void shouldBeAbleToReAddAnOptionalPropertyWithSameType() {
        // Given:
        try (Tester t = tester.testEvolution(topicWithKeyType(TestKeyV1.class))) {

            // When:
            final Tester result =
                    t.testEvolution(topicWithKeyType(TestKeyReAddNewIdSameType.class));

            // Then: did not throw
            result.close();
        }
    }

    private static <K> OwnedKafkaTopicInput<K, TestValueV0> topicWithKeyType(
            final Class<K> keyType) {
        return TopicDescriptors.inputTopic(
                CLUSTER_NAME,
                SCHEMA_REGISTRY_NAME,
                "Bob",
                keyType,
                TestValueV0.class,
                withPartitions(1));
    }
}
