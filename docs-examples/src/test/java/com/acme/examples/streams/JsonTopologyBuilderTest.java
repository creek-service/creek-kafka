/*
 * Copyright 2023-2026 Creek Contributors (https://github.com/creek-service)
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

package com.acme.examples.streams;

import com.acme.examples.service.MyServiceDescriptor;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.api.kafka.serde.schema.store.endpoint.MockEndpointsLoader;
import org.creekservice.api.kafka.serde.schema.store.endpoint.SchemaStoreEndpoints;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;

import java.util.List;

import static com.acme.examples.service.MyServiceDescriptor.InputTopic;
import static com.acme.examples.service.MyServiceDescriptor.OutputTopic;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

// begin-snippet: class-setup
class JsonTopologyBuilderTest {

    private static CreekContext ctx;

    @BeforeAll
    public static void classSetup() {
        ctx = CreekServices.builder(new MyServiceDescriptor())
                // Configure Creek to work without an actual Kafka cluster or Schema Registry:
                .with(KafkaStreamsExtensionOptions.testBuilder().build())
                .with(JsonSerdeExtensionOptions.builder()
                    // Install custom client:
                    .withTypeOverride(
                            JsonSchemaStoreClient.Factory.class,
                            (schemaRegistryName, endpoints) ->
                                    new CustomSchemaClient(
                                            schemaRegistryName,
                                            new MockSchemaRegistryClient(
                                                    List.of(new JsonSchemaProvider()))))
                    // Install custom endpoint loader:
                    .withTypeOverride(SchemaStoreEndpoints.Loader.class, new MockEndpointsLoader() {})
                    .build())
                .build();
    }
// end-snippet

    private TopologyTestDriver testDriver;
    private TestInputTopic<Long, String> inputTopic;
    private TestOutputTopic<Long, String> outputTopic;
    private KafkaStreamsExtension ext;

    @AfterAll
    static void afterAll() {
        ctx.close();
    }

    @BeforeEach
    public void setUp() {
        ext = ctx.extension(KafkaStreamsExtension.class);

        // Build topology using the extension:
        final Topology topology = new TopologyBuilder(ext).build();

        testDriver = new TopologyTestDriver(topology, ext.properties(DEFAULT_CLUSTER_NAME));

        // Use `TestTopics` to build topics:
        inputTopic = TestTopics.inputTopic(InputTopic, ext, testDriver);
        outputTopic = TestTopics.outputTopic(OutputTopic, ext, testDriver);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
        ext.close();
    }

    @Test
    void shouldTestSomething() {
        // When:
        inputTopic.pipeInput(1L, "a");

        // Then:
        assertThat(outputTopic.readKeyValuesToList(), contains(pair(1L, "a")));
    }
}
