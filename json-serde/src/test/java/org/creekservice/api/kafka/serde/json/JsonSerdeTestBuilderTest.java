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

package org.creekservice.api.kafka.serde.json;

import static org.creekservice.internal.kafka.serde.json.util.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.api.kafka.serde.schema.store.endpoint.SchemaStoreEndpoints;
import org.creekservice.internal.kafka.serde.json.model.TestValueV0;
import org.creekservice.internal.kafka.serde.json.schema.serde.JsonSchemaSerdeFactory;
import org.creekservice.internal.kafka.serde.json.schema.store.SchemaStore;
import org.creekservice.internal.kafka.serde.json.schema.store.SrSchemaStores;
import org.creekservice.internal.kafka.serde.json.schema.validation.JsonSchemaValidationFailed;
import org.creekservice.internal.kafka.serde.json.util.TopicDescriptors;
import org.junit.jupiter.api.Test;

class JsonSerdeTestBuilderTest {

    private static final OwnedKafkaTopicOutput<String, TestValueV0> OWNED =
            TopicDescriptors.outputTopic(
                    "upstream-json", String.class, TestValueV0.class, withPartitions(1));

    @Test
    void shouldInitializeConsumerWithoutOwningServiceOrRegisteredSubject() {
        // The upstream owner has not registered any schemas in this isolated test.
        final SchemaStore store = store();
        final Serde<TestValueV0> serde =
                new JsonSchemaSerdeFactory(Map.of())
                        .create(store.loadFromClasspath(OWNED.toInput().value()));
        assertValidatesLocalSchema(serde);
    }

    @Test
    void shouldInitializeOwnedJsonTopic() {
        final SchemaStore store = store();
        store.registerFromClasspath(OWNED.value());
        final Serde<TestValueV0> serde =
                new JsonSchemaSerdeFactory(Map.of()).create(store.loadFromClasspath(OWNED.value()));
        assertValidatesLocalSchema(serde);
    }

    private static SchemaStore store() {
        final JsonSerdeExtensionOptions options = JsonSerdeExtensionOptions.testBuilder().build();
        return new SrSchemaStores(
                        options.typeOverride(SchemaStoreEndpoints.Loader.class).orElseThrow(),
                        options.typeOverride(JsonSchemaStoreClient.Factory.class).orElseThrow())
                .get("default");
    }

    private static void assertValidatesLocalSchema(final Serde<TestValueV0> serde) {
        final TestValueV0 valid = new TestValueV0(Optional.of("Jane"), 24);
        final byte[] bytes = serde.serializer().serialize(OWNED.name(), valid);
        assertThat(serde.deserializer().deserialize(OWNED.name(), bytes), is(valid));

        assertThrows(
                JsonSchemaValidationFailed.class,
                () ->
                        serde.serializer()
                                .serialize(OWNED.name(), new TestValueV0(Optional.of("Jane"), -1)));
        assertThrows(
                JsonSchemaValidationFailed.class,
                () ->
                        serde.deserializer()
                                .deserialize(
                                        OWNED.name(),
                                        "{\"age\":-1}".getBytes(StandardCharsets.UTF_8)));
    }
}
