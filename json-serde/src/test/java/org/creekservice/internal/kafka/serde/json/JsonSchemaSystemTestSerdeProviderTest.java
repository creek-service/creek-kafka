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

package org.creekservice.internal.kafka.serde.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.creekservice.api.kafka.metadata.serde.JsonSchemaKafkaSerde;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
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
class JsonSchemaSystemTestSerdeProviderTest {

    @Mock private PartDescriptor<?> part;

    private JsonSchemaSystemTestSerdeProvider provider;
    private SystemTestSerde serde;

    @BeforeEach
    void setUp() {
        provider = new JsonSchemaSystemTestSerdeProvider();
        serde = provider.create();
    }

    @Test
    void shouldReturnJsonSchemaFormat() {
        assertThat(provider.format(), is(JsonSchemaKafkaSerde.format()));
    }

    @Test
    void shouldHandleString() {
        assertRoundTrip("hello");
    }

    @Test
    void shouldHandleBooleans() {
        assertRoundTrip(Boolean.TRUE);
        assertRoundTrip(Boolean.FALSE);
        assertRoundTrip(false);
        assertRoundTrip(true);
    }

    @Test
    void shouldHandleIntegerTypes() {
        assertRoundTrip(42);
        assertRoundTrip(Integer.MAX_VALUE + 1L);
    }

    @Test
    void shouldHandleDecimals() {
        assertRoundTrip(new BigDecimal("42.00"));
    }

    @Test
    void shouldHandleMaps() {
        assertRoundTrip(Map.of("name", "alice", "age", 30));
    }

    @Test
    void shouldHandleLists() {
        assertRoundTrip(List.of("a", "b", "c"));
    }

    @Test
    void shouldHandleNulls() {
        assertRoundTrip(null);
    }

    @Test
    void shouldNormaliseFloatingPointToDecimal() {
        // Given:
        final Object data = 189.4f;

        // When:
        final Object result = serde.normalise(data, part, "topic");

        // Then:
        assertThat(result, is(new BigDecimal("189.4")));
    }

    @Test
    void shouldThrowOnSerializationError() {
        // Given: a self-referencing object that can't be serialized
        final Object[] selfRef = new Object[1];
        selfRef[0] = selfRef;

        // When:
        final RuntimeException e =
                assertThrows(RuntimeException.class, () -> serde.serialize(selfRef, part, "topic"));

        // Then:
        assertThat(e.getMessage(), containsString("Failed to serialize"));
    }

    @Test
    void shouldThrowOnDeserializationError() {
        // Given:
        final byte[] invalidJson = "not-valid-json".getBytes(StandardCharsets.UTF_8);

        // When:
        final RuntimeException e =
                assertThrows(
                        RuntimeException.class,
                        () -> serde.deserialize(invalidJson, part, "topic"));

        // Then:
        assertThat(e.getMessage(), containsString("Failed to deserialize"));
    }

    private void assertRoundTrip(final Object data) {
        // When:
        final byte[] bytes = serde.serialize(data, part, "topic");
        final Object result = serde.deserialize(bytes, part, "topic");

        // Then:
        assertThat(result, is(data));
    }
}
