/*
 * Copyright 2024-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.schema.serde;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.internal.kafka.serde.json.mapper.JsonWriter;
import org.creekservice.internal.kafka.serde.json.schema.validation.SchemaValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JsonSchemaSerializerTest {

    private static final String TOPIC = "the-topic";
    private static final String DATA = "data";
    private static final Map<String, Object> PROPS = Map.of("some", "props");

    @Mock private SchemaValidator validator;
    @Mock private JsonWriter<String> mapper;
    private JsonSchemaSerializer<String> serializer;

    @SuppressWarnings({"unchecked", "rawtypes"})
    @BeforeEach
    void setUp() {
        serializer = new JsonSchemaSerializer<>(validator, mapper);

        when(mapper.convertToMap(DATA)).thenReturn((Map) PROPS);
    }

    @Test
    void shouldThrowOnMapperIssue() {
        // Given:
        final RuntimeException exception = new RuntimeException();
        when(mapper.convertToMap(any())).thenThrow(exception);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> serializer.serialize(TOPIC, DATA));

        // Then:
        assertThat(e, is(sameInstance(exception)));
    }

    @Test
    void shouldThrowOnValidationIssue() {
        // Given:
        final RuntimeException exception = new RuntimeException();
        doThrow(exception).when(validator).validate(any(), any(), any());

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> serializer.serialize(TOPIC, DATA));

        // Then:
        assertThat(e, is(sameInstance(exception)));
    }

    @Test
    void shouldValidateKey() {
        // Given:
        serializer.configure(null, true);

        // When:
        serializer.serialize(TOPIC, DATA);

        // Then:
        verify(validator).validate(PROPS, TOPIC, Part.key);
    }

    @Test
    void shouldValidateValue() {
        // Given:
        serializer.configure(null, false);

        // When:
        serializer.serialize(TOPIC, DATA);

        // Then:
        verify(validator).validate(PROPS, TOPIC, Part.value);
    }

    @Test
    void shouldDeserialize() {
        // Given:
        final byte[] expected = "expected".getBytes(StandardCharsets.UTF_8);
        when(mapper.writeAsBytes(PROPS)).thenReturn(expected);

        // When:
        final byte[] actual = serializer.serialize(TOPIC, DATA);

        // Then:
        assertThat(expected, is(actual));
    }
}
