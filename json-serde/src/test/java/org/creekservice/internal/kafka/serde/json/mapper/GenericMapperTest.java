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

package org.creekservice.internal.kafka.serde.json.mapper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenericMapperTest {
    @Mock private JsonMapper underlying;
    private GenericMapper<String> mapper;

    @BeforeEach
    void setUp() {
        mapper = new GenericMapper<>(String.class, underlying);
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldConvertToMap() {
        // Given:
        when(underlying.convertValue(eq("text"), any(TypeReference.class)))
                .thenAnswer(inv -> Map.of("data", inv.getArgument(0)));

        // When:
        final Map<String, ?> result = mapper.convertToMap("text");

        // Then:
        assertThat(result, is(Map.of("data", "text")));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldThrowOnFailureToConvertToMap() {
        // Given:
        final RuntimeException exception = new RuntimeException("boom");
        when(underlying.convertValue(any(), any(TypeReference.class))).thenThrow(exception);

        // When:
        final Exception e = assertThrows(RuntimeException.class, () -> mapper.convertToMap("text"));

        // Then:
        assertThat(e.getMessage(), is("Error serializing type: java.lang.String"));
        assertThat(e.getCause(), is(sameInstance(exception)));
    }

    @Test
    void shouldWriteAsBytes() throws Exception {
        // Given:
        final Map<String, String> properties = Map.of("data", "text");
        when(underlying.writeValueAsBytes(properties)).thenReturn("text".getBytes(UTF_8));

        // When:
        final byte[] result = mapper.writeAsBytes(properties);

        // Then:
        assertThat(result, is("text".getBytes(UTF_8)));
    }

    @Test
    void shouldThrowOnFailureToWriteAsBytes() throws Exception {
        // Given:
        final RuntimeException exception = new RuntimeException("boom");
        when(underlying.writeValueAsBytes(any())).thenThrow(exception);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> mapper.writeAsBytes(Map.of("data", "text")));

        // Then:
        assertThat(e.getMessage(), is("Error serializing type: java.lang.String"));
        assertThat(e.getCause(), is(sameInstance(exception)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldReadValue() throws Exception {
        // Given:
        final byte[] data = "text".getBytes(UTF_8);
        when(underlying.readValue(eq(data), any(TypeReference.class)))
                .thenAnswer(inv -> Map.of("data", new String(inv.getArgument(0), UTF_8)));

        // When:
        final Map<String, Object> result = mapper.readValue(data);

        // Then:
        assertThat(result, is(Map.of("data", "text")));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldThrowOnFailureToReadValue() throws Exception {
        // Given:
        final RuntimeException exception = new RuntimeException("boom");
        when(underlying.readValue(any(byte[].class), any(TypeReference.class)))
                .thenThrow(exception);

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class, () -> mapper.readValue("text".getBytes(UTF_8)));

        // Then:
        assertThat(e.getMessage(), is("Error deserializing type: java.lang.String"));
        assertThat(e.getCause(), is(sameInstance(exception)));
    }

    @Test
    void shouldConvertFromMap() {
        // Given:
        final Map<String, String> properties = Map.of("data", "text");
        when(underlying.convertValue(properties, String.class))
                .thenAnswer(inv -> inv.<Map<String, ?>>getArgument(0).get("data"));

        // When:
        final String result = mapper.convertFromMap(properties);

        // Then:
        assertThat(result, is("text"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldThrowOnFailureToConvertFromMap() {
        // Given:
        final RuntimeException exception = new RuntimeException("boom");
        when(underlying.convertValue(anyMap(), any(Class.class))).thenThrow(exception);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> mapper.convertFromMap(Map.of()));

        // Then:
        assertThat(e.getMessage(), is("Error deserializing type: java.lang.String"));
        assertThat(e.getCause(), is(sameInstance(exception)));
    }
}
