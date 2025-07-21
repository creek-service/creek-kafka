/*
 * Copyright 2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.schema.validation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.util.Map;
import net.jimblackler.jsonschemafriend.Schema;
import net.jimblackler.jsonschemafriend.StandardValidationException;
import net.jimblackler.jsonschemafriend.ValidationException;
import net.jimblackler.jsonschemafriend.Validator;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor.Part;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.YamlSchema;
import org.creekservice.internal.kafka.serde.json.schema.SchemaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaFriendValidatorTest {

    private static final YamlSchema SCHEMA = ProducerSchema.fromJson("true");
    @Mock private Validator underlying;
    private SchemaFriendValidator validator;

    @BeforeEach
    void setUp() {
        validator = new SchemaFriendValidator(SCHEMA, underlying);
    }

    @Test
    void shouldThrowOnValidationFailures() throws Exception {
        // Given:
        final ValidationException exception = new StandardValidationException(Map.of());
        doThrow(exception).when(underlying).validate(any(), anyMap());

        // When:
        final Exception e =
                assertThrows(
                        SchemaException.class, () -> validator.validate(Map.of(), "t", Part.value));

        // Then:
        assertThat(e.getMessage(), is("Validation failed. topic: t, part: value"));
        assertThat(e.getCause(), is(sameInstance(exception)));
    }

    @Test
    void shouldNotThrowIfValidationPasses() throws Exception {
        // Given:
        final Map<String, Object> properties = Map.of();

        // When:
        validator.validate(properties, "t", Part.value);

        // Then: did not throw.
        verify(underlying).validate(any(Schema.class), eq(properties));
    }
}
