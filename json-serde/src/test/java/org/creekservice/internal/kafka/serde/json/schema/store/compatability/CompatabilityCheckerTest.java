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

package org.creekservice.internal.kafka.serde.json.schema.store.compatability;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.List;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient;
import org.creekservice.api.kafka.serde.json.schema.store.client.JsonSchemaStoreClient.VersionedSchema;
import org.creekservice.internal.kafka.serde.json.model.IncompatibleValue;
import org.creekservice.internal.kafka.serde.json.model.TestKeyAddingMandatory;
import org.creekservice.internal.kafka.serde.json.model.TestKeyReAddNewIdDifferentType;
import org.creekservice.internal.kafka.serde.json.model.TestKeyReAddNewIdSameType;
import org.creekservice.internal.kafka.serde.json.model.TestKeyRemovingMandatory;
import org.creekservice.internal.kafka.serde.json.model.TestKeyV0;
import org.creekservice.internal.kafka.serde.json.model.TestKeyV1;
import org.creekservice.internal.kafka.serde.json.model.TestKeyV2;
import org.creekservice.internal.kafka.serde.json.model.TestValueV0;
import org.creekservice.internal.kafka.serde.json.model.TestValueV1;
import org.creekservice.internal.kafka.serde.json.model.TestValueV2;
import org.creekservice.internal.kafka.serde.json.schema.LocalSchemaLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CompatabilityCheckerTest {

    private static final String SUBJECT = "test-subject";

    @Mock private JsonSchemaStoreClient client;
    private CompatabilityChecker checker;

    @BeforeEach
    void setUp() {
        checker = new CompatabilityChecker(client);
    }

    @Nested
    final class FirstVersion {

        @Test
        void shouldAllowFirstSchemaWithNoExistingVersions() {
            // Given:
            final ProducerSchema schema = loadSchema(TestKeyV0.class);
            when(client.allVersions(SUBJECT)).thenReturn(List.of());

            // When/Then:
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }
    }

    @Nested
    final class KeyPropertyEvolution {

        @Test
        void shouldAllowAddingOptionalProperty() {
            // Given:
            final ProducerSchema schema = loadSchema(TestKeyV1.class);
            givenExistingVersions(loadSchema(TestKeyV0.class));

            // When/Then: V1 adds optional 'text', removes optional 'newId'
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldNotAllowAddingMandatoryProperty() {
            // Given:
            final ProducerSchema schema = loadSchema(TestKeyAddingMandatory.class);
            givenExistingVersions(loadSchema(TestKeyV0.class));

            // When:
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldNotAllowRemovingMandatoryProperty() {
            // Given:
            final ProducerSchema schema = loadSchema(TestKeyRemovingMandatory.class);
            givenExistingVersions(loadSchema(TestKeyV0.class));

            // When:
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldNotAllowReAddingOptionalPropertyWithDifferentType() {
            // Given: V0 has 'newId' as OptionalInt, V1 removes it
            final ProducerSchema schema = loadSchema(TestKeyReAddNewIdDifferentType.class);
            givenExistingVersions(loadSchema(TestKeyV0.class));

            // When: re-add 'newId' as Optional<String>
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldAllowReAddingOptionalPropertyWithSameType() {
            // Given: V0 has 'newId' as OptionalInt, V1 removes it
            final ProducerSchema schema = loadSchema(TestKeyReAddNewIdSameType.class);
            givenExistingVersions(loadSchema(TestKeyV0.class));

            // When: re-add 'newId' as OptionalInt (same type)
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }
    }

    @Nested
    final class ValuePropertyEvolution {

        @Test
        void shouldAllowAddingOptionalValueProperty() {
            // Given: V0 has 'name' (optional String) and 'age' (required int)
            final ProducerSchema schema = loadSchema(TestValueV1.class);
            givenExistingVersions(loadSchema(TestValueV0.class));

            // When: V1 removes optional 'name', adds optional 'address'
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldNotAllowChangingValuePropertyType() {
            // Given:
            final ProducerSchema schema = loadSchema(IncompatibleValue.class);
            givenExistingVersions(loadSchema(TestValueV0.class));

            // When: 'name' changes from String to Long
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }
    }

    @Nested
    final class MultiVersionChain {

        @Test
        void shouldAllowV2CompatibleWithBothV0AndV1ForKeys() {
            // Given: V0 and V1 already registered
            final ProducerSchema schema = loadSchema(TestKeyV2.class);
            givenExistingVersions(loadSchema(TestKeyV0.class), loadSchema(TestKeyV1.class));

            // When: V2 removes 'text' (from V1), adds optional 'address'
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldAllowV2CompatibleWithBothV0AndV1ForValues() {
            // Given: V0 and V1 already registered
            final ProducerSchema schema = loadSchema(TestValueV2.class);
            givenExistingVersions(loadSchema(TestValueV0.class), loadSchema(TestValueV1.class));

            // When: V2 removes 'address' (from V1), re-adds 'name' (from V0), adds 'v'
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldDetectIncompatibilityWithOlderVersionInChain() {
            // Given: V0 has required 'id' and optional 'newId',
            //        V1 has required 'id' and optional 'text' (removed 'newId')
            //        New schema removes required 'id' — compatible with neither V0 nor V1
            final ProducerSchema schema = loadSchema(TestKeyRemovingMandatory.class);
            givenExistingVersions(loadSchema(TestKeyV0.class), loadSchema(TestKeyV1.class));

            // When:
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldDetectIncompatibilityWithOlderVersionEvenWhenNewerVersionIsCompatible() {
            // Given: V0 has required 'id' + optional 'newId' (int)
            //        V1 has required 'id' + optional 'text' (no 'newId')
            //        New schema re-adds 'newId' as string — type clash with V0, but V1 is fine.
            final ProducerSchema schema = loadSchema(TestKeyReAddNewIdDifferentType.class);
            givenExistingVersions(loadSchema(TestKeyV0.class), loadSchema(TestKeyV1.class));

            // When:
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
            assertThat(e.getMessage(), containsString("version: 1"));
        }
    }

    @Nested
    final class EnumEvolution {

        private static final String ENUM_BASE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  colour:
                    type: string
                    enum:
                    - red
                    - blue
                    - yellow
                required:
                - colour
                additionalProperties: false
                """;

        private static final String ENUM_VALUE_ADDED =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  colour:
                    type: string
                    enum:
                    - red
                    - blue
                    - yellow
                    - green
                required:
                - colour
                additionalProperties: false
                """;

        private static final String ENUM_VALUE_REMOVED =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  colour:
                    type: string
                    enum:
                    - red
                    - blue
                required:
                - colour
                additionalProperties: false
                """;

        @Test
        void shouldNotAllowAddingEnumValue() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(ENUM_VALUE_ADDED);
            givenExistingVersions(ProducerSchema.fromYaml(ENUM_BASE));

            // When: add 'green' to enum
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldNotAllowRemovingEnumValue() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(ENUM_VALUE_REMOVED);
            givenExistingVersions(ProducerSchema.fromYaml(ENUM_BASE));

            // When: remove 'yellow' from enum
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
            assertThat(
                    e.getMessage(),
                    containsString("A type at path '#/properties/colour' is different"));
        }

        @Test
        void shouldAllowSameEnumValues() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(ENUM_BASE);
            givenExistingVersions(ProducerSchema.fromYaml(ENUM_BASE));

            // When/Then: same enum values = compatible
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }
    }

    @Nested
    final class NestedObjectEvolution {

        private static final String NESTED_V0 =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  name:
                    type: string
                  address:
                    type: object
                    properties:
                      street:
                        type: string
                      city:
                        type: string
                    required:
                    - street
                    - city
                    additionalProperties: false
                required:
                - name
                additionalProperties: false
                """;

        private static final String NESTED_ADD_OPTIONAL =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  name:
                    type: string
                  address:
                    type: object
                    properties:
                      street:
                        type: string
                      city:
                        type: string
                      zip:
                        type: string
                    required:
                    - street
                    - city
                    additionalProperties: false
                required:
                - name
                additionalProperties: false
                """;

        private static final String NESTED_ADD_REQUIRED =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  name:
                    type: string
                  address:
                    type: object
                    properties:
                      street:
                        type: string
                      city:
                        type: string
                      zip:
                        type: string
                    required:
                    - street
                    - city
                    - zip
                    additionalProperties: false
                required:
                - name
                additionalProperties: false
                """;

        private static final String NESTED_CHANGE_TYPE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  name:
                    type: string
                  address:
                    type: object
                    properties:
                      street:
                        type: integer
                      city:
                        type: string
                    required:
                    - street
                    - city
                    additionalProperties: false
                required:
                - name
                additionalProperties: false
                """;

        @Test
        void shouldAllowAddingOptionalPropertyInNestedObject() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(NESTED_ADD_OPTIONAL);
            givenExistingVersions(ProducerSchema.fromYaml(NESTED_V0));

            // When/Then: add optional 'zip' to nested address
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldNotAllowAddingRequiredPropertyInNestedObject() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(NESTED_ADD_REQUIRED);
            givenExistingVersions(ProducerSchema.fromYaml(NESTED_V0));

            // When: add required 'zip' to nested address
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldNotAllowChangingTypeInNestedObject() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(NESTED_CHANGE_TYPE);
            givenExistingVersions(ProducerSchema.fromYaml(NESTED_V0));

            // When: change 'street' from string to integer in nested address
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }
    }

    @Nested
    final class PolymorphicTypeEvolution {

        static final String POLY_BASE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  TypeA:
                    type: object
                    properties:
                      text:
                        type: string
                      '@type':
                        const: type-a
                    required:
                    - '@type'
                    additionalProperties: false
                  TypeB:
                    type: object
                    properties:
                      age:
                        type: integer
                      '@type':
                        const: type-b
                    required:
                    - age
                    - '@type'
                    additionalProperties: false
                type: object
                properties:
                  inner:
                    anyOf:
                    - $ref: "#/$defs/TypeA"
                    - $ref: "#/$defs/TypeB"
                additionalProperties: false
                """;

        private static final String POLY_SUBTYPE_ADDED =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  TypeA:
                    type: object
                    properties:
                      text:
                        type: string
                      '@type':
                        const: type-a
                    required:
                    - '@type'
                    additionalProperties: false
                  TypeB:
                    type: object
                    properties:
                      age:
                        type: integer
                      '@type':
                        const: type-b
                    required:
                    - age
                    - '@type'
                    additionalProperties: false
                  TypeC:
                    type: object
                    properties:
                      flag:
                        type: boolean
                      '@type':
                        const: type-c
                    required:
                    - flag
                    - '@type'
                    additionalProperties: false
                type: object
                properties:
                  inner:
                    anyOf:
                    - $ref: "#/$defs/TypeA"
                    - $ref: "#/$defs/TypeB"
                    - $ref: "#/$defs/TypeC"
                additionalProperties: false
                """;

        private static final String POLY_SUBTYPE_REMOVED =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  TypeA:
                    type: object
                    properties:
                      text:
                        type: string
                      '@type':
                        const: type-a
                    required:
                    - '@type'
                    additionalProperties: false
                type: object
                properties:
                  inner:
                    anyOf:
                    - $ref: "#/$defs/TypeA"
                additionalProperties: false
                """;

        private static final String POLY_ADD_OPTIONAL_PROP_TO_SUBTYPE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  TypeA:
                    type: object
                    properties:
                      text:
                        type: string
                      nickname:
                        type: string
                      '@type':
                        const: type-a
                    required:
                    - '@type'
                    additionalProperties: false
                  TypeB:
                    type: object
                    properties:
                      age:
                        type: integer
                      '@type':
                        const: type-b
                    required:
                    - age
                    - '@type'
                    additionalProperties: false
                type: object
                properties:
                  inner:
                    anyOf:
                    - $ref: "#/$defs/TypeA"
                    - $ref: "#/$defs/TypeB"
                additionalProperties: false
                """;

        private static final String POLY_ADD_REQUIRED_PROP_TO_SUBTYPE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  TypeA:
                    type: object
                    properties:
                      text:
                        type: string
                      nickname:
                        type: string
                      '@type':
                        const: type-a
                    required:
                    - '@type'
                    - nickname
                    additionalProperties: false
                  TypeB:
                    type: object
                    properties:
                      age:
                        type: integer
                      '@type':
                        const: type-b
                    required:
                    - age
                    - '@type'
                    additionalProperties: false
                type: object
                properties:
                  inner:
                    anyOf:
                    - $ref: "#/$defs/TypeA"
                    - $ref: "#/$defs/TypeB"
                additionalProperties: false
                """;

        private static final String POLY_REMOVE_OPTIONAL_PROP_FROM_SUBTYPE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  TypeA:
                    type: object
                    properties:
                      '@type':
                        const: type-a
                    required:
                    - '@type'
                    additionalProperties: false
                  TypeB:
                    type: object
                    properties:
                      age:
                        type: integer
                      '@type':
                        const: type-b
                    required:
                    - age
                    - '@type'
                    additionalProperties: false
                type: object
                properties:
                  inner:
                    anyOf:
                    - $ref: "#/$defs/TypeA"
                    - $ref: "#/$defs/TypeB"
                additionalProperties: false
                """;

        private static final String POLY_REMOVE_REQUIRED_PROP_FROM_SUBTYPE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  TypeA:
                    type: object
                    properties:
                      text:
                        type: string
                      '@type':
                        const: type-a
                    required:
                    - '@type'
                    additionalProperties: false
                  TypeB:
                    type: object
                    properties:
                      '@type':
                        const: type-b
                    required:
                    - '@type'
                    additionalProperties: false
                type: object
                properties:
                  inner:
                    anyOf:
                    - $ref: "#/$defs/TypeA"
                    - $ref: "#/$defs/TypeB"
                additionalProperties: false
                """;

        @Test
        void shouldNotAllowAddingSubtype() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(POLY_SUBTYPE_ADDED);
            givenExistingVersions(ProducerSchema.fromYaml(POLY_BASE));

            // When: add TypeC
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldNotAllowRemovingSubtype() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(POLY_SUBTYPE_REMOVED);
            givenExistingVersions(ProducerSchema.fromYaml(POLY_BASE));

            // When: remove TypeB
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldAllowAddingOptionalPropertyToSubtype() {
            // Given:
            final ProducerSchema schema =
                    ProducerSchema.fromYaml(POLY_ADD_OPTIONAL_PROP_TO_SUBTYPE);
            givenExistingVersions(ProducerSchema.fromYaml(POLY_BASE));

            // When/Then: add optional 'nickname' to TypeA
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldNotAllowAddingRequiredPropertyToSubtype() {
            // Given:
            final ProducerSchema schema =
                    ProducerSchema.fromYaml(POLY_ADD_REQUIRED_PROP_TO_SUBTYPE);
            givenExistingVersions(ProducerSchema.fromYaml(POLY_BASE));

            // When: add required 'nickname' to TypeA
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldAllowRemovingOptionalPropertyFromSubtype() {
            // Given:
            final ProducerSchema schema =
                    ProducerSchema.fromYaml(POLY_REMOVE_OPTIONAL_PROP_FROM_SUBTYPE);
            givenExistingVersions(ProducerSchema.fromYaml(POLY_BASE));

            // When/Then: remove optional 'text' from TypeA
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldNotAllowRemovingRequiredPropertyFromSubtype() {
            // Given:
            final ProducerSchema schema =
                    ProducerSchema.fromYaml(POLY_REMOVE_REQUIRED_PROP_FROM_SUBTYPE);
            givenExistingVersions(ProducerSchema.fromYaml(POLY_BASE));

            // When: remove required 'age' from TypeB
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }
    }

    @Nested
    final class ArraySchemaEvolution {

        private static final String ARRAY_STRING_ITEMS =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  tags:
                    type: array
                    items:
                      type: string
                required:
                - tags
                additionalProperties: false
                """;

        private static final String ARRAY_INTEGER_ITEMS =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  tags:
                    type: array
                    items:
                      type: integer
                required:
                - tags
                additionalProperties: false
                """;

        @Test
        void shouldNotAllowChangingArrayItemType() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(ARRAY_INTEGER_ITEMS);
            givenExistingVersions(ProducerSchema.fromYaml(ARRAY_STRING_ITEMS));

            // When: change items from string to integer
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldAllowSameArraySchema() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(ARRAY_STRING_ITEMS);
            givenExistingVersions(ProducerSchema.fromYaml(ARRAY_STRING_ITEMS));

            // When/Then:
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }
    }

    @Nested
    final class ConstraintChanges {

        private static final String WITH_MIN_0 =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  count:
                    type: integer
                    minimum: 0
                    maximum: 100
                required:
                - count
                additionalProperties: false
                """;

        private static final String WITH_MIN_10 =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  count:
                    type: integer
                    minimum: 10
                    maximum: 100
                required:
                - count
                additionalProperties: false
                """;

        private static final String WITH_MAX_50 =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  count:
                    type: integer
                    minimum: 0
                    maximum: 50
                required:
                - count
                additionalProperties: false
                """;

        private static final String WITH_WIDER_RANGE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                type: object
                properties:
                  count:
                    type: integer
                    minimum: -10
                    maximum: 200
                required:
                - count
                additionalProperties: false
                """;

        @Test
        void shouldNotAllowTighteningMinimumConstraint() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(WITH_MIN_10);
            givenExistingVersions(ProducerSchema.fromYaml(WITH_MIN_0));

            // When: raise minimum from 0 to 10
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldNotAllowTighteningMaximumConstraint() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(WITH_MAX_50);
            givenExistingVersions(ProducerSchema.fromYaml(WITH_MIN_0));

            // When: lower maximum from 100 to 50
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldNotAllowWideningRange() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(WITH_WIDER_RANGE);
            givenExistingVersions(ProducerSchema.fromYaml(WITH_MIN_0));

            // When: widen range to [-10, 200]
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldAllowSameConstraints() {
            // Given:
            final ProducerSchema schema = ProducerSchema.fromYaml(WITH_MIN_0);
            givenExistingVersions(ProducerSchema.fromYaml(WITH_MIN_0));

            // When/Then:
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }
    }

    @Nested
    final class Draft202012Features {

        @Test
        void shouldHandleSchemaWithDefs() {
            // Given: schema uses $defs (draft 2020-12 keyword)
            final ProducerSchema schema =
                    ProducerSchema.fromYaml(PolymorphicTypeEvolution.POLY_BASE);
            givenExistingVersions(ProducerSchema.fromYaml(PolymorphicTypeEvolution.POLY_BASE));

            // When/Then: same schema should be compatible
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldHandleGeneratedSchemaWithDefs() {
            // Given: TypeWithExplicitPolymorphism generates schema with $defs
            final ProducerSchema schema =
                    LocalSchemaLoader.loadFromClasspath(
                            org.creekservice.internal.kafka.serde.json.model
                                    .TypeWithExplicitPolymorphism.class);
            givenExistingVersions(schema);

            // When/Then: same schema compatible with itself
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldHandleGeneratedEnumSchema() {
            // Given: TypeWithEnum generates schema with enum constraint
            final ProducerSchema schema =
                    LocalSchemaLoader.loadFromClasspath(
                            org.creekservice.internal.kafka.serde.json.model.TypeWithEnum.class);
            givenExistingVersions(schema);

            // When/Then: same schema compatible with itself
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }
    }

    @Nested
    final class DraftVersionEvolution {

        private static final String DRAFT_07_BASE =
                """
                ---
                $schema: http://json-schema.org/draft-07/schema#
                definitions:
                  Address:
                    type: object
                    properties:
                      street:
                        type: string
                      city:
                        type: string
                    required:
                    - street
                    additionalProperties: false
                type: object
                properties:
                  id:
                    type: integer
                    minimum: 0
                  name:
                    type: string
                  address:
                    $ref: "#/definitions/Address"
                required:
                - id
                additionalProperties: false
                """;

        private static final String DRAFT_2020_12_COMPATIBLE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  Address:
                    type: object
                    properties:
                      street:
                        type: string
                      city:
                        type: string
                    required:
                    - street
                    additionalProperties: false
                type: object
                properties:
                  id:
                    type: integer
                    minimum: 0
                  name:
                    type: string
                  email:
                    type: string
                  address:
                    $ref: "#/$defs/Address"
                required:
                - id
                additionalProperties: false
                """;

        private static final String DRAFT_2020_12_INCOMPATIBLE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  Address:
                    type: object
                    properties:
                      street:
                        type: string
                      city:
                        type: string
                    required:
                    - street
                    additionalProperties: false
                type: object
                properties:
                  id:
                    type: string
                  name:
                    type: string
                  address:
                    $ref: "#/$defs/Address"
                required:
                - id
                additionalProperties: false
                """;

        private static final String DRAFT_2020_12_NESTED_COMPATIBLE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  Address:
                    type: object
                    properties:
                      street:
                        type: string
                      city:
                        type: string
                      zip:
                        type: string
                    required:
                    - street
                    additionalProperties: false
                type: object
                properties:
                  id:
                    type: integer
                    minimum: 0
                  name:
                    type: string
                  address:
                    $ref: "#/$defs/Address"
                required:
                - id
                additionalProperties: false
                """;

        private static final String DRAFT_2020_12_NESTED_INCOMPATIBLE =
                """
                ---
                $schema: https://json-schema.org/draft/2020-12/schema
                $defs:
                  Address:
                    type: object
                    properties:
                      street:
                        type: integer
                      city:
                        type: string
                    required:
                    - street
                    additionalProperties: false
                type: object
                properties:
                  id:
                    type: integer
                    minimum: 0
                  name:
                    type: string
                  address:
                    $ref: "#/$defs/Address"
                required:
                - id
                additionalProperties: false
                """;

        @Test
        void shouldAllowCompatibleEvolutionFromDraft07To202012() {
            // Given: existing schema is draft-07
            final ProducerSchema schema = ProducerSchema.fromYaml(DRAFT_2020_12_COMPATIBLE);
            givenExistingVersions(ProducerSchema.fromYaml(DRAFT_07_BASE));

            // When/Then: add optional 'email', same draft change
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldDetectIncompatibleEvolutionFromDraft07To202012() {
            // Given: existing schema is draft-07
            final ProducerSchema schema = ProducerSchema.fromYaml(DRAFT_2020_12_INCOMPATIBLE);
            givenExistingVersions(ProducerSchema.fromYaml(DRAFT_07_BASE));

            // When: change 'id' type from integer to string
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }

        @Test
        void shouldAllowCompatibleNestedTypeEvolutionFromDraft07To202012() {
            // Given: existing schema is draft-07 with Address definition
            final ProducerSchema schema = ProducerSchema.fromYaml(DRAFT_2020_12_NESTED_COMPATIBLE);
            givenExistingVersions(ProducerSchema.fromYaml(DRAFT_07_BASE));

            // When/Then: add optional 'zip' to nested Address, definitions→$defs
            assertDoesNotThrow(() -> checker.checkCompatability(SUBJECT, schema));
        }

        @Test
        void shouldDetectIncompatibleNestedTypeEvolutionFromDraft07To202012() {
            // Given: existing schema is draft-07 with Address definition
            final ProducerSchema schema =
                    ProducerSchema.fromYaml(DRAFT_2020_12_NESTED_INCOMPATIBLE);
            givenExistingVersions(ProducerSchema.fromYaml(DRAFT_07_BASE));

            // When: change 'street' type from string to integer in nested Address
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("not compatible"));
        }
    }

    @Nested
    final class ErrorMessages {

        @Test
        void shouldIncludeSubjectInErrorMessage() {
            // Given:
            final ProducerSchema schema = loadSchema(TestKeyAddingMandatory.class);
            givenExistingVersions(loadSchema(TestKeyV0.class));

            // When:
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("subject: " + SUBJECT));
        }

        @Test
        void shouldIncludeVersionInErrorMessage() {
            // Given:
            final ProducerSchema schema = loadSchema(TestKeyAddingMandatory.class);
            givenExistingVersions(loadSchema(TestKeyV0.class));

            // When:
            final RuntimeException e =
                    assertThrows(
                            RuntimeException.class,
                            () -> checker.checkCompatability(SUBJECT, schema));

            // Then:
            assertThat(e.getMessage(), containsString("version: 1"));
        }
    }

    private ProducerSchema loadSchema(final Class<?> type) {
        return LocalSchemaLoader.loadFromClasspath(type);
    }

    private void givenExistingVersions(final ProducerSchema... schemas) {
        final List<VersionedSchema> versions =
                java.util.stream.IntStream.range(0, schemas.length)
                        .mapToObj(i -> versioned(i + 1, schemas[i]))
                        .toList();
        when(client.allVersions(SUBJECT)).thenReturn(versions);
    }

    private static VersionedSchema versioned(final int version, final ProducerSchema schema) {
        return new VersionedSchema() {
            @Override
            public int version() {
                return version;
            }

            @Override
            public ProducerSchema schema() {
                return schema;
            }
        };
    }
}
