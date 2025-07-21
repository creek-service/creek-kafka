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

package org.creekservice.internal.kafka.serde.json.schema.serde;

import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.internal.kafka.serde.json.model.DateTimeTypes;
import org.creekservice.internal.kafka.serde.json.model.TemporalTypes;
import org.creekservice.internal.kafka.serde.json.model.TypeWithExplicitPolymorphism;
import org.creekservice.internal.kafka.serde.json.model.TypeWithImplicitPolymorphism;
import org.creekservice.internal.kafka.serde.json.model.WithAmbiguousFloat;
import org.creekservice.internal.kafka.serde.json.model.WithEnum;
import org.creekservice.internal.kafka.serde.json.model.WithPrimitiveTypes;
import org.creekservice.internal.kafka.serde.json.schema.LocalSchemaLoader;
import org.creekservice.internal.kafka.serde.json.schema.store.RegisteredSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
class JsonSchemaSerdeFactoryTest {

    private JsonSchemaSerdeFactory serdeFactory;

    @BeforeEach
    void setUp() {
        serdeFactory = new JsonSchemaSerdeFactory(Map.of());
    }

    @Test
    void shouldHandleDateTimeTypes() {
        // When:
        final Serde<DateTimeTypes> serde =
                serdeFactory.create(registeredSchema(DateTimeTypes.class));
        serde.configure(Map.of(), false);

        // Then:
        final String json =
                assertRoundTrip(
                        serde,
                        new DateTimeTypes(
                                LocalTime.parse(
                                        "13:49:58.670943", DateTimeFormatter.ISO_LOCAL_TIME),
                                LocalDate.parse("2024-01-16", DateTimeFormatter.ISO_LOCAL_DATE),
                                LocalDateTime.parse(
                                        "2024-01-16T13:49:58.670944",
                                        DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                                ZonedDateTime.parse(
                                        "2024-01-16T13:49:58.670945+01:00[Europe/Paris]",
                                        DateTimeFormatter.ISO_ZONED_DATE_TIME),
                                OffsetTime.parse(
                                        "13:49:58.670946+02:00", DateTimeFormatter.ISO_OFFSET_TIME),
                                OffsetDateTime.parse(
                                        "2024-01-16T13:49:58.670947-03:00",
                                        DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                                MonthDay.parse("2024-01-16", DateTimeFormatter.ISO_LOCAL_DATE),
                                YearMonth.parse("2024-01-16", DateTimeFormatter.ISO_LOCAL_DATE),
                                Year.parse("2024-01-16", DateTimeFormatter.ISO_LOCAL_DATE)));

        assertThat(
                json,
                is(
                        "{"
                                + lineSeparator()
                                + "  \"localDate\" : \"2024-01-16\","
                                + lineSeparator()
                                + "  \"localDateTime\" : \"2024-01-16T13:49:58.670944\","
                                + lineSeparator()
                                + "  \"localTime\" : \"13:49:58.670943\","
                                + lineSeparator()
                                + "  \"monthDay\" : \"--01-16\","
                                + lineSeparator()
                                + "  \"offsetDateTime\" : \"2024-01-16T13:49:58.670947-03:00\","
                                + lineSeparator()
                                + "  \"offsetTime\" : \"13:49:58.670946+02:00\","
                                + lineSeparator()
                                + "  \"year\" : \"2024\","
                                + lineSeparator()
                                + "  \"yearMonth\" : \"2024-01\","
                                + lineSeparator()
                                + "  \"zonedDateTime\" : \"2024-01-16T13:49:58.670945+01:00\""
                                + lineSeparator()
                                + "}"));
    }

    @Test
    void shouldHandleTemporalTypes() {
        // When:
        final Serde<TemporalTypes> serde =
                serdeFactory.create(registeredSchema(TemporalTypes.class));
        serde.configure(Map.of(), false);

        // Then:
        final String json =
                assertRoundTrip(
                        serde,
                        new TemporalTypes(
                                Duration.parse("P2DT3H4M0.345000025S"),
                                Period.parse("P1Y2M3D"),
                                Instant.parse("2007-12-03T10:15:30.00Z")));

        assertThat(
                json,
                is(
                        "{"
                                + lineSeparator()
                                + "  \"duration\" : 183840.345000025,"
                                + lineSeparator()
                                + "  \"instant\" : \"2007-12-03T10:15:30Z\","
                                + lineSeparator()
                                + "  \"period\" : \"P1Y2M3D\""
                                + lineSeparator()
                                + "}"));
    }

    @Test
    void shouldNotFailOnDeserializeOnUnknownProperty() {
        // Given:
        final Serde<WithPrimitiveTypes> serde =
                serdeFactory.create(registeredSchema(WithPrimitiveTypes.class));
        serde.configure(Map.of(), false);
        final Deserializer<WithPrimitiveTypes> deserializer = serde.deserializer();

        // When:
        deserializer.deserialize("t", "{\"id\":10,\"additional\":2}".getBytes(UTF_8));

        // Then: did not throw.
    }

    @Test
    void shouldFailOnDeserializeOfNullPrimitive() {
        // Given:
        final Serde<WithPrimitiveTypes> serde =
                serdeFactory.create(registeredSchema(WithPrimitiveTypes.class));
        serde.configure(Map.of(), false);
        final Deserializer<WithPrimitiveTypes> deserializer = serde.deserializer();

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> deserializer.deserialize("t", "{\"id\":null}".getBytes(UTF_8)));

        // Then:
        assertThat(e.getMessage(), is("Validation failed. topic: t, part: value"));
        assertThat(e.getCause().getMessage(), containsString("Expected: [integer] Found: [null]"));
    }

    @Test
    void shouldFailOnDeserializeOfMissingPrimitive() {
        // Given:
        final Serde<WithPrimitiveTypes> serde =
                serdeFactory.create(registeredSchema(WithPrimitiveTypes.class));
        serde.configure(Map.of(), false);
        final Deserializer<WithPrimitiveTypes> deserializer = serde.deserializer();

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> deserializer.deserialize("t", "{}".getBytes(UTF_8)));

        // Then:
        assertThat(e.getMessage(), is("Validation failed. topic: t, part: value"));
        assertThat(e.getCause().getMessage(), containsString("Missing property id"));
    }

    @Test
    void shouldDeserializeAsBigDecimal() {
        // Given:
        final Serde<WithAmbiguousFloat> serde =
                serdeFactory.create(registeredSchema(WithAmbiguousFloat.class));
        serde.configure(Map.of(), false);
        final Deserializer<WithAmbiguousFloat> deserializer = serde.deserializer();

        // When:
        final WithAmbiguousFloat actual =
                deserializer.deserialize("t", "{\"number\":0.1}".getBytes(UTF_8));

        // Then:
        assertThat(actual.getNumber(), is(new BigDecimal("0.1")));
    }

    @Test
    void shouldFailToDeserializeUnknownSubType() {
        // Given:
        final Serde<TypeWithExplicitPolymorphism> serde =
                serdeFactory.create(registeredSchema(TypeWithExplicitPolymorphism.class));
        serde.configure(Map.of(), false);
        final Deserializer<TypeWithExplicitPolymorphism> deserializer = serde.deserializer();

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                deserializer.deserialize(
                                        "t",
                                        "{\"inner\": {\"@type\":\"unknown\"}}".getBytes(UTF_8)));

        // Then:
        assertThat(e.getMessage(), is("Validation failed. topic: t, part: value"));
        assertThat(e.getCause().getMessage(), containsString("Object not in enum"));
    }

    @Test
    void shouldFailToDeserializeUnknownEnum() {
        // Given:
        final Serde<WithEnum> serde = serdeFactory.create(registeredSchema(WithEnum.class));
        serde.configure(Map.of(), false);
        final Deserializer<WithEnum> deserializer = serde.deserializer();

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                deserializer.deserialize(
                                        "t", "{\"enumType\": \"unknown\"}".getBytes(UTF_8)));

        // Then:
        assertThat(e.getMessage(), is("Validation failed. topic: t, part: value"));
        assertThat(
                e.getCause().getMessage(),
                containsString("Object not in enums: [one, two, three]"));
    }

    @Test
    void shouldSupportManuallySuppliedSubtype() {
        // Given:
        serdeFactory =
                new JsonSchemaSerdeFactory(
                        Map.of(
                                TypeWithImplicitPolymorphism.ExplicitlyNamed.class, "",
                                TypeWithImplicitPolymorphism.ImplicitlyNamed.class, ""));

        final Serde<TypeWithImplicitPolymorphism> serde =
                serdeFactory.create(registeredSchema(TypeWithImplicitPolymorphism.class));
        serde.configure(Map.of(), false);
        final Deserializer<TypeWithImplicitPolymorphism> deserializer = serde.deserializer();

        // When:
        final TypeWithImplicitPolymorphism implicit =
                deserializer.deserialize(
                        "t",
                        "{\"inner\": {\"@type\":\"TypeWithImplicitPolymorphism$ImplicitlyNamed\", \"age\": 1}}"
                                .getBytes(UTF_8));
        final TypeWithImplicitPolymorphism explicit =
                deserializer.deserialize(
                        "t", "{\"inner\": {\"@type\":\"the-explicit-name\"}}".getBytes(UTF_8));

        // Then:
        assertThat(
                implicit.inner(),
                is(instanceOf(TypeWithImplicitPolymorphism.ImplicitlyNamed.class)));
        assertThat(
                explicit.inner(),
                is(instanceOf(TypeWithImplicitPolymorphism.ExplicitlyNamed.class)));
    }

    private static <T> RegisteredSchema<T> registeredSchema(final Class<T> type) {
        final ProducerSchema jsonSchema = LocalSchemaLoader.loadFromClasspath(type);
        return new RegisteredSchema<>(jsonSchema, 1, "s", type);
    }

    private <T> String assertRoundTrip(final Serde<T> serde, final T instance) {
        final byte[] bytes = assertCanSerialize(serde, instance);
        final T actual = assertCanDeserialize(serde, bytes);
        assertThat(actual, is(instance));
        return new String(bytes, UTF_8);
    }

    private static <T> byte[] assertCanSerialize(final Serde<T> serde, final T instance) {
        try {
            return serde.serializer().serialize("t", instance);
        } catch (final Exception e) {
            throw new AssertionError("Failed to serialize", e);
        }
    }

    private <T> T assertCanDeserialize(final Serde<T> serde, final byte[] bytes) {
        try {
            return serde.deserializer().deserialize("t", bytes);
        } catch (final Exception e) {
            throw new AssertionError("Failed to deserialize", e);
        }
    }
}
