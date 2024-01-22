/*
 * Copyright 2024 Creek Contributors (https://github.com/creek-service)
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

import static java.lang.System.lineSeparator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.json.JsonMapper;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class BaseJsonMapperTest {

    private final JsonMapper mapper = BaseJsonMapper.get();

    @Test
    void shouldIndentOutput() throws Exception {
        assertThat(
                mapper.writeValueAsString(new WithPrimitiveTypes(12)),
                is("{" + lineSeparator() + "  \"id\" : 12" + lineSeparator() + "}"));
    }

    @Test
    void shouldHandleJdk8Types() throws Exception {
        // Given:
        final Optional<UUID> optional = Optional.of(UUID.randomUUID());

        // When:
        final String json = mapper.writeValueAsString(new WithOptional(optional));
        final WithOptional result = mapper.readValue(json, WithOptional.class);

        // Then:
        assertThat(result.getOptional(), is(optional));
    }

    @Test
    void shouldNotSerializeEmptyProperties() throws Exception {
        // When:
        final String json =
                mapper.writeValueAsString(new EmptyType(null, "", Optional.empty(), List.of()));
        final EmptyType result = mapper.readValue(json, EmptyType.class);

        // Then:
        assertThat(json, is("{ }"));
        assertThat(result.getBoxed(), is(nullValue()));
        assertThat(result.getString(), is(nullValue()));
        assertThat(result.getOptional(), is(Optional.empty()));
        assertThat(result.getCollection(), is(nullValue()));
    }

    @Test
    void shouldHandleDateTimeTypes() throws Exception {
        // Given:
        final LocalTime localTime =
                LocalTime.parse("13:49:58.670943", DateTimeFormatter.ISO_LOCAL_TIME);
        final LocalDate localDate = LocalDate.parse("2024-01-16", DateTimeFormatter.ISO_LOCAL_DATE);
        final LocalDateTime localDateTime =
                LocalDateTime.parse(
                        "2024-01-16T13:49:58.670944", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        final ZonedDateTime zonedDateTime =
                ZonedDateTime.parse(
                        "2024-01-16T13:49:58.670945+01:00[Europe/Paris]",
                        DateTimeFormatter.ISO_ZONED_DATE_TIME);
        final OffsetTime offsetTime =
                OffsetTime.parse("13:49:58.670946+02:00", DateTimeFormatter.ISO_OFFSET_TIME);
        final OffsetDateTime offsetDateTime =
                OffsetDateTime.parse(
                        "2024-01-16T13:49:58.670947-03:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        final MonthDay monthDay = MonthDay.parse("2024-01-16", DateTimeFormatter.ISO_LOCAL_DATE);
        final YearMonth yearMonth = YearMonth.parse("2024-01-16", DateTimeFormatter.ISO_LOCAL_DATE);
        final Year year = Year.parse("2024-01-16", DateTimeFormatter.ISO_LOCAL_DATE);

        // When:
        final String json =
                mapper.writeValueAsString(
                        new DateTimeTypes(
                                localTime,
                                localDate,
                                localDateTime,
                                zonedDateTime,
                                offsetTime,
                                offsetDateTime,
                                monthDay,
                                yearMonth,
                                year));
        final DateTimeTypes result = mapper.readValue(json, DateTimeTypes.class);

        // Then:
        assertThat(
                json,
                is(
                        "{"
                                + lineSeparator()
                                + ""
                                + "  \"localDate\" : \"2024-01-16\","
                                + lineSeparator()
                                + ""
                                + "  \"localDateTime\" : \"2024-01-16T13:49:58.670944\","
                                + lineSeparator()
                                + ""
                                + "  \"localTime\" : \"13:49:58.670943\","
                                + lineSeparator()
                                + ""
                                + "  \"monthDay\" : \"--01-16\","
                                + lineSeparator()
                                + ""
                                + "  \"offsetDateTime\" : \"2024-01-16T13:49:58.670947-03:00\","
                                + lineSeparator()
                                + ""
                                + "  \"offsetTime\" : \"13:49:58.670946+02:00\","
                                + lineSeparator()
                                + ""
                                + "  \"year\" : \"2024\","
                                + lineSeparator()
                                + ""
                                + "  \"yearMonth\" : \"2024-01\","
                                + lineSeparator()
                                + ""
                                + "  \"zonedDateTime\" : \"2024-01-16T13:49:58.670945+01:00\""
                                + lineSeparator()
                                + ""
                                + "}"));
        assertThat(result.getLocalTime(), is(localTime));
        assertThat(result.getLocalDate(), is(localDate));
        assertThat(result.getLocalDateTime(), is(localDateTime));
        assertThat(result.getOffsetTime(), is(offsetTime));
        assertThat(result.getOffsetDateTime(), is(offsetDateTime));
        assertThat(result.getMonthDay(), is(monthDay));
        assertThat(result.getYearMonth(), is(yearMonth));
        assertThat(result.getYear(), is(year));

        // Note: Zoned Date Time does not include zone info in serialized form,
        // as the zone name is not standardized.
        assertThat(result.getZonedDateTime().getOffset(), is(zonedDateTime.getOffset()));
        assertThat(
                result.getZonedDateTime().toLocalDateTime(), is(zonedDateTime.toLocalDateTime()));
    }

    @Test
    void shouldHandleTemporalTypes() throws Exception {
        // Given:
        final Duration duration = Duration.parse("P2DT3H4M0.345000025S");
        final Period period = Period.parse("P1Y2M3D");
        final Instant instant = Instant.parse("2007-12-03T10:15:30.00Z");

        // When:
        final String json = mapper.writeValueAsString(new TemporalTypes(duration, period, instant));
        final TemporalTypes result = mapper.readValue(json, TemporalTypes.class);

        // Then:
        assertThat(
                json,
                is(
                        "{"
                                + lineSeparator()
                                + ""
                                + "  \"duration\" : 183840.345000025,"
                                + lineSeparator()
                                + ""
                                + "  \"instant\" : \"2007-12-03T10:15:30Z\","
                                + lineSeparator()
                                + ""
                                + "  \"period\" : \"P1Y2M3D\""
                                + lineSeparator()
                                + ""
                                + "}"));
        assertThat(result.getDuration(), is(duration));
        assertThat(result.getPeriod(), is(period));
        assertThat(result.getInstant(), is(instant));
    }

    @Test
    void shouldNotFailOnDeserializeOnUnknownProperty() throws Exception {
        // When:
        mapper.readValue("{\"id\":10,\"additional\":2}", WithPrimitiveTypes.class);

        // Then: did not throw.
    }

    @Test
    void shouldFailOnDeserializeOfNullPrimitive() {
        // When:
        final Exception e =
                assertThrows(
                        MismatchedInputException.class,
                        () -> mapper.readValue("{\"id\":null}", WithPrimitiveTypes.class));

        // Then:
        assertThat(e.getMessage(), containsString("Cannot map `null` into type `long`"));
    }

    @Test
    void shouldDeserializeAsBigDecimal() throws Exception {
        // When:
        final WithAmbiguousFloat result =
                mapper.readValue("{\"number\":0.1}", WithAmbiguousFloat.class);

        // Then:
        assertThat(result.number, is(new BigDecimal("0.1")));
    }

    @Test
    void shouldFailToDeserializeUnknownSubType() {
        // When:
        final Exception e =
                assertThrows(
                        MismatchedInputException.class,
                        () -> mapper.readValue("{\"@type\":\"unknown\"}", SomeType.class));

        // Then:
        assertThat(
                e.getMessage(), containsString("Could not resolve type id 'unknown' as a subtype"));
    }

    @Test
    void shouldDeserializeUnknownSubTypeAsDefault() throws Exception {
        // When:
        final TypeWithDefault result =
                mapper.readValue("{\"@type\":\"unknown\"}", TypeWithDefault.class);

        // Then:
        assertThat(result, is(instanceOf(DefaultType.class)));
    }

    @Test
    void shouldFailToDeserializeUnknownEnum() {
        // When:
        final Exception e =
                assertThrows(
                        MismatchedInputException.class,
                        () -> mapper.readValue("\"unknown\"", EnumType.class));

        // Then:
        assertThat(e.getMessage(), containsString("not one of the values accepted for Enum class"));
    }

    @Test
    void shouldDeserializeUnknownEnumAsDefault() throws Exception {
        // When:
        final EnumWithDefault result = mapper.readValue("\"new\"", EnumWithDefault.class);

        // Then:
        assertThat(result, is(EnumWithDefault.unknown));
    }

    public enum EnumType {
        one,
        two,
        three
    }

    public enum EnumWithDefault {
        one,
        two,
        @JsonEnumDefaultValue
        unknown
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    public interface SomeType {}

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = DefaultType.class)
    public interface TypeWithDefault {}

    public static final class DefaultType implements TypeWithDefault {}

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class WithOptional {
        private final Optional<UUID> optional;

        WithOptional(@JsonProperty("optional") final Optional<UUID> optional) {
            this.optional = optional;
        }

        public Optional<UUID> getOptional() {
            return optional;
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class EmptyType {

        private final Long boxed;
        private final String string;
        private final Optional<String> optional;
        private final List<String> collection;

        EmptyType(
                @JsonProperty("boxed") final Long boxed,
                @JsonProperty("string") final String string,
                @JsonProperty("optional") final Optional<String> optional,
                @JsonProperty("collection") final List<String> collection) {
            this.boxed = boxed;
            this.string = string;
            this.optional = optional;
            this.collection = collection;
        }

        public Long getBoxed() {
            return boxed;
        }

        public String getString() {
            return string;
        }

        public Optional<String> getOptional() {
            return optional;
        }

        public List<String> getCollection() {
            return collection == null ? null : List.copyOf(collection);
        }
    }

    @SuppressWarnings("unused")
    public static final class WithPrimitiveTypes {

        WithPrimitiveTypes(@JsonProperty("id") final long id) {}

        public long getId() {
            return 12;
        }
    }

    public static final class DateTimeTypes {

        private final LocalTime localTime;
        private final LocalDate localDate;
        private final LocalDateTime localDateTime;
        private final ZonedDateTime zonedDateTime;
        private final OffsetTime offsetTime;
        private final OffsetDateTime offsetDateTime;
        private final MonthDay monthDay;
        private final YearMonth yearMonth;
        private final Year year;

        @SuppressWarnings("checkstyle:ParameterNumber")
        DateTimeTypes(
                @JsonProperty("localTime") final LocalTime localTime,
                @JsonProperty("localDate") final LocalDate localDate,
                @JsonProperty("localDateTime") final LocalDateTime localDateTime,
                @JsonProperty("zonedDateTime") final ZonedDateTime zonedDateTime,
                @JsonProperty("offsetTime") final OffsetTime offsetTime,
                @JsonProperty("offsetDateTime") final OffsetDateTime offsetDateTime,
                @JsonProperty("monthDay") final MonthDay monthDay,
                @JsonProperty("yearMonth") final YearMonth yearMonth,
                @JsonProperty("year") final Year year) {
            this.localTime = localTime;
            this.localDate = localDate;
            this.localDateTime = localDateTime;
            this.zonedDateTime = zonedDateTime;
            this.offsetTime = offsetTime;
            this.offsetDateTime = offsetDateTime;
            this.monthDay = monthDay;
            this.yearMonth = yearMonth;
            this.year = year;
        }

        public LocalTime getLocalTime() {
            return localTime;
        }

        public LocalDate getLocalDate() {
            return localDate;
        }

        public LocalDateTime getLocalDateTime() {
            return localDateTime;
        }

        public ZonedDateTime getZonedDateTime() {
            return zonedDateTime;
        }

        public OffsetTime getOffsetTime() {
            return offsetTime;
        }

        public OffsetDateTime getOffsetDateTime() {
            return offsetDateTime;
        }

        public MonthDay getMonthDay() {
            return monthDay;
        }

        public YearMonth getYearMonth() {
            return yearMonth;
        }

        public Year getYear() {
            return year;
        }
    }

    public static final class TemporalTypes {

        private final Duration duration;
        private final Period period;
        private final Instant instant;

        TemporalTypes(
                @JsonProperty("duration") final Duration duration,
                @JsonProperty("period") final Period period,
                @JsonProperty("instant") final Instant instant) {
            this.duration = duration;
            this.period = period;
            this.instant = instant;
        }

        public Duration getDuration() {
            return duration;
        }

        public Period getPeriod() {
            return period;
        }

        public Instant getInstant() {
            return instant;
        }
    }

    public static final class WithAmbiguousFloat {

        private final Object number;

        WithAmbiguousFloat(@JsonProperty("number") final Object number) {
            this.number = number;
        }
    }
}
