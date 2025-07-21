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

package org.creekservice.internal.kafka.serde.json.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.util.Objects;
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

@SuppressWarnings("unused")
@GeneratesSchema
public final class DateTimeTypes {

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
    public DateTimeTypes(
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

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DateTimeTypes that = (DateTimeTypes) o;

        return Objects.equals(localTime, that.localTime)
                && Objects.equals(localDate, that.localDate)
                && Objects.equals(localDateTime, that.localDateTime)
                // Note: ZonedDateTime does not include zone info in serialized form,
                // as the zone name is not standardized. So we exclude that in this comparison
                && Objects.equals(zonedDateTime.getOffset(), that.zonedDateTime.getOffset())
                && Objects.equals(
                        zonedDateTime.toLocalDateTime(), that.zonedDateTime.toLocalDateTime())
                && Objects.equals(offsetTime, that.offsetTime)
                && Objects.equals(offsetDateTime, that.offsetDateTime)
                && Objects.equals(monthDay, that.monthDay)
                && Objects.equals(yearMonth, that.yearMonth)
                && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                localTime,
                localDate,
                localDateTime,
                zonedDateTime,
                offsetTime,
                offsetDateTime,
                monthDay,
                yearMonth,
                year);
    }

    @Override
    public String toString() {
        return "DateTimeTypes{"
                + "localTime="
                + localTime
                + ", localDate="
                + localDate
                + ", localDateTime="
                + localDateTime
                + ", zonedDateTime="
                + zonedDateTime
                + ", offsetTime="
                + offsetTime
                + ", offsetDateTime="
                + offsetDateTime
                + ", monthDay="
                + monthDay
                + ", yearMonth="
                + yearMonth
                + ", year="
                + year
                + '}';
    }
}
