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
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.Objects;
import org.creekservice.api.base.annotation.schema.GeneratesSchema;

@SuppressWarnings("unused")
@GeneratesSchema
public final class TemporalTypes {

    private final Duration duration;
    private final Period period;
    private final Instant instant;

    public TemporalTypes(
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TemporalTypes that = (TemporalTypes) o;
        return Objects.equals(duration, that.duration)
                && Objects.equals(period, that.period)
                && Objects.equals(instant, that.instant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(duration, period, instant);
    }

    @Override
    public String toString() {
        return "TemporalTypes{"
                + "duration="
                + duration
                + ", period="
                + period
                + ", instant="
                + instant
                + '}';
    }
}
