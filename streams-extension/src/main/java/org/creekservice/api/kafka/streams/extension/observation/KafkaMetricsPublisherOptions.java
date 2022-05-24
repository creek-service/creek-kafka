/*
 * Copyright 2022 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.streams.extension.observation;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultMetricsFilter;

public final class KafkaMetricsPublisherOptions {

    private static final Duration DEFAULT_PERIOD = Duration.ofMinutes(1);

    private final Duration publishPeriod;
    private final KafkaMetricsFilter metricsFilter;

    public static Builder builder() {
        return new Builder();
    }

    private KafkaMetricsPublisherOptions(
            final Duration publishPeriod, final KafkaMetricsFilter metricsFilter) {
        this.publishPeriod = requireNonNull(publishPeriod, "publishPeriod");
        this.metricsFilter = requireNonNull(metricsFilter, "metricsFilter");
    }

    /** @return time between each publishing of metrics. */
    public Duration publishPeriod() {
        return publishPeriod;
    }

    /** @return filter to control what metrics should be published. */
    public KafkaMetricsFilter metricsFilter() {
        return metricsFilter;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KafkaMetricsPublisherOptions that = (KafkaMetricsPublisherOptions) o;
        return Objects.equals(publishPeriod, that.publishPeriod)
                && Objects.equals(metricsFilter, that.metricsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(publishPeriod, metricsFilter);
    }

    @Override
    public String toString() {
        return "KafkaMetricsPublisherOptions{"
                + "publishPeriod="
                + publishPeriod
                + ", metricsFilter="
                + metricsFilter
                + '}';
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static final class Builder {

        private Duration publishPeriod = DEFAULT_PERIOD;
        private Optional<KafkaMetricsFilter> metricsFilter = Optional.empty();

        private Builder() {}

        /**
         * Set the publishing period, i.e. how frequently should metrics be published?
         *
         * <p>Setting the period to zero will disable metrics publishing.
         *
         * <p>Default is {@link #DEFAULT_PERIOD}.
         *
         * @param period the required publishing period.
         * @return self.
         */
        public Builder withPublishPeriod(final Duration period) {
            this.publishPeriod = requireNonNull(period, "period");
            return this;
        }

        /**
         * Set a filter to control what metrics should be published.
         *
         * @param filter the filter
         * @return self.
         */
        public Builder withMetricsFilter(final KafkaMetricsFilter filter) {
            this.metricsFilter = Optional.of(filter);
            return this;
        }

        public KafkaMetricsPublisherOptions build() {
            return new KafkaMetricsPublisherOptions(
                    publishPeriod, metricsFilter.orElseGet(DefaultMetricsFilter::new));
        }
    }
}
