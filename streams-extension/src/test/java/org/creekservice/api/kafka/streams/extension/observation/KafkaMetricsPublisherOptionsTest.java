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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import java.time.Duration;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultMetricsFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaMetricsPublisherOptionsTest {

    private KafkaMetricsPublisherOptions.Builder builder;

    @BeforeEach
    void setUp() {
        builder = KafkaMetricsPublisherOptions.builder();
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        KafkaMetricsPublisherOptions.builder().build(),
                        KafkaMetricsPublisherOptions.builder().build())
                .addEqualityGroup(
                        KafkaMetricsPublisherOptions.builder()
                                .withPublishPeriod(Duration.ZERO)
                                .build())
                .addEqualityGroup(
                        KafkaMetricsPublisherOptions.builder()
                                .withMetricsFilter(mock(KafkaMetricsFilter.class))
                                .build())
                .testEquals();
    }

    @Test
    void shouldDefaultPublishingPeriod() {
        assertThat(builder.build().publishPeriod(), is(Duration.ofMinutes(1)));
    }

    @Test
    void shouldCustomizePublishingPeriod() {
        // Given:
        builder.withPublishPeriod(Duration.ofHours(1));

        // When:
        final KafkaMetricsPublisherOptions options = builder.build();

        // Then:
        assertThat(options.publishPeriod(), is(Duration.ofHours(1)));
    }

    @Test
    void shouldDefaultMetricsFilter() {
        assertThat(builder.build().metricsFilter(), is(instanceOf(DefaultMetricsFilter.class)));
    }

    @Test
    void shouldCustomizePublishingFilter() {
        // Given:
        final KafkaMetricsFilter filter = mock(KafkaMetricsFilter.class);
        builder.withMetricsFilter(filter);

        // When:
        final KafkaMetricsPublisherOptions options = builder.build();

        // Then:
        assertThat(options.metricsFilter(), is(filter));
    }
}
