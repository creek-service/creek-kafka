/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaMetricsFilterTest {

    private KafkaMetricsFilter filter;

    @BeforeEach
    void setUp() {
        filter = new KafkaMetricsFilter() {};
    }

    @Test
    void shouldIncludeAllMetricsByDefault() {
        assertThat(filter.includeMetric(mock(MetricName.class)), is(true));
    }

    @Test
    void shouldIncludeAllValuesByDefault() {
        assertThat(filter.includeValue(null), is(true));
    }

    @Test
    void shouldIncludeAllTagValuesByDefault() {
        // When:
        final Stream<String> result = filter.filterTags(Map.of("k2", "v2", "k1", "v1"));

        // Then:
        final List<String> namespaces = result.collect(Collectors.toUnmodifiableList());
        assertThat(namespaces, containsInAnyOrder("v2", "v1"));
    }
}
