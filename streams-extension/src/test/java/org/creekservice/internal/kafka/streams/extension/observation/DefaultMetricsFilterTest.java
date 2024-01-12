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

package org.creekservice.internal.kafka.streams.extension.observation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.MetricName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DefaultMetricsFilterTest {

    private DefaultMetricsFilter filter;

    @BeforeEach
    void setUp() {
        filter = new DefaultMetricsFilter();
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(new DefaultMetricsFilter(), new DefaultMetricsFilter())
                .addEqualityGroup("diff")
                .testEquals();
    }

    @Test
    void shouldImplementToString() {
        assertThat(new DefaultMetricsFilter().toString(), is("DefaultMetricsFilter"));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "app-info",
                "consumer-coordinator-metrics",
                "consumer-fetch-manager-metrics",
                "consumer_node_metrics",
                "consumer-metrics",
                "producer-metrics",
                "producer-node-metrics",
                "producer-topic-metrics",
                "stream-metrics",
                "stream-thread-metrics",
            })
    void shouldIncludeGroups(final String groupName) {
        // Given:
        final MetricName name = new MetricName("anything", groupName, "description", Map.of());

        // Then:
        assertThat(filter.includeMetric(name), is(true));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "admin-client-metrics",
                "admin-client-node-metrics",
            })
    void shouldExcludeOtherGroups() {
        // Given:
        final MetricName name = new MetricName("anything", "other", "description", Map.of());

        // Then:
        assertThat(filter.includeMetric(name), is(false));
    }

    @Test
    void shouldExcludeTopologyDescription() {
        // Given:
        final MetricName name =
                new MetricName("topology-description", "stream-metrics", "description", Map.of());

        // Then:
        assertThat(filter.includeMetric(name), is(false));
    }

    @Test
    void shouldExcludeInfiniteDouble() {
        assertThat(filter.includeValue(Double.POSITIVE_INFINITY), is(false));
        assertThat(filter.includeValue(Double.NEGATIVE_INFINITY), is(false));
    }

    @Test
    void shouldExcludeInfiniteFloat() {
        assertThat(filter.includeValue(Float.POSITIVE_INFINITY), is(false));
        assertThat(filter.includeValue(Float.NEGATIVE_INFINITY), is(false));
    }

    @Test
    void shouldIncludeValueValues() {
        assertThat(filter.includeValue(1.0), is(true));
        assertThat(filter.includeValue(1.0d), is(true));
        assertThat(filter.includeValue("this"), is(true));
        assertThat(filter.includeValue(null), is(true));
    }

    @Test
    void shouldReturnTagValues() {
        // When:
        final Stream<String> result = filter.filterTags(Map.of("k2", "v2", "k1", "v1"));

        // Then:
        assertThat(result.collect(Collectors.toUnmodifiableList()), containsInAnyOrder("v2", "v1"));
    }

    @Test
    void shouldExcludeClientIdTag() {
        // When:
        final Stream<String> result = filter.filterTags(Map.of("client-id", "ignore me"));

        // Then:
        assertThat(result.collect(Collectors.toUnmodifiableList()), is(empty()));
    }

    @Test
    void shouldShortenThreadId() {
        // When:
        final Stream<String> result =
                filter.filterTags(Map.of("thread-id", "the-client-id-StreamThread-12"));

        // Then:
        assertThat(result.collect(Collectors.toUnmodifiableList()), contains("thread-12"));
    }

    @Test
    void shouldLeaveNonStandardThreadIdAlone() {
        // When:
        final Stream<String> result = filter.filterTags(Map.of("thread-id", "not-a-stream-thread"));

        // Then:
        assertThat(
                result.collect(Collectors.toUnmodifiableList()), contains("not-a-stream-thread"));
    }
}
