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

package org.creekservice.internal.kafka.streams.extension.observation;


import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.kafka.common.MetricName;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsFilter;

/** Default implentation of {@link KafkaMetricsFilter}. */
public final class DefaultMetricsFilter implements KafkaMetricsFilter {

    private static final Predicate<String> ALL = name -> true;
    private static final Predicate<String> NONE = name -> false;

    private static final Map<String, Predicate<String>> METRIC_GROUP_FILTERS =
            Map.of(
                    "app-info",
                    ALL,
                    "consumer-coordinator-metrics",
                    ALL,
                    "consumer-fetch-manager-metrics",
                    ALL,
                    "consumer_node_metrics",
                    ALL,
                    "consumer-metrics",
                    ALL,
                    "producer-metrics",
                    ALL,
                    "producer-node-metrics",
                    ALL,
                    "producer-topic-metrics",
                    ALL,
                    "stream-metrics",
                    name -> !name.equals("topology-description"),
                    "stream-thread-metrics",
                    ALL);

    private static final Set<String> EXCLUDED_TAGS = Set.of("client-id");
    private static final String THREAD_ID_TAG = "thread-id";

    @Override
    public boolean includeMetric(final MetricName name) {
        return METRIC_GROUP_FILTERS.getOrDefault(name.group(), NONE).test(name.name());
    }

    @Override
    public boolean includeValue(final Object v) {
        if (v instanceof Double) {
            return Double.isFinite((Double) v);
        }
        if (v instanceof Float) {
            return Float.isFinite((Float) v);
        }
        return true;
    }

    @Override
    public Stream<String> filterTags(final Map<String, String> tags) {
        return tags.entrySet().stream()
                .filter(e -> !EXCLUDED_TAGS.contains(e.getKey()))
                .map(e -> namespace(e.getKey(), e.getValue()));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private String namespace(final String tagKey, final String tagValue) {
        return THREAD_ID_TAG.equals(tagKey) ? threadName(tagValue) : tagValue;
    }

    private static String threadName(final String name) {
        final int idx = name.indexOf("StreamThread");
        return idx < 0 ? name : name.substring(idx + "Stream".length()).toLowerCase();
    }
}
