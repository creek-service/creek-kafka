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


import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.MetricName;

/** Filter of Kafka metrics. */
public interface KafkaMetricsFilter {

    /**
     * Called to determine if a metric with the supplied {@code name} should be included.
     *
     * @param name the name of the metric.
     * @return {@code true} if it should be included, {@code false} otherwise.
     */
    default boolean includeMetric(final MetricName name) {
        return true;
    }

    /**
     * Called to determine if a metric should be excluded based on its value.
     *
     * @param value the value of the metric.
     * @return {@code true} if it should be included, {@code false} otherwise.
     */
    default boolean includeValue(final Object value) {
        return true;
    }

    /**
     * Called to determine the namespace a metric should be in.
     *
     * <p>Multiple Kafka metrics can have the same group and name, but different tags. To avoid
     * clashes, the metrics should be placed in suitable namespaces.
     *
     * @param tags the metric tags.
     * @return A stream of namespace names to nest the metric in.
     */
    default Stream<String> filterTags(final Map<String, String> tags) {
        return tags.values().stream();
    }
}
