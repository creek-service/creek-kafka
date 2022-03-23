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

package org.creek.internal.kafka.streams.extension.observation;


import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

/** Publisher of app metrics */
public interface MetricsPublisher extends AutoCloseable {

    /**
     * Schedule metrics publication.
     *
     * <p>The supplied {@code metricsSupplier} will periodically be polled to get the current
     * metrics to publish.
     *
     * @param metricsSupplier the supplier of metrics to publish.
     */
    void schedule(Supplier<Map<MetricName, ? extends Metric>> metricsSupplier);

    /** Shutdown the publisher and release any resources. */
    default void close() {}
}
