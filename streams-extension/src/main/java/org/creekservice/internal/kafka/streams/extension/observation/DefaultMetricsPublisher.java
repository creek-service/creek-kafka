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

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsFilter;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.observability.logging.structured.LogEntryCustomizer;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;

/** Default impl of {@link MetricsPublisher}. */
public final class DefaultMetricsPublisher implements MetricsPublisher {

    private static final StructuredLogger LOGGER =
            StructuredLoggerFactory.internalLogger(DefaultMetricsPublisher.class);

    private static final Pattern SANITIZE_PATTERN = Pattern.compile("[^A-Za-z0-9]");

    private final KafkaMetricsPublisherOptions options;
    private final StructuredLogger logger;
    private final ScheduledExecutorService scheduler;

    /**
     * @param options publisher options.
     */
    public DefaultMetricsPublisher(final KafkaMetricsPublisherOptions options) {
        this(
                options,
                LOGGER,
                Executors.newScheduledThreadPool(1, r -> new Thread(r, "creek-metrics-publisher")));
    }

    @VisibleForTesting
    DefaultMetricsPublisher(
            final KafkaMetricsPublisherOptions options,
            final StructuredLogger logger,
            final ScheduledExecutorService scheduler) {
        this.options = requireNonNull(options, "options");
        this.logger = requireNonNull(logger, "logger");
        this.scheduler = requireNonNull(scheduler, "scheduler");
    }

    @Override
    public void schedule(final Supplier<Map<MetricName, ? extends Metric>> metricsSupplier) {
        final long period = options.publishPeriod().toMillis();
        if (period <= 0) {
            return;
        }

        scheduler.scheduleAtFixedRate(
                () -> log(metricsSupplier), period, period, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        scheduler.shutdown();

        try {
            final long timeoutMs = 2 * options.publishPeriod().toMillis();
            final boolean terminated = scheduler.awaitTermination(timeoutMs, TimeUnit.MILLISECONDS);
            if (!terminated) {
                logger.warn("Timed out shutting down scheduler");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void log(final Supplier<Map<MetricName, ? extends Metric>> metricsSupplier) {
        try {
            logger.info(
                    "Kafka metrics",
                    log -> metricsSupplier.get().forEach((n, m) -> addMetric(log, n, m)));
        } catch (final Exception e) {
            logger.error("Logging of Kafka metrics failed", log -> log.withThrowable(e));
        }
    }

    private void addMetric(
            final LogEntryCustomizer log, final MetricName name, final Metric metric) {
        final KafkaMetricsFilter filter = options.metricsFilter();
        if (!filter.includeMetric(name)) {
            return;
        }

        final Object value = metric.metricValue();
        if (!filter.includeValue(value)) {
            return;
        }

        nsByTags(log, name).with(sanitize(name.name()), value);
    }

    private LogEntryCustomizer nsByTags(final LogEntryCustomizer log, final MetricName name) {

        final LogEntryCustomizer[] nested = {log.ns(sanitize(name.group()))};

        final KafkaMetricsFilter filter = options.metricsFilter();

        filter.filterTags(name.tags())
                .sorted()
                .forEachOrdered(ns -> nested[0] = nested[0].ns(sanitize(ns)));

        return nested[0];
    }

    private static String sanitize(final String name) {
        return SANITIZE_PATTERN.matcher(name).replaceAll("_");
    }
}
