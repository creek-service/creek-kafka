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

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.observability.lifecycle.LifecycleLogging.lifecycleLogMessage;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creekservice.api.observability.lifecycle.BasicLifecycle;
import org.creekservice.api.observability.lifecycle.LoggableLifecycle;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;

/** Default implementation of {@link LifecycleObserver} */
public final class DefaultLifecycleObserver implements LifecycleObserver {

    private static final StructuredLogger LOGGER =
            StructuredLoggerFactory.internalLogger(DefaultLifecycleObserver.class);

    private final StructuredLogger logger;

    /** Constructor */
    public DefaultLifecycleObserver() {
        this(LOGGER);
    }

    @VisibleForTesting
    DefaultLifecycleObserver(final StructuredLogger logger) {
        this.logger = requireNonNull(logger, "logger");
    }

    @Override
    public void starting() {
        log(Lifecycle.starting, Map.of());
    }

    @Override
    public void rebalancing() {
        log(Lifecycle.rebalancing, Map.of());
    }

    @Override
    public void running() {
        log(Lifecycle.running, Map.of());
    }

    @Override
    public void started() {
        log(Lifecycle.started, Map.of());
    }

    @Override
    public void failed(final Throwable e) {
        logger.error("Failure of streams app detected.", log -> log.withThrowable(e));
    }

    @Override
    public void stopping(final ExitCode exitCode) {
        log(Lifecycle.stopping, Map.of(Metric.exitCode, exitCode));
    }

    @Override
    public void stopTimedOut(final Duration closeTimeout) {
        logger.warn(
                "Failed to stop the Kafka Streams app within the configured timeout, i.e. streams.close() returned false",
                log -> log.with(Metric.closeTimeout, closeTimeout));
    }

    @Override
    public void stopFailed(final Throwable cause) {
        logger.error(
                "Failed to stop the Kafka Streams app as streams.close() threw an exception",
                log -> log.withThrowable(cause));
    }

    @Override
    public void stopped(final ExitCode exitCode) {
        log(Lifecycle.stopped, Map.of(Metric.exitCode, exitCode));
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

    private void log(final Lifecycle lifecycle, final Map<Metric, ?> additional) {
        logger.info(
                "Kafka streams app state change",
                log -> {
                    log.with(
                            Metric.lifecycle, lifecycle.logMessage(LoggableLifecycle.SERVICE_TYPE));
                    additional.forEach(log::with);
                });
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private enum Lifecycle implements LoggableLifecycle {
        starting(BasicLifecycle.starting),
        started(BasicLifecycle.started),
        stopping(BasicLifecycle.stopping),
        stopped(BasicLifecycle.stopped),
        rebalancing(null),
        running(null);

        private final Optional<BasicLifecycle> basic;

        Lifecycle(final BasicLifecycle basic) {
            this.basic = Optional.ofNullable(basic);
        }

        @Override
        public String logMessage(final String targetType) {
            return basic.map(b -> b.logMessage(targetType))
                    .orElseGet(() -> lifecycleLogMessage(targetType, this));
        }
    }

    private enum Metric {
        lifecycle,
        exitCode,
        closeTimeout
    }
}
