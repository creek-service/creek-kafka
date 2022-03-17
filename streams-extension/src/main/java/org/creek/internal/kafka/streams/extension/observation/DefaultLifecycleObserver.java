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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.Map;
import org.creek.api.kafka.streams.observation.LifecycleObserver;
import org.creek.api.observability.logging.structured.StructuredLogger;
import org.creek.api.observability.logging.structured.StructuredLoggerFactory;

public final class DefaultLifecycleObserver implements LifecycleObserver {

    private static final StructuredLogger LOGGER =
            StructuredLoggerFactory.internalLogger(DefaultLifecycleObserver.class);

    private final StructuredLogger logger;

    public DefaultLifecycleObserver() {
        this(LOGGER);
    }

    public DefaultLifecycleObserver(final StructuredLogger logger) {
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
                    log.with(Metric.lifecycle, lifecycle);
                    additional.forEach(log::with);
                });
    }

    private enum Lifecycle {
        starting,
        started,
        stopped,
        stopping,
        rebalancing,
        running
    }

    private enum Metric {
        lifecycle,
        exitCode,
        closeTimeout
    }
}
