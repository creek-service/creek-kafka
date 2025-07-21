/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creekservice.api.observability.logging.structured.LogEntryCustomizer;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;

/** Default implementation of {@link StateRestoreObserver}. */
public final class DefaultStateRestoreObserver implements StateRestoreObserver {

    private static final StructuredLogger LOGGER =
            StructuredLoggerFactory.internalLogger(DefaultStateRestoreObserver.class);

    private final StructuredLogger logger;

    /** Constructor */
    public DefaultStateRestoreObserver() {
        this(LOGGER);
    }

    @VisibleForTesting
    DefaultStateRestoreObserver(final StructuredLogger logger) {
        this.logger = requireNonNull(logger, "logger");
    }

    @Override
    public void restoreStarted(
            final String topic,
            final int partition,
            final String storeName,
            final long startingOffset,
            final long endingOffset) {
        logger.info(
                "Restore of state store partition starting",
                log ->
                        defaults(log, topic, partition, storeName)
                                .with("startOffset", startingOffset)
                                .with("endOffset", endingOffset));
    }

    @Override
    public void restoreFinished(
            final String topic,
            final int partition,
            final String storeName,
            final long totalRestored) {
        logger.info(
                "Restore of state store partition finished",
                log -> defaults(log, topic, partition, storeName).with("restored", totalRestored));
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

    private static LogEntryCustomizer defaults(
            final LogEntryCustomizer log,
            final String topic,
            final int partition,
            final String storeName) {
        return log.with(Metric.store, storeName)
                .with(Metric.topic, topic)
                .with(Metric.partition, partition);
    }

    private enum Metric {
        store,
        topic,
        partition
    }
}
