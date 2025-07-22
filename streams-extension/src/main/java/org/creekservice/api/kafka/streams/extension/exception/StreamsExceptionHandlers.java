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

package org.creekservice.api.kafka.streams.extension.exception;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.observability.logging.structured.StructuredLogger;
import org.creekservice.api.observability.logging.structured.StructuredLoggerFactory;

/** Util class definining streams exception handlers. */
public final class StreamsExceptionHandlers {

    private StreamsExceptionHandlers() {}

    private static final StructuredLogger LOGGER =
            StructuredLoggerFactory.internalLogger(StreamsExceptionHandlers.class);

    /** Exception handler that logs the exception and causes the streams app to exit. */
    public static final class LogAndFailProductionExceptionHandler
            implements ProductionExceptionHandler {

        private final StructuredLogger logger;

        /** Constructor. */
        @SuppressWarnings("unused") // Invoked via reflection
        public LogAndFailProductionExceptionHandler() {
            this(LOGGER);
        }

        @VisibleForTesting
        LogAndFailProductionExceptionHandler(final StructuredLogger logger) {
            this.logger = requireNonNull(logger, "logger");
        }

        @Override
        public ProductionExceptionHandlerResponse handle(
                final ProducerRecord<byte[], byte[]> record, final Exception exception) {

            logger.error(
                    "Failed to produce to topic",
                    log ->
                            log.with("topic", record.topic())
                                    .with("partition", record.partition())
                                    .with("key", Arrays.toString(record.key()))
                                    .withThrowable(exception));

            return ProductionExceptionHandlerResponse.FAIL;
        }

        @Override
        public void configure(final Map<String, ?> configs) {}
    }
}
