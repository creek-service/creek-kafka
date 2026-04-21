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
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.observability.logging.structured.LogEntryCustomizer;
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
        public Response handleError(
                final ErrorHandlerContext context,
                final ProducerRecord<byte[], byte[]> record,
                final Exception exception) {

            logger.error(
                    "Failed to produce to topic",
                    log -> with(record, with(context, log)).withThrowable(exception));

            return Response.fail();
        }

        @SuppressWarnings("rawtypes")
        @Override
        public Response handleSerializationError(
                final ErrorHandlerContext context,
                final ProducerRecord record,
                final Exception exception,
                final SerializationExceptionOrigin origin) {
            logger.error(
                    "Failed to serialize",
                    log ->
                            with(record, with(context, log))
                                    .with("origin", origin.name())
                                    .withThrowable(exception));

            return Response.fail();
        }

        @Override
        public void configure(final Map<String, ?> configs) {}
    }

    private static LogEntryCustomizer with(
            final ErrorHandlerContext context, final LogEntryCustomizer log) {
        return log.with("input-topic", context.topic())
                .with("input-partition", context.partition())
                .with("input-key", Arrays.toString(context.sourceRawKey()));
    }

    private static LogEntryCustomizer with(
            final ProducerRecord<?, ?> record, final LogEntryCustomizer log) {
        return log.with("output-topic", record.topic())
                .with("output-partition", record.partition())
                .with("output-key", keyToString(record.key()));
    }

    @SuppressWarnings({"checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    private static String keyToString(final Object key) {
        final boolean isArray = key != null && key.getClass().isArray();
        if (!isArray) {
            return Objects.toString(key);
        }

        final Class<?> c = key.getClass().getComponentType();
        if (c == int.class) {
            return Arrays.toString((int[]) key);
        }
        if (c == long.class) {
            return Arrays.toString((long[]) key);
        }
        if (c == double.class) {
            return Arrays.toString((double[]) key);
        }
        if (c == float.class) {
            return Arrays.toString((float[]) key);
        }
        if (c == boolean.class) {
            return Arrays.toString((boolean[]) key);
        }
        if (c == byte.class) {
            return Arrays.toString((byte[]) key);
        }
        if (c == short.class) {
            return Arrays.toString((short[]) key);
        }
        if (c == char.class) {
            return Arrays.toString((char[]) key);
        }
        return Arrays.toString((Object[]) key);
    }
}
