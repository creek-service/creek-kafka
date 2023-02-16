/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse;
import org.creekservice.api.kafka.streams.extension.exception.StreamsExceptionHandlers.LogAndFailProductionExceptionHandler;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StreamsExceptionHandlersTest {

    private final TestStructuredLogger logger = TestStructuredLogger.create();
    private final Exception exception = new IllegalArgumentException("BOOM");
    private final ProducerRecord<byte[], byte[]> record =
            new ProducerRecord<>(
                    "some-topic",
                    22,
                    "key".getBytes(StandardCharsets.UTF_8),
                    "value".getBytes(StandardCharsets.UTF_8));

    private LogAndFailProductionExceptionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new LogAndFailProductionExceptionHandler(logger);
    }

    @Test
    void shouldDoNothingInConfigure() {
        handler.configure(null);
    }

    @Test
    void shouldLogErrorOnProduceFailure() {
        // Then:
        handler.handle(record, exception);

        // Then:
        assertThat(
                logger.textEntries(),
                contains(
                        "ERROR: {key=[107, 101, 121], message=Failed to produce to topic,"
                            + " partition=22, topic=some-topic} java.lang.IllegalArgumentException:"
                            + " BOOM"));
    }

    @Test
    void shouldReturnFailToStopTheAppOnProduceFailure() {
        // When:
        final ProductionExceptionHandlerResponse result = handler.handle(record, exception);

        // Then:
        assertThat(result, is(ProductionExceptionHandlerResponse.FAIL));
    }
}
