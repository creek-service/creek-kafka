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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.Response;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.Result;
import org.creekservice.api.kafka.streams.extension.exception.StreamsExceptionHandlers.LogAndFailProductionExceptionHandler;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamsExceptionHandlersTest {

    private static final ProducerRecord<byte[], byte[]> RECORD =
            new ProducerRecord<>(
                    "destination-topic",
                    22,
                    "destination-key".getBytes(UTF_8),
                    "destination-value".getBytes(UTF_8));
    private static final Exception EXCEPTION = new IllegalArgumentException("BOOM");

    private final TestStructuredLogger logger = TestStructuredLogger.create();

    @Mock(strictness = LENIENT)
    private ErrorHandlerContext context;

    private LogAndFailProductionExceptionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new LogAndFailProductionExceptionHandler(logger);

        when(context.topic()).thenReturn("source-topic");
        when(context.partition()).thenReturn(11);
        when(context.sourceRawKey()).thenReturn("source-key".getBytes(UTF_8));
    }

    @Test
    void shouldDoNothingInConfigure() {
        handler.configure(null);
    }

    @Test
    void shouldLogErrorOnProduceFailure() {
        // Then:
        handler.handleError(context, RECORD, EXCEPTION);

        // Then:
        assertThat(
                logger.textEntries(),
                contains(
                        """
ERROR: {\
input-key=[115, 111, 117, 114, 99, 101, 45, 107, 101, 121], \
input-partition=11, \
input-topic=source-topic, \
message=Failed to produce to topic, \
output-key=[100, 101, 115, 116, 105, 110, 97, 116, 105, 111, 110, 45, 107, 101, 121], \
output-partition=22, \
output-topic=destination-topic} \
java.lang.IllegalArgumentException: BOOM\
"""));
    }

    @Test
    void shouldReturnFailToStopTheAppOnProduceFailure() {
        // When:
        final Response result = handler.handleError(context, RECORD, EXCEPTION);

        // Then:
        assertThat(result.result(), is(Result.FAIL));
        assertThat(result.deadLetterQueueRecords(), is(empty()));
    }

    @Test
    void shouldLogErrorOnSerializationFailure() {
        // Then:
        handler.handleSerializationError(
                context,
                RECORD,
                EXCEPTION,
                ProductionExceptionHandler.SerializationExceptionOrigin.KEY);

        // Then:
        assertThat(
                logger.textEntries(),
                contains(
                        """
ERROR: {\
input-key=[115, 111, 117, 114, 99, 101, 45, 107, 101, 121], \
input-partition=11, \
input-topic=source-topic, \
message=Failed to serialize, \
origin=KEY, \
output-key=[100, 101, 115, 116, 105, 110, 97, 116, 105, 111, 110, 45, 107, 101, 121], \
output-partition=22, \
output-topic=destination-topic} \
java.lang.IllegalArgumentException: BOOM\
"""));
    }

    @Test
    void shouldReturnFailToStopTheAppOnSerializationFailure() {
        // When:
        final Response result =
                handler.handleSerializationError(
                        context,
                        RECORD,
                        EXCEPTION,
                        ProductionExceptionHandler.SerializationExceptionOrigin.VALUE);

        // Then:
        assertThat(result.result(), is(Result.FAIL));
        assertThat(result.deadLetterQueueRecords(), is(empty()));
    }
}
