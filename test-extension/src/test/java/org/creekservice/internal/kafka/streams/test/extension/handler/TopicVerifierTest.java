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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.lang.System.lineSeparator;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.creekservice.internal.kafka.streams.test.extension.handler.MatchResult.Mismatched;
import org.creekservice.internal.kafka.streams.test.extension.handler.MatchResult.Unmatched;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicVerifierTest {

    private static final String TOPIC_NAME = "topic-a";
    private static final Instant START = Instant.now();
    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(465);
    private static final Duration EXTRA_TIMEOUT = Duration.ofSeconds(12);

    @Mock private TopicConsumers consumers;
    @Mock private RecordMatcher matcher;
    @Mock private Clock clock;
    @Mock private TopicConsumer consumer;
    @Mock private MatchResult result;
    @Mock private ConsumedRecord rec0;
    @Mock private ConsumedRecord rec1;
    @Mock private ConsumedRecord extra;
    private TopicVerifier verifier;

    @BeforeEach
    void setUp() {
        verifier =
                new TopicVerifier(
                        TOPIC_NAME, consumers, matcher, VERIFY_TIMEOUT, EXTRA_TIMEOUT, clock);

        when(consumers.get(any())).thenReturn(consumer);
        when(clock.instant()).thenReturn(START, START.plus(VERIFY_TIMEOUT));
        when(matcher.match(any())).thenReturn(result);
    }

    @Test
    void shouldGetTopicConsumerByName() {
        // When:
        verifier.verify();

        // Then:
        verify(consumers).get(TOPIC_NAME);
    }

    @Test
    void shouldAttemptToConsumeAtLeastRequiredNumOfRecordsWithinTimeout() {
        // Given:
        final long minRecords = 8765L;
        when(matcher.minRecords()).thenReturn(minRecords);

        // When:
        verifier.verify();

        // Then:
        verify(consumer).consume(minRecords, START.plus(VERIFY_TIMEOUT));
    }

    @Test
    void shouldDoFinalConsumeToCheckForExtraRecords() {
        // When:
        verifier.verify();

        // Then:
        verify(consumer).consume(Long.MAX_VALUE, START.plus(VERIFY_TIMEOUT).plus(EXTRA_TIMEOUT));
    }

    @Test
    void shouldInvokeMatcherWithAllConsumedRecords() {
        // Given:
        when(consumer.consume(anyLong(), any()))
                .thenReturn(List.of(rec0, rec1))
                .thenReturn(List.of(extra));

        // When:
        verifier.verify();

        // Then:
        verify(matcher).match(List.of(rec0, rec1, extra));
    }

    @Test
    void shouldThrowOnUnmatched() {
        // Given:
        final TopicRecord expected = mock(TopicRecord.class, withSettings().name("e-record"));
        final Unmatched unmatched = mock(Unmatched.class);
        final Mismatched mismatched = mock(Mismatched.class);
        when(unmatched.expected()).thenReturn(expected);
        when(unmatched.mismatches()).thenReturn(List.of(mismatched));
        when(mismatched.actual()).thenReturn(rec1);
        when(mismatched.mismatchDescription()).thenReturn("mismatch details");

        when(result.matched()).thenReturn(List.of(rec0));
        when(result.unmatched()).thenReturn(List.of(unmatched));

        // When:
        final Error e = assertThrows(AssertionError.class, () -> verifier.verify());

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "1 expected record(s) not found."
                                + lineSeparator()
                                + "Unmatched records: ["
                                + lineSeparator()
                                + "\tExpected: e-record"
                                + lineSeparator()
                                + "\tActual: ["
                                + lineSeparator()
                                + "\t\t(mismatch details) rec1"
                                + lineSeparator()
                                + "\t]"
                                + lineSeparator()
                                + "]"
                                + lineSeparator()
                                + "Matched records: ["
                                + lineSeparator()
                                + "\trec0"
                                + lineSeparator()
                                + "]"));
    }

    @Test
    void shouldThrowOnExtra() {
        // Given:
        when(result.unmatched()).thenReturn(List.of());
        when(result.extras()).thenReturn(List.of(extra));

        // When:
        final Error e = assertThrows(AssertionError.class, () -> verifier.verify());

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Additional records were produced."
                                + lineSeparator()
                                + "Unmatched records: ["
                                + lineSeparator()
                                + "\textra"
                                + lineSeparator()
                                + "]"));
    }
}
