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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.lang.System.lineSeparator;
import static java.util.Objects.requireNonNull;
import static org.creekservice.internal.kafka.streams.test.extension.util.ErrorMsgUtil.formatList;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler;
import org.creekservice.internal.kafka.streams.test.extension.handler.MatchResult.Unmatched;

final class TopicVerifier implements ExpectationHandler.Verifier {

    private final Clock clock;
    private final String topicName;
    private final TopicConsumers consumers;
    private final RecordMatcher matcher;
    private final Duration verifyTimeout;
    private final Duration extraRecordTimeout;

    TopicVerifier(
            final String topicName,
            final TopicConsumers consumers,
            final RecordMatcher matcher,
            final Duration verifyTimeout,
            final Duration extraRecordTimeout) {
        this(topicName, consumers, matcher, verifyTimeout, extraRecordTimeout, Clock.systemUTC());
    }

    @VisibleForTesting
    TopicVerifier(
            final String topicName,
            final TopicConsumers consumers,
            final RecordMatcher matcher,
            final Duration verifyTimeout,
            final Duration extraRecordTimeout,
            final Clock clock) {
        this.topicName = requireNonNull(topicName, "topicName");
        this.consumers = requireNonNull(consumers, "consumer");
        this.matcher = requireNonNull(matcher, "matcher");
        this.verifyTimeout = requireNonNull(verifyTimeout, "Duration");
        this.extraRecordTimeout = requireNonNull(extraRecordTimeout, "extraRecordTimeout");
        this.clock = requireNonNull(clock, "clock");
    }

    @Override
    public void verify() {
        validate(consume());
    }

    private List<ConsumedRecord> consume() {
        final TopicConsumer consumer = consumers.get(topicName);
        final List<ConsumedRecord> consumed = new ArrayList<>();
        consumed.addAll(
                consumer.consume(matcher.minRecords(), clock.instant().plus(verifyTimeout)));
        consumed.addAll(consumer.consume(Long.MAX_VALUE, clock.instant().plus(extraRecordTimeout)));
        return consumed;
    }

    private void validate(final List<ConsumedRecord> consumed) {
        final MatchResult result = matcher.match(consumed);

        if (!result.unmatched().isEmpty()) {
            throw missingRecordsError(result);
        }

        if (!result.extras().isEmpty()) {
            throw extrasError(result.extras());
        }
    }

    private static AssertionError missingRecordsError(final MatchResult result) {
        final List<String> unmatched =
                result.unmatched().stream().map(TopicVerifier::format).collect(Collectors.toList());

        return new AssertionError(
                result.unmatched().size()
                        + " expected record(s) not found."
                        + lineSeparator()
                        + "Unmatched records: "
                        + formatList(unmatched)
                        + lineSeparator()
                        + "Matched records: "
                        + formatList(result.matched()));
    }

    private static AssertionError extrasError(final List<ConsumedRecord> extras) {
        return new AssertionError(
                "Additional records were produced."
                        + lineSeparator()
                        + "Unmatched records: "
                        + formatList(extras));
    }

    private static String format(final Unmatched unmatched) {

        final List<String> mismatchReasons =
                unmatched.mismatches().stream()
                        .map(e -> "(" + e.mismatchDescription() + ") " + e.actual())
                        .collect(Collectors.toList());

        return "Expected: "
                + unmatched.expected()
                + lineSeparator()
                + "\tActual: "
                + formatList(mismatchReasons, 1);
    }
}
