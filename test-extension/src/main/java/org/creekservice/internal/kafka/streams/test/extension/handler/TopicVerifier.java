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

package org.creekservice.internal.kafka.streams.test.extension.handler;

import static java.lang.System.lineSeparator;
import static java.util.Objects.requireNonNull;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler;
import org.creekservice.api.system.test.extension.test.model.ExpectationHandler.ExpectationOptions;
import org.creekservice.internal.kafka.streams.test.extension.handler.MatchResult.Unmatched;

final class TopicVerifier implements ExpectationHandler.Verifier {
    private static final Duration EXTRA_RECORD_TIMEOUT = Duration.ofSeconds(1);

    private final Clock clock;
    private final String topicName;
    private final TopicConsumers consumers;
    private final RecordMatcher matcher;
    private final ExpectationOptions options;

    TopicVerifier(
            final String topicName,
            final TopicConsumers consumers,
            final RecordMatcher matcher,
            final ExpectationOptions options) {
        this(topicName, consumers, matcher, options, Clock.systemUTC());
    }

    @VisibleForTesting
    TopicVerifier(
            final String topicName,
            final TopicConsumers consumers,
            final RecordMatcher matcher,
            final ExpectationOptions options,
            final Clock clock) {
        this.topicName = requireNonNull(topicName, "topicName");
        this.consumers = requireNonNull(consumers, "consumer");
        this.matcher = requireNonNull(matcher, "matcher");
        this.options = requireNonNull(options, "options");
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
                consumer.consume(matcher.minRecords(), clock.instant().plus(options.timeout())));
        consumed.addAll(
                consumer.consume(Long.MAX_VALUE, clock.instant().plus(EXTRA_RECORD_TIMEOUT)));
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
                        + formattedArray(unmatched)
                        + lineSeparator()
                        + "Matched records: "
                        + formattedArray(result.matched()));
    }

    private static AssertionError extrasError(final List<ConsumedRecord> extras) {
        return new AssertionError(
                "Additional records were produced."
                        + lineSeparator()
                        + "Unmatched records: "
                        + formattedArray(extras));
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
                + formattedArray(mismatchReasons, 2);
    }

    static String formattedArray(final List<?> list) {
        return formattedArray(list, 1);
    }

    private static String formattedArray(final List<?> list, final int indentLevel) {
        final String indent = "\t".repeat(indentLevel);
        final String finalIndent = "\t".repeat(indentLevel - 1);
        return list.stream()
                .map(Objects::toString)
                .map(s -> lineSeparator() + indent + s)
                .collect(Collectors.joining(",", "[", lineSeparator() + finalIndent + "]"));
    }
}
