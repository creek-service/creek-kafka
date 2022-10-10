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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.creekservice.internal.kafka.streams.test.extension.handler.MatchResult.Unmatched;
import static org.creekservice.internal.kafka.streams.test.extension.handler.MismatchDescription.mismatchDescription;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.internal.kafka.streams.test.extension.handler.MatchResult.Mismatched;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
final class RecordMatcher {
    private final List<TopicRecord> expectedRecords;

    RecordMatcher(final Collection<TopicRecord> expectedRecords) {
        this.expectedRecords = List.copyOf(requireNonNull(expectedRecords, "expectedRecords"));
    }

    long minRecords() {
        return expectedRecords.size();
    }

    MatchResult match(final List<ConsumedRecord> consumedRecords) {
        final List<ConsumedRecord> matched = new ArrayList<>();
        final List<TopicRecord> remaining = new ArrayList<>(expectedRecords);
        final List<ConsumedRecord> extras = new ArrayList<>();

        for (final ConsumedRecord consumed : consumedRecords) {
            final Optional<TopicRecord> match =
                    remaining.stream()
                            .filter(
                                    expected ->
                                            recordMismatchDescription(expected, consumed).isEmpty())
                            .findFirst();

            if (match.isPresent()) {
                remaining.remove(match.get());
                matched.add(consumed);
            } else {
                extras.add(consumed);
            }
        }

        final List<Unmatched> unmatched =
                remaining.stream().map(expected -> unmatched(expected, extras)).collect(toList());

        return new MatchResult(matched, unmatched, extras);
    }

    private Unmatched unmatched(final TopicRecord expected, final List<ConsumedRecord> extras) {
        final Stream<ConsumedRecord> s =
                expected.key().isPresent()
                        ? extras.stream().sorted(matchingKeyFirst(expected.key().get()))
                        : extras.stream();

        final List<Mismatched> mismatched =
                s.map(
                                actual ->
                                        new Mismatched(
                                                actual,
                                                recordMismatchDescription(expected, actual)
                                                        .orElseThrow()))
                        .collect(toList());

        return new Unmatched(expected, mismatched);
    }

    private Comparator<? super ConsumedRecord> matchingKeyFirst(final Optional<?> expectedKey) {
        return (Comparator<ConsumedRecord>)
                (o1, o2) -> {
                    if (!Objects.equals(o1.key(), expectedKey)) {
                        return 1;
                    }
                    if (!Objects.equals(o2.key(), expectedKey)) {
                        return -1;
                    }
                    return 0;
                };
    }

    private static Optional<MismatchDescription> recordMismatchDescription(
            final TopicRecord expected, final ConsumedRecord actual) {
        if (expected.key().isProvided()) {
            final Optional<MismatchDescription> mismatch =
                    maybeMismatchDescription("key", expected.key().get(), actual.key());
            if (mismatch.isPresent()) {
                return mismatch;
            }
        }

        if (expected.value().isProvided()) {
            final Optional<MismatchDescription> mismatch =
                    maybeMismatchDescription("value", expected.value().get(), actual.value());
            if (mismatch.isPresent()) {
                return mismatch;
            }
        }

        return Optional.empty();
    }

    private static Optional<MismatchDescription> maybeMismatchDescription(
            final String path, final Optional<?> expected, final Optional<?> actual) {
        if (actual.isEmpty() && expected.isEmpty()) {
            return Optional.empty();
        }

        if (actual.isPresent() != expected.isPresent()) {
            return Optional.of(mismatchDescription(path, expected, actual));
        }

        if (Objects.equals(expected, actual)) {
            return Optional.empty();
        }

        return Optional.of(mismatchDescription(path, expected, actual));
    }
}
