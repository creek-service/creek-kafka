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

import static java.util.Objects.requireNonNull;

import java.util.List;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;

final class MatchResult {
    private final List<ConsumedRecord> matched;
    private final List<Unmatched> unmatched;
    private final List<ConsumedRecord> extras;

    MatchResult(
            final List<ConsumedRecord> matched,
            final List<Unmatched> unmatched,
            final List<ConsumedRecord> extras) {
        this.matched = List.copyOf(requireNonNull(matched, "matched"));
        this.unmatched = List.copyOf(requireNonNull(unmatched, "unmatched"));
        this.extras = List.copyOf(requireNonNull(extras, "extras"));
    }

    List<ConsumedRecord> matched() {
        return matched;
    }

    List<Unmatched> unmatched() {
        return unmatched;
    }

    List<ConsumedRecord> extras() {
        return extras;
    }

    static final class Unmatched {

        private final TopicRecord expected;
        private final List<Mismatched> mismatches;

        Unmatched(final TopicRecord expected, final List<Mismatched> mismatches) {
            this.expected = requireNonNull(expected, "expected");
            this.mismatches = List.copyOf(requireNonNull(mismatches, "mismatched"));
        }

        TopicRecord expected() {
            return expected;
        }

        List<Mismatched> mismatches() {
            return mismatches;
        }
    }

    static final class Mismatched {

        private final ConsumedRecord actual;
        private final String mismatchDescription;

        Mismatched(final ConsumedRecord actual, final String mismatchDescription) {
            this.actual = requireNonNull(actual, "actual");
            this.mismatchDescription = requireNonNull(mismatchDescription, "mismatchDescription");
        }

        public ConsumedRecord actual() {
            return actual;
        }

        public String mismatchDescription() {
            return mismatchDescription;
        }
    }
}
