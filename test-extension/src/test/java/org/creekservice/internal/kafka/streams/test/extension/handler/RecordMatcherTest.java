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

import static org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions.OutputOrdering.BY_KEY;
import static org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions.OutputOrdering.NONE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.creekservice.internal.kafka.streams.test.extension.handler.MatchResult.Mismatched;
import org.creekservice.internal.kafka.streams.test.extension.handler.MatchResult.Unmatched;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions.OutputOrdering;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@ExtendWith(MockitoExtension.class)
class RecordMatcherTest {

    private static final String CLUSTER_NAME = "cluster-z";
    private static final String TOPIC_NAME = "topic-a";

    @Mock private ConsumerRecord<byte[], byte[]> consumerRecord;
    private final AtomicInteger expectedCounter = new AtomicInteger();
    private List<TopicRecord> expected;
    private RecordMatcher matcher;

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldMatchInOrder(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(1, "a"), expectedRecord(1, "b"), expectedRecord(2, "c")),
                outputOrdering);

        final List<ConsumedRecord> actual =
                List.of(actualRecord(1, "a"), actualRecord(1, "b"), actualRecord(2, "c"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(empty()));
    }

    @Test
    void shouldMatchOutOfOrderIfNoOrderingRequested() {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(1, "a"), expectedRecord(1, "b"), expectedRecord(2, "c")),
                NONE);

        final List<ConsumedRecord> actual =
                List.of(actualRecord(1, "b"), actualRecord(2, "c"), actualRecord(1, "a"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(empty()));
    }

    @Test
    void shouldNotMatchOutOfOrderIfOrderingByKeyRequested() {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(1, "a"), expectedRecord(1, "b"), expectedRecord(2, "c")),
                BY_KEY);

        final List<ConsumedRecord> actual =
                List.of(actualRecord(1, "b"), actualRecord(2, "c"), actualRecord(1, "a"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(List.of(actual.get(0), actual.get(1))));
        assertThat(
                result.unmatched(),
                contains(
                        unmatched(
                                expected.get(0),
                                contains(
                                        mismatched(
                                                actual.get(2),
                                                "Records match, but the order is wrong")))));
        assertThat(result.extras(), is(List.of(actual.get(2))));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldMatchWithNoKeyExpectation(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(Optional3.notProvided(), Optional3.of("a"))),
                outputOrdering);

        final List<ConsumedRecord> actual = List.of(actualRecord(1, "a"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(empty()));
    }

    @Test
    void shouldNotEnforceOrderingWhereNoKeyProvided() {
        // Given:
        givenMatcherFor(
                List.of(
                        expectedRecord(Optional3.notProvided(), Optional3.of("a")),
                        expectedRecord(Optional3.notProvided(), Optional3.of("b"))),
                BY_KEY);

        final List<ConsumedRecord> actual = List.of(actualRecord(1, "b"), actualRecord(2, "a"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(empty()));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldMatchWithNullKeyExpectation(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(Optional3.explicitlyNull(), Optional3.of("a"))),
                outputOrdering);

        final List<ConsumedRecord> actual = List.of(actualRecord(null, "a"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(empty()));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldNotMatchWithNullKeyExpectation(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(Optional3.explicitlyNull(), Optional3.of("a"))),
                outputOrdering);

        final List<ConsumedRecord> actual = List.of(actualRecord("not null", "a"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(empty()));
        assertThat(
                result.unmatched(),
                contains(
                        unmatched(
                                expected.get(0),
                                contains(
                                        mismatched(
                                                actual.get(0),
                                                "Mismatch@key@char0, expected: <empty>, actual:"
                                                        + " String(not null)")))));
        assertThat(result.extras(), is(actual));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldMatchWithNoValueExpectation(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(Optional3.of(1), Optional3.notProvided())), outputOrdering);

        final List<ConsumedRecord> actual = List.of(actualRecord(1, "a"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(empty()));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldMatchWithNullValueExpectation(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(Optional3.of(1), Optional3.explicitlyNull())),
                outputOrdering);

        final List<ConsumedRecord> actual = List.of(actualRecord(1, null));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(empty()));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldNotMatchWithNullValueExpectation(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(
                List.of(expectedRecord(Optional3.of(1), Optional3.explicitlyNull())),
                outputOrdering);

        final List<ConsumedRecord> actual = List.of(actualRecord(1, "not null"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(empty()));
        assertThat(
                result.unmatched(),
                contains(
                        unmatched(
                                expected.get(0),
                                contains(
                                        mismatched(
                                                actual.get(0),
                                                "Mismatch@value@char0, expected: <empty>, actual:"
                                                        + " String(not null)")))));
        assertThat(result.extras(), is(actual));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldMatchDuplicates(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(List.of(expectedRecord(1, "a"), expectedRecord(1, "a")), outputOrdering);

        final List<ConsumedRecord> actual = List.of(actualRecord(1, "a"), actualRecord(1, "a"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(empty()));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldDetectExtra(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(List.of(expectedRecord(1, "a"), expectedRecord(2, "b")), outputOrdering);

        final List<ConsumedRecord> actual =
                List.of(
                        actualRecord(2, "a"),
                        actualRecord(1, "a"),
                        actualRecord(2, "b"),
                        actualRecord(2, "b"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(actual.subList(1, 3)));
        assertThat(result.unmatched(), is(empty()));
        assertThat(result.extras(), is(List.of(actual.get(0), actual.get(3))));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldDetectUnmatchedWithWithMatchingKeyAtTop(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(List.of(expectedRecord(1, "a"), expectedRecord(2, "b")), outputOrdering);

        final List<ConsumedRecord> actual =
                List.of(
                        actualRecord(2, "a"),
                        actualRecord(1, "b"),
                        actualRecord("c", 3),
                        actualRecord(2, "c"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(empty()));
        assertThat(
                result.unmatched(),
                contains(
                        unmatched(
                                expected.get(0),
                                contains(
                                        mismatched(
                                                actual.get(1),
                                                "Mismatch@value@char7, expected: String(a), actual:"
                                                        + " String(b)"),
                                        mismatched(
                                                actual.get(0),
                                                "Mismatch@key@char8, expected: Integer(1), actual:"
                                                        + " Integer(2)"),
                                        mismatched(
                                                actual.get(2),
                                                "Mismatch@key@char0, expected: Integer(1), actual:"
                                                        + " String(c)"),
                                        mismatched(
                                                actual.get(3),
                                                "Mismatch@key@char8, expected: Integer(1), actual:"
                                                        + " Integer(2)"))),
                        unmatched(
                                expected.get(1),
                                contains(
                                        mismatched(
                                                actual.get(0),
                                                "Mismatch@value@char7, expected: String(b), actual:"
                                                        + " String(a)"),
                                        mismatched(
                                                actual.get(3),
                                                "Mismatch@value@char7, expected: String(b), actual:"
                                                        + " String(c)"),
                                        mismatched(
                                                actual.get(1),
                                                "Mismatch@key@char8, expected: Integer(2), actual:"
                                                        + " Integer(1)"),
                                        mismatched(
                                                actual.get(2),
                                                "Mismatch@key@char0, expected: Integer(2), actual:"
                                                        + " String(c)")))));
        assertThat(result.extras(), is(actual));
    }

    @ParameterizedTest
    @EnumSource(OutputOrdering.class)
    void shouldDetectUnmatchedWithNoKeyAndValueExpectation(final OutputOrdering outputOrdering) {
        // Given:
        givenMatcherFor(
                List.of(
                        expectedRecord(Optional3.of(1), Optional3.notProvided()),
                        expectedRecord(Optional3.of(2), Optional3.explicitlyNull()),
                        expectedRecord(Optional3.notProvided(), Optional3.of("a")),
                        expectedRecord(Optional3.explicitlyNull(), Optional3.of("a"))),
                outputOrdering);

        final List<ConsumedRecord> actual = List.of(actualRecord(2, "c"));

        // When:
        final MatchResult result = matcher.match(actual);

        // Then:
        assertThat(result.matched(), is(empty()));
        assertThat(
                result.unmatched(),
                contains(
                        unmatched(
                                expected.get(0),
                                contains(
                                        mismatched(
                                                actual.get(0),
                                                "Mismatch@key@char8, expected: Integer(1), actual:"
                                                        + " Integer(2)"))),
                        unmatched(
                                expected.get(1),
                                contains(
                                        mismatched(
                                                actual.get(0),
                                                "Mismatch@value@char0, expected: <empty>, actual:"
                                                        + " String(c)"))),
                        unmatched(
                                expected.get(2),
                                contains(
                                        mismatched(
                                                actual.get(0),
                                                "Mismatch@value@char7, expected: String(a), actual:"
                                                        + " String(c)"))),
                        unmatched(
                                expected.get(3),
                                contains(
                                        mismatched(
                                                actual.get(0),
                                                "Mismatch@key@char0, expected: <empty>, actual:"
                                                        + " Integer(2)")))));
        assertThat(result.extras(), is(actual));
    }

    private void givenMatcherFor(final List<TopicRecord> expected, final OutputOrdering ordering) {
        this.expected = expected;
        this.matcher = new RecordMatcher(this.expected, ordering);
    }

    private TopicRecord expectedRecord(final Object key, final Object value) {
        return expectedRecord(Optional3.ofNullable(key), Optional3.ofNullable(value));
    }

    private TopicRecord expectedRecord(final Optional3<Object> key, final Optional3<Object> value) {
        return new TopicRecord(
                URI.create("topic:///" + expectedCounter.incrementAndGet()),
                CLUSTER_NAME,
                TOPIC_NAME,
                key,
                value);
    }

    private ConsumedRecord actualRecord(final Object key, final Object value) {
        return actualRecord(Optional.ofNullable(key), Optional.ofNullable(value));
    }

    private ConsumedRecord actualRecord(final Optional<?> key, final Optional<?> value) {
        return new ConsumedRecord(consumerRecord, key, value);
    }

    private Matcher<? super Unmatched> unmatched(
            final TopicRecord topicRecord,
            final Matcher<Iterable<? extends Mismatched>> mismatched) {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(
                    final Unmatched item, final Description mismatchDescription) {
                if (item.expected() != topicRecord) {
                    mismatchDescription
                            .appendText("wrong expected record. expected: ")
                            .appendValue(topicRecord)
                            .appendText(",\nbut got: ")
                            .appendValue(item.expected());
                    return false;
                }

                if (!mismatched.matches(item.mismatches())) {
                    mismatchDescription.appendText("wrong mismatched. ");
                    mismatched.describeMismatch(item.mismatches(), mismatchDescription);
                    return false;
                }

                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description
                        .appendText("Unmatched with topic record: ")
                        .appendValue(topicRecord)
                        .appendText(", and mismatched: ")
                        .appendDescriptionOf(mismatched);
            }
        };
    }

    private Matcher<? super Mismatched> mismatched(
            final ConsumedRecord consumedRecord, final String reason) {
        return new TypeSafeDiagnosingMatcher<>() {
            @Override
            protected boolean matchesSafely(
                    final Mismatched item, final Description mismatchDescription) {
                if (item.actual() != consumedRecord) {
                    mismatchDescription
                            .appendText("wrong actual record. expected: ")
                            .appendValue(consumedRecord)
                            .appendText(",\nbut got: ")
                            .appendValue(item.actual());
                    return false;
                }

                if (!item.mismatchDescription().equals(reason)) {
                    mismatchDescription
                            .appendText("wrong mismatch description. expected: ")
                            .appendValue(reason)
                            .appendText(",\nbut got: ")
                            .appendValue(item.mismatchDescription());
                    return false;
                }

                return true;
            }

            @Override
            public void describeTo(final Description description) {
                description
                        .appendText("Mismatched with consumed record: ")
                        .appendValue(consumedRecord)
                        .appendText(", and mismatched reason: ")
                        .appendValue(reason);
            }
        };
    }
}
