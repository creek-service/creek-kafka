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

package org.creekservice.internal.kafka.streams.test.extension.model;

import static java.lang.System.lineSeparator;
import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.internal.kafka.streams.test.extension.model.ModelUtil.createParser;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import org.creekservice.api.system.test.test.util.ModelParser;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.junit.jupiter.api.Test;

class TopicExpectationTest {

    private static final ModelParser PARSER = createParser();

    @Test
    void shouldParseFullyPopulated() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records(), hasSize(1));
        assertThat(result.records().get(0).topicName(), is("topic-a"));
        assertThat(result.records().get(0).clusterName(), is("cluster-b"));
        assertThat(result.records().get(0).key(), is(Optional3.of("k")));
        assertThat(result.records().get(0).value(), is(Optional3.of("v")));
    }

    @Test
    void shouldParseWithoutFileLevelTopic() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records().get(0).topicName(), is("topic-a"));
    }

    @Test
    void shouldParseWithNullFileLevelTopic() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: ~\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records().get(0).topicName(), is("topic-a"));
    }

    @Test
    void shouldPopulateRecordTopicIfMissing() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: topic-a\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records().get(0).topicName(), is("topic-a"));
    }

    @Test
    void shouldThrowIfNothingDefinesTopicName() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final Exception e =
                assertThrows(
                        ValueInstantiationException.class,
                        () -> PARSER.parseExpectation(yaml, TopicExpectation.class));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Cannot construct instance of "
                                + "`org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation`, "
                                + "problem: Topic not set. Topic must be supplied either at the file or record level. "
                                + "location: unknown"
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 8, column: 13]"));
    }

    @Test
    void shouldParseWithoutFileLevelCluster() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: t\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records().get(0).clusterName(), is("cluster-b"));
    }

    @Test
    void shouldParseWithNullFileLevelCluster() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: t\n"
                + "cluster: ~\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records().get(0).clusterName(), is("cluster-b"));
    }

    @Test
    void shouldParseToDefaultClusterName() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: t\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records().get(0).clusterName(), is(DEFAULT_CLUSTER_NAME));
    }

    @Test
    void shouldParseWithoutNotes() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result, is(notNullValue()));
    }

    @Test
    void shouldThrowOnNoRecords() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "cluster: c\n"
                + "records: []";

        // When formatting:on:
        final Exception e =
                assertThrows(
                        ValueInstantiationException.class,
                        () -> PARSER.parseExpectation(yaml, TopicExpectation.class));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Cannot construct instance of "
                                + "`org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation`, "
                                + "problem: At least one record is required"
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 4, column: 12]"));
    }

    @Test
    void shouldThrowOnMissingRecords() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: t\n"
                + "cluster: c";

        // When formatting:on:
        final Exception e =
                assertThrows(
                        MismatchedInputException.class,
                        () -> PARSER.parseExpectation(yaml, TopicExpectation.class));

        // Then:
        assertThat(e.getMessage(), startsWith("Null value for creator property 'records'"));
        assertThat(e.getMessage(), containsString("[Source: (StringReader); line: 4, column: 11]"));
    }

    @Test
    void shouldThrowOnUnknownProperty() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "not_notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";
        // Then formatting:on:
        assertThrows(
                UnrecognizedPropertyException.class,
                () -> PARSER.parseOther(yaml, TopicExpectation.class));
    }

    @Test
    void shouldParseWithoutRecordKey() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records(), hasSize(1));
        assertThat(result.records().get(0).key(), is(Optional3.notProvided()));
    }

    @Test
    void shouldParseWithoutRecordValue() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    key: k";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records(), hasSize(1));
        assertThat(result.records().get(0).value(), is(Optional3.notProvided()));
    }

    @Test
    void shouldParseWithExplicitNullKey() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    key: ~\n"
                + "    value: v";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records(), hasSize(1));
        assertThat(result.records().get(0).key(), is(Optional3.explicitlyNull()));
    }

    @Test
    void shouldParseWithExplicitNullValue() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic@1\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    key: k\n"
                + "    value: ~";

        // When formatting:on:
        final TopicExpectation result = PARSER.parseExpectation(yaml, TopicExpectation.class);

        // Then:
        assertThat(result.records(), hasSize(1));
        assertThat(result.records().get(0).value(), is(Optional3.explicitlyNull()));
    }
}
