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

package org.creekservice.internal.kafka.streams.test.extension.model;

import static java.lang.System.lineSeparator;
import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import java.util.Optional;
import org.creekservice.api.system.test.test.util.CreekSystemTestExtensionTester;
import org.creekservice.api.system.test.test.util.ModelParser;
import org.creekservice.internal.kafka.streams.test.extension.KafkaTestExtension;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.junit.jupiter.api.Test;

class TopicInputTest {

    private static final ModelParser PARSER = createParser();

    @Test
    void shouldParseFullyPopulated() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicInput result = PARSER.parseInput(yaml, TopicInput.class);

        // Then:
        assertThat(result.topicName(), is(Optional.of("t")));
        assertThat(result.clusterName(), is(Optional.of("c")));
        assertThat(result.notes(), is(Optional.of("n")));

        assertThat(result.records(), hasSize(1));
        assertThat(result.records().get(0).topicName(), is(Optional.of("topic-a")));
        assertThat(result.records().get(0).clusterName(), is(Optional.of("cluster-b")));
        assertThat(result.records().get(0).key(), is(Optional3.of("k")));
        assertThat(result.records().get(0).value(), is(Optional3.of("v")));
    }

    @Test
    void shouldParseWithoutFileLevelTopic() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicInput result = PARSER.parseInput(yaml, TopicInput.class);

        // Then:
        assertThat(result.topicName(), is(Optional.empty()));
        assertThat(result.records().get(0).topicName(), is(Optional.of("topic-a")));
    }

    @Test
    void shouldParseWithNullFileLevelTopic() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: ~\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicInput result = PARSER.parseInput(yaml, TopicInput.class);

        // Then:
        assertThat(result.topicName(), is(Optional.empty()));
        assertThat(result.records().get(0).topicName(), is(Optional.of("topic-a")));
    }

    @Test
    void shouldPopulateRecordTopicIfMissing() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: topic-a\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicInput result = PARSER.parseInput(yaml, TopicInput.class);

        // Then:
        assertThat(result.records().get(0).topicName(), is(Optional.of("topic-a")));
    }

    @Test
    void shouldThrowIfNothingDefinesTopicName() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "cluster: c\n"
                + "notes: n\n"
                + "records:\n"
                + "  - cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final Error e =
                assertThrows(AssertionError.class, () -> PARSER.parseInput(yaml, TopicInput.class));

        // Then:
        assertThat(e.getCause(), is(instanceOf(ValueInstantiationException.class)));
        assertThat(
                e.getCause().getMessage(),
                is(
                        "Cannot construct instance of "
                                + "`org.creekservice.internal.kafka.streams.test.extension.model.TopicInput`, "
                                + "problem: Topic not set. Topic must be supplied either at the file or record level."
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 8, column: 13]"));
    }

    @Test
    void shouldParseWithoutFileLevelCluster() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: t\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicInput result = PARSER.parseInput(yaml, TopicInput.class);

        // Then:
        assertThat(result.clusterName(), is(Optional.empty()));
        assertThat(result.records().get(0).clusterName(), is(Optional.of("cluster-b")));
    }

    @Test
    void shouldParseWithNullFileLevelCluster() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: t\n"
                + "cluster: ~\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicInput result = PARSER.parseInput(yaml, TopicInput.class);

        // Then:
        assertThat(result.clusterName(), is(Optional.empty()));
        assertThat(result.records().get(0).clusterName(), is(Optional.of("cluster-b")));
    }

    @Test
    void shouldParseToDefaultClusterName() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: t\n"
                + "notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicInput result = PARSER.parseInput(yaml, TopicInput.class);

        // Then:
        assertThat(result.clusterName(), is(Optional.empty()));
        assertThat(result.records().get(0).clusterName(), is(Optional.of(DEFAULT_CLUSTER_NAME)));
    }

    @Test
    void shouldParseWithoutNotes() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";

        // When formatting:on:
        final TopicInput result = PARSER.parseInput(yaml, TopicInput.class);

        // Then:
        assertThat(result.notes(), is(Optional.empty()));
    }

    @Test
    void shouldThrowOnNoRecords() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "cluster: c\n"
                + "records: []";

        // When formatting:on:
        final Error e =
                assertThrows(AssertionError.class, () -> PARSER.parseInput(yaml, TopicInput.class));

        // Then:
        assertThat(e.getCause(), is(instanceOf(ValueInstantiationException.class)));
        assertThat(
                e.getCause().getMessage(),
                is(
                        "Cannot construct instance of "
                                + "`org.creekservice.internal.kafka.streams.test.extension.model.TopicInput`, "
                                + "problem: At least one record is required"
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 4, column: 12]"));
    }

    @Test
    void shouldThrowOnMissingRecords() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: t\n"
                + "cluster: c";

        // When formatting:on:
        final Error e =
                assertThrows(AssertionError.class, () -> PARSER.parseInput(yaml, TopicInput.class));

        // Then:
        assertThat(e.getCause(), is(instanceOf(MismatchedInputException.class)));
        assertThat(
                e.getCause().getMessage(), startsWith("Null value for creator property 'records'"));
        assertThat(
                e.getCause().getMessage(),
                containsString("[Source: (StringReader); line: 4, column: 11]"));
    }

    @Test
    void shouldThrowOnUnknownProperty() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-topic\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "not_notes: n\n"
                + "records:\n"
                + "  - topic: topic-a\n"
                + "    cluster: cluster-b\n"
                + "    key: k\n"
                + "    value: v";
        // When formatting:on:
        final Error e =
                assertThrows(
                        AssertionError.class, () -> PARSER.parseOther(yaml, TopicRecord.class));

        // Then:
        assertThat(e.getCause(), is(instanceOf(JsonParseException.class)));
        assertThat(
                e.getCause().getMessage(),
                is(
                        "Unknown property: not_notes"
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 5, column: 10]"));
    }

    private static ModelParser createParser() {
        final CreekSystemTestExtensionTester.YamlParserBuilder builder =
                CreekSystemTestExtensionTester.extensionTester().yamlParser();
        KafkaTestExtension.initializeModel(builder.model());
        return builder.build();
    }
}
