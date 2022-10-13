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
import static org.creekservice.internal.kafka.streams.test.extension.model.ModelUtil.createParser;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonParseException;
import java.net.URI;
import java.util.Optional;
import org.creekservice.api.system.test.test.util.ModelParser;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicRecord.RecordBuilder;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.junit.jupiter.api.Test;

class TopicRecordTest {

    private static final ModelParser PARSER = createParser();
    private static final URI LOCATION = URI.create("some/location");

    @Test
    void shouldParseFullyPopulated() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.clusterName, is(Optional.of("c")));
        assertThat(result.topicName, is(Optional.of("t")));
        assertThat(result.key, is(Optional3.of(10)));
        assertThat(result.value, is(Optional3.of("hello")));
    }

    @Test
    void shouldIgnoreNotes() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "notes: 101.4\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.clusterName, is(Optional.of("c")));
        assertThat(result.topicName, is(Optional.of("t")));
        assertThat(result.key, is(Optional3.of(10)));
        assertThat(result.value, is(Optional3.of("hello")));
    }

    @Test
    void shouldParseWithOutTopic() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.topicName, is(Optional.empty()));
    }

    @Test
    void shouldParseWithNullTopic() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: ~\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.topicName, is(Optional.empty()));
    }

    @Test
    void shouldParseWithOutCluster() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.clusterName, is(Optional.empty()));
    }

    @Test
    void shouldParseWithNullCluster() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: ~\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.clusterName, is(Optional.empty()));
    }

    @Test
    void shouldParseWithOutKey() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "value: hello";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.key, is(Optional3.notProvided()));
    }

    @Test
    void shouldParseWithNullKey() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: ~\n"
                + "value: hello";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.key, is(Optional3.explicitlyNull()));
    }

    @Test
    void shouldParseWithOutValue() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: 10";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.value, is(Optional3.notProvided()));
    }

    @Test
    void shouldParseWithNullValue() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: ~";

        // When formatting:on:
        final RecordBuilder result = PARSER.parseOther(yaml, RecordBuilder.class);

        // Then:
        assertThat(result.value, is(Optional3.explicitlyNull()));
    }

    @Test
    void shouldThrowOnUnknownProperty() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "not_value: ~";

        // When formatting:on:
        final Exception e =
                assertThrows(
                        JsonParseException.class,
                        () -> PARSER.parseOther(yaml, RecordBuilder.class));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Unknown property: not_value"
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 5, column: 10]"));
    }

    @Test
    void shouldThrowOnEmptyTopicName() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: \"\"\n"
                + "cluster: c\n"
                + "key: 10";

        // When formatting:on:
        final Exception e =
                assertThrows(
                        JsonParseException.class,
                        () -> PARSER.parseOther(yaml, RecordBuilder.class));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Property can not be blank: topic"
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 2, column: 10]"));
    }

    @Test
    void shouldThrowOnEmptyClusterName() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: \"\t\"\n"
                + "key: 10";

        // When formatting:on:
        final Exception e =
                assertThrows(
                        JsonParseException.class,
                        () -> PARSER.parseOther(yaml, RecordBuilder.class));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Property can not be blank: cluster"
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 3, column: 13]"));
    }

    @Test
    void shouldSetDefaults() {
        // Given:
        final RecordBuilder builder =
                new RecordBuilder(
                        LOCATION,
                        Optional.empty(),
                        Optional.empty(),
                        Optional3.of("k"),
                        Optional3.of("v"));

        // When:
        final TopicRecord result = builder.build(Optional.of("c"), Optional.of("t"));

        // Then:
        assertThat(result.location(), is(LOCATION));
        assertThat(result.clusterName(), is("c"));
        assertThat(result.topicName(), is("t"));
        assertThat(result.key(), is(Optional3.of("k")));
        assertThat(result.value(), is(Optional3.of("v")));
    }

    @Test
    void shouldNotRequireDefaults() {
        // Given:
        final RecordBuilder builder =
                new RecordBuilder(
                        LOCATION,
                        Optional.of("oc"),
                        Optional.of("ot"),
                        Optional3.of("k"),
                        Optional3.of("v"));

        // When:
        final TopicRecord result = builder.build(Optional.empty(), Optional.empty());

        // Then:
        assertThat(result.clusterName(), is("oc"));
        assertThat(result.topicName(), is("ot"));
        assertThat(result.key(), is(Optional3.of("k")));
        assertThat(result.value(), is(Optional3.of("v")));
    }

    @Test
    void shouldIgnoreDefaults() {
        // Given:
        final RecordBuilder builder =
                new RecordBuilder(
                        LOCATION,
                        Optional.of("oc"),
                        Optional.of("ot"),
                        Optional3.of("k"),
                        Optional3.of("v"));

        // When:
        final TopicRecord result = builder.build(Optional.of("c"), Optional.of("t"));

        // Then:
        assertThat(result.clusterName(), is("oc"));
        assertThat(result.topicName(), is("ot"));
        assertThat(result.key(), is(Optional3.of("k")));
        assertThat(result.value(), is(Optional3.of("v")));
    }

    @Test
    void shouldThrowIfDefaultTopicNeededButNotAvailable() {
        // Given:
        final RecordBuilder builder =
                new RecordBuilder(
                        LOCATION,
                        Optional.of("c"),
                        Optional.empty(),
                        Optional3.of("k"),
                        Optional3.of("v"));

        // When:
        final Exception e =
                assertThrows(
                        RuntimeException.class,
                        () -> builder.build(Optional.of("c"), Optional.empty()));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Topic not set. Topic must be supplied either at the file or record level. location: "
                                + LOCATION));
    }

    @Test
    void shouldDefaultToDefaultCluster() {
        // Given:
        final RecordBuilder builder =
                new RecordBuilder(
                        LOCATION,
                        Optional.empty(),
                        Optional.empty(),
                        Optional3.of("k"),
                        Optional3.of("v"));

        // When:
        final TopicRecord result = builder.build(Optional.empty(), Optional.of("t"));

        // Then:
        assertThat(result.clusterName(), is(DEFAULT_CLUSTER_NAME));
        assertThat(result.topicName(), is("t"));
        assertThat(result.key(), is(Optional3.of("k")));
        assertThat(result.value(), is(Optional3.of("v")));
    }

    @Test
    void shouldUpdateWithCoercedKeyAndValue() {
        // Given:
        final TopicRecord record =
                new TopicRecord(LOCATION, "c", "t", Optional3.of("k"), Optional3.of("v"));

        // When:
        final TopicRecord result = record.with(Optional3.of("new-k"), Optional3.of("new-v"));

        // Then:
        assertThat(result.clusterName(), is("c"));
        assertThat(result.topicName(), is("t"));
        assertThat(result.key(), is(Optional3.of("new-k")));
        assertThat(result.value(), is(Optional3.of("new-v")));
    }
}
