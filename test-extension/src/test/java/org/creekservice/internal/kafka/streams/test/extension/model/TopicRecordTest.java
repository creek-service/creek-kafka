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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonParseException;
import java.util.Optional;
import org.creekservice.api.system.test.test.util.CreekSystemTestExtensionTester;
import org.creekservice.api.system.test.test.util.ModelParser;
import org.creekservice.internal.kafka.streams.test.extension.util.Optional3;
import org.junit.jupiter.api.Test;

class TopicRecordTest {

    private static final ModelParser PARSER =
            CreekSystemTestExtensionTester.extensionTester().yamlParser().build();

    @Test
    void shouldParseFullyPopulated() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.topicName(), is(Optional.of("t")));
        assertThat(result.clusterName(), is(Optional.of("c")));
        assertThat(result.key(), is(Optional3.of(10)));
        assertThat(result.value(), is(Optional3.of("hello")));
    }

    @Test
    void shouldParseWithOutTopic() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.topicName(), is(Optional.empty()));
    }

    @Test
    void shouldParseWithNullTopic() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: ~\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.topicName(), is(Optional.empty()));
    }

    @Test
    void shouldParseWithOutCluster() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.clusterName(), is(Optional.empty()));
    }

    @Test
    void shouldParseWithNullCluster() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: ~\n"
                + "key: 10\n"
                + "value: hello";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.clusterName(), is(Optional.empty()));
    }

    @Test
    void shouldParseWithOutKey() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "value: hello";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.key(), is(Optional3.notProvided()));
    }

    @Test
    void shouldParseWithNullKey() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: ~\n"
                + "value: hello";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.key(), is(Optional3.explicitlyNull()));
    }

    @Test
    void shouldParseWithOutValue() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: 10";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.value(), is(Optional3.notProvided()));
    }

    @Test
    void shouldParseWithNullValue() {
        // Given formatting:off:
        final String yaml = "---\n"
                + "topic: t\n"
                + "cluster: c\n"
                + "key: 10\n"
                + "value: ~";

        // When formatting:on:
        final TopicRecord result = PARSER.parseOther(yaml, TopicRecord.class);

        // Then:
        assertThat(result.value(), is(Optional3.explicitlyNull()));
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
        final Error e =
                assertThrows(
                        AssertionError.class, () -> PARSER.parseOther(yaml, TopicRecord.class));

        // Then:
        assertThat(e.getCause(), is(instanceOf(JsonParseException.class)));
        assertThat(
                e.getCause().getMessage(),
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
        final Error e =
                assertThrows(
                        AssertionError.class, () -> PARSER.parseOther(yaml, TopicRecord.class));

        // Then:
        assertThat(e.getCause(), is(instanceOf(JsonParseException.class)));
        assertThat(
                e.getCause().getMessage(),
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
        final Error e =
                assertThrows(
                        AssertionError.class, () -> PARSER.parseOther(yaml, TopicRecord.class));

        // Then:
        assertThat(e.getCause(), is(instanceOf(JsonParseException.class)));
        assertThat(
                e.getCause().getMessage(),
                is(
                        "Property can not be blank: cluster"
                                + lineSeparator()
                                + " at [Source: (StringReader); line: 3, column: 13]"));
    }

    @Test
    void shouldSetTopicName() {
        // Given:
        final TopicRecord initial =
                new TopicRecord(
                        Optional.of("t"), Optional.of("c"), Optional3.of("k"), Optional3.of("v"));

        // When:
        final TopicRecord result = initial.withTopicName("new");

        // Then:
        assertThat(result.topicName(), is(Optional.of("new")));
        assertThat(result.clusterName(), is(Optional.of("c")));
        assertThat(result.key(), is(Optional3.of("k")));
        assertThat(result.value(), is(Optional3.of("v")));

        assertThat(initial.topicName(), is(Optional.of("t")));
    }

    @Test
    void shouldSetClusterName() {
        // Given:
        final TopicRecord initial =
                new TopicRecord(
                        Optional.of("t"), Optional.of("c"), Optional3.of("k"), Optional3.of("v"));

        // When:
        final TopicRecord result = initial.withClusterName("new");

        // Then:
        assertThat(result.topicName(), is(Optional.of("t")));
        assertThat(result.clusterName(), is(Optional.of("new")));
        assertThat(result.key(), is(Optional3.of("k")));
        assertThat(result.value(), is(Optional3.of("v")));

        assertThat(initial.topicName(), is(Optional.of("t")));
    }
}
