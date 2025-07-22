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

package org.creekservice.internal.kafka.streams.test.extension.model;

import static org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions.DEFAULT_EXTRA_TIMEOUT;
import static org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions.DEFAULT_KAFKA_DOCKER_IMAGE;
import static org.creekservice.internal.kafka.streams.test.extension.model.ModelUtil.createParser;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.creekservice.api.system.test.test.util.ModelParser;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions.OutputOrdering;
import org.junit.jupiter.api.Test;

class TestOptionsKafka {

    private static final ModelParser PARSER = createParser();

    @Test
    void shouldParseMinimal() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-options@1\n"
                + "notes: ignored\n";

        // When formatting:on:
        final KafkaOptions result = PARSER.parseOther(yaml, KafkaOptions.class);

        // Then:
        assertThat(result.outputOrdering(), is(OutputOrdering.BY_KEY));
        assertThat(result.verifierTimeout(), is(Optional.empty()));
        assertThat(result.extraTimeout(), is(DEFAULT_EXTRA_TIMEOUT));
        assertThat(result.kafkaDockerImage(), is(DEFAULT_KAFKA_DOCKER_IMAGE));
    }

    @Test
    void shouldParseFullyPopulated() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-options@1\n"
                + "outputOrdering: NONE\n"
                + "verifierTimeout: PT49.1S\n"
                + "extraTimeout: PT2S\n"
                + "kafkaDockerImage: custom-image\n"
                + "notes: ignored";

        // When formatting:on:
        final KafkaOptions result = PARSER.parseOther(yaml, KafkaOptions.class);

        // Then:
        assertThat(result.outputOrdering(), is(OutputOrdering.NONE));
        assertThat(result.verifierTimeout(), is(Optional.of(Duration.ofSeconds(49, 100000000))));
        assertThat(result.extraTimeout(), is(Duration.ofSeconds(2)));
        assertThat(result.kafkaDockerImage(), is("custom-image"));
    }

    @Test
    void shouldParseDurationsAsSeconds() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-options@1\n"
                + "verifierTimeout: 120\n"
                + "extraTimeout: 2";

        // When formatting:on:
        final KafkaOptions result = PARSER.parseOther(yaml, KafkaOptions.class);

        // Then:
        assertThat(result.verifierTimeout(), is(Optional.of(Duration.ofSeconds(120))));
        assertThat(result.extraTimeout(), is(Duration.ofSeconds(2)));
    }

    @Test
    void shouldSetLocation() {
        // Given:
        final URI location = URI.create("loc:///1");

        final KafkaOptions initial =
                new KafkaOptions(
                        Optional.of(OutputOrdering.NONE),
                        Optional.of(Duration.ofSeconds(1)),
                        Optional.of(Duration.ofSeconds(2)),
                        Optional.of("kafka:6.2"),
                        Optional.empty());

        // When:
        final KafkaOptions result = initial.withLocation(location);

        // Then:
        assertThat(result.location(), is(location));
        assertThat(result.outputOrdering(), is(OutputOrdering.NONE));
        assertThat(result.verifierTimeout(), is(Optional.of(Duration.ofSeconds(1))));
        assertThat(result.extraTimeout(), is(Duration.ofSeconds(2)));
        assertThat(result.kafkaDockerImage(), is("kafka:6.2"));
    }
}
