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

import static org.creekservice.internal.kafka.streams.test.extension.model.ModelUtil.createParser;
import static org.creekservice.internal.kafka.streams.test.extension.model.TestOptions.DEFAULT_EXTRA_TIMEOUT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.creekservice.api.system.test.test.util.ModelParser;
import org.creekservice.internal.kafka.streams.test.extension.model.TestOptions.OutputOrdering;
import org.junit.jupiter.api.Test;

class TestOptionsTest {

    private static final ModelParser PARSER = createParser();

    @Test
    void shouldParseMinimal() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-options@1\n"
                + "notes: ignored\n";

        // When formatting:on:
        final TestOptions result = PARSER.parseOther(yaml, TestOptions.class);

        // Then:
        assertThat(result.outputOrdering(), is(OutputOrdering.BY_KEY));
        assertThat(result.verifierTimeout(), is(Optional.empty()));
        assertThat(result.extraTimeout(), is(DEFAULT_EXTRA_TIMEOUT));
    }

    @Test
    void shouldParseFullyPopulated() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-options@1\n"
                + "outputOrdering: NONE\n"
                + "verifierTimeout: PT49.1S\n"
                + "extraTimeout: PT2S\n"
                + "notes: ignored";

        // When formatting:on:
        final TestOptions result = PARSER.parseOther(yaml, TestOptions.class);

        // Then:
        assertThat(result.outputOrdering(), is(OutputOrdering.NONE));
        assertThat(result.verifierTimeout(), is(Optional.of(Duration.ofSeconds(49, 100000000))));
        assertThat(result.extraTimeout(), is(Duration.ofSeconds(2)));
    }

    @Test
    void shouldParseDurationsAsSeconds() throws Exception {
        // Given formatting:off:
        final String yaml = "---\n"
                + "!creek/kafka-options@1\n"
                + "outputOrdering: NONE\n"
                + "verifierTimeout: 120\n"
                + "extraTimeout: 2\n"
                + "notes: ignored";

        // When formatting:on:
        final TestOptions result = PARSER.parseOther(yaml, TestOptions.class);

        // Then:
        assertThat(result.outputOrdering(), is(OutputOrdering.NONE));
        assertThat(result.verifierTimeout(), is(Optional.of(Duration.ofSeconds(120))));
        assertThat(result.extraTimeout(), is(Duration.ofSeconds(2)));
    }

    @Test
    void shouldSetLocation() {
        // Given:
        final URI location = URI.create("loc:///1");

        final TestOptions initial =
                new TestOptions(
                        Optional.of(OutputOrdering.NONE),
                        Optional.of(Duration.ofSeconds(1)),
                        Optional.of(Duration.ofSeconds(2)),
                        Optional.empty());

        // When:
        final TestOptions result = initial.withLocation(location);

        // Then:
        assertThat(result.location(), is(location));
        assertThat(result.outputOrdering(), is(OutputOrdering.NONE));
        assertThat(result.verifierTimeout(), is(Optional.of(Duration.ofSeconds(1))));
        assertThat(result.extraTimeout(), is(Duration.ofSeconds(2)));
    }
}
