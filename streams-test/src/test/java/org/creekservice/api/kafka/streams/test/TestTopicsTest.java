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

package org.creekservice.api.kafka.streams.test;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.apache.kafka.streams.KeyValue.pair;
import static org.creekservice.api.kafka.streams.test.util.TestServiceDescriptor.InputTopic;
import static org.creekservice.api.kafka.streams.test.util.TestServiceDescriptor.OutputTopic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtension;
import org.creekservice.api.kafka.streams.test.util.TestServiceDescriptor;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTopicsTest {

    private static CreekContext ctx;

    private TopologyTestDriver testDriver;

    @BeforeAll
    public static void classSetup() {
        ctx =
                CreekServices.builder(new TestServiceDescriptor())
                        .with(TestKafkaStreamsExtensionOptions.defaults())
                        .build();
    }

    @BeforeEach
    void setUp() {
        testDriver =
                new TopologyTestDriver(
                        topology(), ctx.extension(KafkaStreamsExtension.class).properties());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldGetInputAndOutputTopics() {
        // Given:
        final TestInputTopic<String, Long> input =
                TestTopics.inputTopic(InputTopic, ctx, testDriver);
        final TestOutputTopic<String, Long> output =
                TestTopics.outputTopic(OutputTopic, ctx, testDriver);

        // When:
        input.pipeInput("a", 1L);

        // Then:
        assertThat(output.readKeyValuesToList(), contains(pair("a", 1L)));
    }

    @Test
    void shouldGetInputWithTimeOffset() {
        // Given:
        final Instant start = Instant.now().minusSeconds(60).truncatedTo(MILLIS);
        final Duration inc = Duration.ofHours(1);

        final TestInputTopic<String, Long> input =
                TestTopics.inputTopic(InputTopic, ctx, testDriver, start, inc);
        final TestOutputTopic<String, Long> output =
                TestTopics.outputTopic(OutputTopic, ctx, testDriver);

        // When:
        input.pipeInput("a", 1L);
        input.pipeInput("b", 2L);

        // Then:
        assertThat(output.readRecord().getRecordTime(), is(start));
        assertThat(output.readRecord().getRecordTime(), is(start.plus(inc)));
    }

    private static Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(InputTopic.name(), Consumed.with(Serdes.String(), Serdes.Long()))
                .to(OutputTopic.name(), Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build(ctx.extension(KafkaStreamsExtension.class).properties());
    }
}
