/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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
import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.internal.kafka.streams.test.util.NativeServiceDescriptor.InputTopic;
import static org.creekservice.internal.kafka.streams.test.util.NativeServiceDescriptor.OutputTopic;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.service.context.CreekContext;
import org.creekservice.api.service.context.CreekServices;
import org.creekservice.internal.kafka.streams.test.util.NativeServiceDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestTopicsTest {

    private CreekContext ctx;
    private TopologyTestDriver testDriver;

    @BeforeEach
    void setUp() {
        ctx =
                CreekServices.builder(new NativeServiceDescriptor())
                        .with(TestKafkaStreamsExtensionOptions.defaults())
                        .build();

        try (KafkaClientsExtension ext = ctx.extension(KafkaClientsExtension.class)) {
            final Properties properties = ext.properties(DEFAULT_CLUSTER_NAME);
            testDriver = new TopologyTestDriver(topology(properties), properties);
        }
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

    private static Topology topology(final Properties properties) {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(InputTopic.name(), Consumed.with(Serdes.String(), Serdes.Long()))
                .to(OutputTopic.name(), Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build(properties);
    }
}
