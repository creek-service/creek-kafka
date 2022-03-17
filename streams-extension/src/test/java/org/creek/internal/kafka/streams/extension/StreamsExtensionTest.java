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

package org.creek.internal.kafka.streams.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.creek.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StreamsExtensionTest {

    @Mock private KafkaStreamsExtensionOptions options;
    @Mock private KafkaStreamsBuilder builder;
    @Mock private KafkaStreamsExecutor executor;
    @Mock private Properties properties;
    @Mock private Topology topology;
    @Mock private KafkaStreams app;
    private StreamsExtension extension;

    @BeforeEach
    void setUp() {
        extension = new StreamsExtension(options, builder, executor);

        when(options.properties()).thenReturn(properties);
        when(builder.build(any())).thenReturn(app);
    }

    @Test
    void shouldReturnName() {
        assertThat(extension.name(), is("Kafka-streams"));
    }

    @Test
    void shouldReturnProperties() {
        assertThat(extension.properties(), is(properties));
    }

    @Test
    void shouldBuildTopology() {
        // When:
        final KafkaStreams result = extension.build(topology);

        // Then:
        verify(builder).build(topology);
        assertThat(result, is(app));
    }

    @Test
    void shouldExecuteApp() {
        // When:
        extension.execute(app);

        // Then:
        verify(executor).execute(app);
    }

    @Test
    void shouldBuildAndExecute() {
        // When:
        extension.execute(topology);

        // Then:
        verify(builder).build(topology);
        verify(executor).execute(app);
    }
}
