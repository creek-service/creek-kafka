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

package org.creekservice.internal.kafka.streams.extension;

import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
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

    @Mock private KafkaClientsExtension clientExtension;
    @Mock private KafkaStreamsBuilder builder;
    @Mock private KafkaStreamsExecutor executor;
    @Mock private Properties properties;
    @Mock private Topology topology;
    @Mock private KafkaStreams app;
    @Mock private KafkaTopicDescriptor<Long, String> topicDef;
    @Mock private KafkaTopic<Long, String> topic;
    private StreamsExtension extension;

    @BeforeEach
    void setUp() {
        extension = new StreamsExtension(clientExtension, builder, executor);

        when(clientExtension.properties(any())).thenReturn(properties);
        when(builder.build(any(), any())).thenReturn(app);
    }

    @Test
    void shouldReturnName() {
        assertThat(extension.name(), is("org.creekservice.kafka.streams"));
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    @Test
    void shouldReturnProperties() {
        // When:
        final Properties result = extension.properties("cluster-bob");

        // Then:
        assertThat(result, is(properties));
        verify(clientExtension).properties("cluster-bob");
    }

    @Test
    void shouldGetTopicsFromClientExt() {
        // Given:
        when(clientExtension.topic(topicDef)).thenReturn(topic);

        // When:
        final KafkaTopic<Long, String> result = extension.topic(topicDef);

        // Then:
        assertThat(result, is(topic));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldGetProducerFromClientExt() {
        // Given:
        final Producer<byte[], byte[]> producer = mock(Producer.class);
        when(clientExtension.producer("name")).thenReturn(producer);

        // When:
        final Producer<byte[], byte[]> result = extension.producer("name");

        // Then:
        assertThat(result, is(producer));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldGetConsumerFromClientExt() {
        // Given:
        final Consumer<byte[], byte[]> consumer = mock(Consumer.class);
        when(clientExtension.consumer("name")).thenReturn(consumer);

        // When:
        final Consumer<byte[], byte[]> result = extension.consumer("name");

        // Then:
        assertThat(result, is(consumer));
    }

    @Test
    void shouldCloseClientExtOnClose() throws Exception {
        // Given:
        final Duration duration = Duration.ofMillis(1535);

        // When:
        extension.close(duration);

        // Then:
        verify(clientExtension).close(duration);
    }

    @Test
    void shouldBuildSpecificTopology() {
        // When:
        final KafkaStreams result = extension.build(topology, "cluster-bob");

        // Then:
        verify(builder).build(topology, "cluster-bob");
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
        verify(builder).build(topology, DEFAULT_CLUSTER_NAME);
        verify(executor).execute(app);
    }
}
