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

package org.creekservice.internal.kafka.streams.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaStreamsBuilderTest {

    @Mock private KafkaStreamsBuilder.AppFactory appFactory;
    @Mock private KafkaStreams app;
    @Mock private KafkaClientsExtension clientsExtension;
    @Mock private Properties properties;
    @Mock private Topology topology;
    private KafkaStreamsBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new KafkaStreamsBuilder(clientsExtension, appFactory);

        when(appFactory.create(any(), any(), any())).thenReturn(app);
        when(clientsExtension.properties(any())).thenReturn(properties);
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    @Test
    void shouldGetPropertiesForCorrectCluster() {
        // When:
        builder.build(topology, "cluster");

        // Then:
        verify(clientsExtension).properties("cluster");
    }

    @Test
    void shouldBuildStreamsApp() {
        // When:
        builder.build(topology, "cluster");

        // Then:
        verify(appFactory)
                .create(eq(topology), eq(properties), isA(DefaultKafkaClientSupplier.class));
    }

    @Test
    void shouldReturnStreamsApp() {
        // When:
        final KafkaStreams result = builder.build(topology, "cluster");

        // Then:
        assertThat(result, is(app));
    }
}
