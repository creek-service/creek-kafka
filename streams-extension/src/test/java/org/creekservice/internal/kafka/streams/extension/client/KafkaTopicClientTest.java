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

package org.creekservice.internal.kafka.streams.extension.client;

import static org.apache.kafka.common.KafkaFuture.completedFuture;
import static org.creekservice.test.TopicConfigBuilder.withPartitions;
import static org.creekservice.test.TopicDescriptors.outputTopic;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.creekservice.api.kafka.common.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaTopicClientTest {

    private static final String CLUSTER = "c";
    private static final CreatableKafkaTopic<?, ?> TOPIC_A =
            outputTopic(CLUSTER, "t", Long.class, String.class, withPartitions(1));

    @Mock private ClustersProperties clusterProps;
    @Mock private Function<Map<String, Object>, Admin> adminFactory;
    @Mock private Admin admin;
    @Mock private CreateTopicsResult createTopicsResult;
    private KafkaTopicClient client;

    @BeforeEach
    void setUp() {
        client = new KafkaTopicClient(clusterProps, adminFactory);

        when(adminFactory.apply(any())).thenReturn(admin);
        when(admin.createTopics(any())).thenReturn(createTopicsResult);
        when(createTopicsResult.values()).thenReturn(Map.of(TOPIC_A.name(), completedFuture(null)));
        when(createTopicsResult.numPartitions(TOPIC_A.name())).thenReturn(completedFuture(1));
        when(createTopicsResult.config(TOPIC_A.name()))
                .thenReturn(completedFuture(new Config(List.of())));
    }

    @Test
    void shouldCreateAdmin() {
        // Given:
        when(clusterProps.get(CLUSTER)).thenReturn(Map.of("a", 1));

        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        verify(adminFactory).apply(Map.of("a", 1));
    }

    @Test
    void shouldCloseAdmin() {
        // When:
        client.ensure(List.of(TOPIC_A));

        // Then:
        verify(admin).close();
    }

    @Test
    void shouldCloseAdminOnException() {
        // Given:
        when(admin.createTopics(any())).thenThrow(new RuntimeException("Boom"));

        // When:
        assertThrows(RuntimeException.class, () -> client.ensure(List.of(TOPIC_A)));

        // Then:
        verify(admin).close();
    }
}
