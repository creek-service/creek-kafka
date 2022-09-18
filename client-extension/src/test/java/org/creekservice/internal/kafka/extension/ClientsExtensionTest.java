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

package org.creekservice.internal.kafka.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.extension.resource.ResourceRegistry;
import org.creekservice.internal.kafka.extension.resource.Topic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ClientsExtensionTest {

    @Mock private ClustersProperties clustersProperties;
    @Mock private ResourceRegistry resources;
    @Mock private Properties properties;
    @Mock private KafkaTopicDescriptor<Long, String> topicDef;
    @Mock private Topic<Long, String> topic;
    private ClientsExtension extension;

    @BeforeEach
    void setUp() {
        extension = new ClientsExtension(clustersProperties, resources);

        when(clustersProperties.properties(any())).thenReturn(properties);
    }

    @Test
    void shouldReturnName() {
        assertThat(extension.name(), is("org.creekservice.kafka.clients"));
    }

    @Test
    void shouldReturnProperties() {
        // When:
        final Properties result = extension.properties("cluster-bob");

        // Then:
        assertThat(result, is(properties));
        verify(clustersProperties).properties("cluster-bob");
    }

    @Test
    void shouldGetTopicsFromRegistry() {
        // Given:
        when(resources.topic(topicDef)).thenReturn(topic);

        // When:
        final KafkaTopic<Long, String> result = extension.topic(topicDef);

        // Then:
        assertThat(result, is(topic));
    }
}
