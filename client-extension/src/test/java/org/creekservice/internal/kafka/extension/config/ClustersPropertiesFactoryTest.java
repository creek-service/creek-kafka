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

package org.creekservice.internal.kafka.extension.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.creekservice.api.kafka.extension.ClientsExtensionOptions;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.internal.kafka.extension.resource.TopicCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClustersPropertiesFactoryTest {

    @Mock private TopicCollector topicCollector;
    @Mock private Collection<? extends ComponentDescriptor> components;
    @Mock private ClustersProperties.Builder propertiesBuilder;
    @Mock private ClientsExtensionOptions options;
    @Mock private KafkaTopicDescriptor<?, ?> topicA;
    @Mock private KafkaTopicDescriptor<?, ?> topicB;
    @Mock private ClustersProperties builtProperties;
    private ClustersPropertiesFactory factory;

    @BeforeEach
    void setUp() {
        factory = new ClustersPropertiesFactory(topicCollector);

        when(options.propertiesBuilder()).thenReturn(propertiesBuilder);

        when(topicCollector.collectTopics(components))
                .thenReturn(
                        Map.of(
                                URI.create("topic://default/a"),
                                topicA,
                                URI.create("topic://default/b"),
                                topicB));

        when(topicA.cluster()).thenReturn("cluster-A");
        when(topicB.cluster()).thenReturn("cluster-B");

        when(propertiesBuilder.build(any())).thenReturn(builtProperties);
    }

    @Test
    void shouldPassClusterNamesToPropertiesBuilder() {
        // When:
        factory.create(components, options);

        // Then:
        verify(propertiesBuilder).build(Set.of("cluster-A", "cluster-B"));
    }

    @Test
    void shouldPassUniqueClusterNames() {
        // Given:
        when(topicB.cluster()).thenReturn("cluster-A");

        // When:
        factory.create(components, options);

        // Then:
        verify(propertiesBuilder).build(Set.of("cluster-A"));
    }

    @Test
    void shouldReturnProperties() {
        // When:
        final ClustersProperties result = factory.create(components, options);

        // Then:
        assertThat(result, is(builtProperties));
    }
}
