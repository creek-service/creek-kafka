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

package org.creekservice.internal.kafka.streams.extension.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.creekservice.api.kafka.common.config.ClustersProperties;
import org.creekservice.api.kafka.common.config.KafkaPropertyOverrides;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClustersPropertiesFactoryTest {

    @Mock private ClustersPropertiesFactory.TopicCollector topicCollector;
    @Mock private Collection<? extends ComponentDescriptor> components;
    @Mock private ClustersProperties.Builder propertiesBuilder;
    @Mock private ClustersProperties.Builder updatedPropertiesBuilder;
    @Mock private KafkaPropertyOverrides propertiesOverrides;
    @Mock private ClustersProperties propertyOverrides;
    @Mock private KafkaStreamsExtensionOptions options;
    @Mock private KafkaTopicDescriptor<?, ?> topicA;
    @Mock private KafkaTopicDescriptor<?, ?> topicB;
    @Mock private ClustersProperties buildProperties;
    private ClustersPropertiesFactory factory;

    @BeforeEach
    void setUp() {
        factory = new ClustersPropertiesFactory(topicCollector);

        when(options.propertiesBuilder()).thenReturn(propertiesBuilder);
        when(options.propertyOverrides()).thenReturn(propertiesOverrides);

        when(topicCollector.collectTopics(components)).thenReturn(Map.of("a", topicA, "b", topicB));

        when(topicA.cluster()).thenReturn("cluster-A");
        when(topicB.cluster()).thenReturn("cluster-B");

        when(propertiesOverrides.get(anySet())).thenReturn(propertyOverrides);
        when(propertiesBuilder.putAll(any())).thenReturn(updatedPropertiesBuilder);
        when(updatedPropertiesBuilder.build()).thenReturn(buildProperties);
    }

    @Test
    void shouldPassClusterNamesToOverridesProvider() {
        // When:
        factory.create(components, options);

        // Then:
        verify(propertiesOverrides).get(Set.of("cluster-A", "cluster-B"));
    }

    @Test
    void shouldPassUniqueClusterNamesToOverridesProvider() {
        // Given:
        when(topicB.cluster()).thenReturn("cluster-A");

        // When:
        factory.create(components, options);

        // Then:
        verify(propertiesOverrides).get(Set.of("cluster-A"));
    }

    @Test
    void shouldApplyOverridesToProperties() {
        // When:
        factory.create(components, options);

        // Then:
        verify(propertiesBuilder).putAll(propertyOverrides);
    }

    @Test
    void shouldReturnProperties() {
        // When:
        final ClustersProperties result = factory.create(components, options);

        // Then:
        assertThat(result, is(buildProperties));
    }
}
