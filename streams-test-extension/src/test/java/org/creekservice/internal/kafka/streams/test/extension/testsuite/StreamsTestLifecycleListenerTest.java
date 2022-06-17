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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.service.ServiceInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamsTestLifecycleListenerTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekSystemTest api;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ServiceInstance serviceInstance0;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ServiceInstance serviceInstance1;

    @Mock private ServiceDescriptor descriptor;

    private StreamsTestLifecycleListener listener;

    @BeforeEach
    void setUp() {
        listener = new StreamsTestLifecycleListener(api);

        when(api.testSuite().services().stream())
                .thenReturn(Stream.of(serviceInstance0, serviceInstance1));
    }

    @Test
    void shouldIgnoreServicesWithoutADescriptor() {
        // Given:
        when(serviceInstance0.descriptor()).thenReturn(Optional.empty());
        when(serviceInstance1.descriptor()).thenReturn(Optional.empty());

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services(), never()).add(any());
    }

    @Test
    void shouldNotStartKafkaIfNoServiceDescriptorsHaveKafkaResources() {
        // Given:
        when(serviceInstance0.descriptor()).thenReturn(Optional.of(descriptor));
        final ResourceDescriptor noneKafkaResource = mock(ResourceDescriptor.class);
        when(descriptor.resources()).thenReturn(Stream.of(noneKafkaResource));

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services(), never()).add(any());
    }

    @Test
    void shouldAddKafka() {
        // Given:
        when(serviceInstance0.descriptor()).thenReturn(Optional.of(descriptor));
        final KafkaTopicDescriptor<?, ?> kafkaResource = mock(KafkaTopicDescriptor.class);
        when(kafkaResource.cluster()).thenReturn("bob");
        when(descriptor.resources()).thenReturn(Stream.of(kafkaResource));

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services()).add(isA(KafkaContainerDef.class));
    }

    @Test
    void shouldAddKafkaBrokerPerCluster() {
        // Todo:
    }

    @Test
    void shouldStartKafka() {
        // When:
        listener.beforeSuite(null);
    }

    @Test
    void shouldSupportMultipleClusters() {
        // Todo:
    }
}

// Todo: start Kafka!
