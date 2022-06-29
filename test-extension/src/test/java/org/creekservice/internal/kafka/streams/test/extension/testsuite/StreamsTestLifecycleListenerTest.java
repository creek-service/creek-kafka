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
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.service.ConfigurableServiceInstance;
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
    private ConfigurableServiceInstance serviceInstance0;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurableServiceInstance serviceInstance1;

    @Mock private ServiceDescriptor descriptor0;
    @Mock private ServiceDescriptor descriptor1;
    @Mock private ResourceDescriptor noneKafkaResource;

    @Mock(strictness = LENIENT)
    private KafkaTopicDescriptor<?, ?> kafkaResource0;

    @Mock(strictness = LENIENT)
    private KafkaTopicDescriptor<?, ?> kafkaResource1;

    private StreamsTestLifecycleListener listener;

    @BeforeEach
    void setUp() {
        listener = new StreamsTestLifecycleListener(api);

        when(api.testSuite().services().stream())
                .thenReturn(Stream.of(serviceInstance0, serviceInstance1));

        when(serviceInstance0.descriptor()).thenReturn(Optional.of(descriptor0));
        when(serviceInstance1.descriptor()).thenReturn(Optional.of(descriptor1));
        when(kafkaResource0.cluster()).thenReturn("bob");
        when(kafkaResource1.cluster()).thenReturn("janet");
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
        when(descriptor0.resources()).thenAnswer(inv -> Stream.of(noneKafkaResource));

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services(), never()).add(any());
    }

    @Test
    void shouldAddKafkaBrokerPerCluster() {
        // Given:
        when(descriptor0.resources()).thenReturn(Stream.of(kafkaResource0));
        when(descriptor1.resources()).thenReturn(Stream.of(kafkaResource1));

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services()).add(new KafkaContainerDef("bob"));
        verify(api.testSuite().services()).add(new KafkaContainerDef("janet"));
    }

    @Test
    void shouldStartKafka() {
        // Given:
        when(descriptor0.resources()).thenReturn(Stream.of(kafkaResource0));

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services().add(any())).start();
    }

    @Test
    void shouldStopKafkaOnlyOnce() {
        // Given:
        when(descriptor0.resources()).thenReturn(Stream.of(kafkaResource0));
        listener.beforeSuite(null);

        // When:
        listener.afterSuite(null);
        listener.afterSuite(null);

        // Then:
        verify(api.testSuite().services().add(any())).stop();
    }
}
