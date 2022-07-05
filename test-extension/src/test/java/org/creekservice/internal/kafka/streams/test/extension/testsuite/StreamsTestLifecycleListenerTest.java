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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.service.ConfigurableServiceInstance;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.StreamsTestLifecycleListener.TopicCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class StreamsTestLifecycleListenerTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekSystemTest api;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurableServiceInstance serviceInstance0;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurableServiceInstance serviceInstance1;

    @Mock private ServiceDescriptor descriptor0;
    @Mock private ServiceDescriptor descriptor1;

    @Mock private KafkaTopicDescriptor<?, ?> kafkaResource0;

    @Mock private KafkaTopicDescriptor<?, ?> kafkaResource1;
    @Mock private TopicCollector topicCollector;

    private StreamsTestLifecycleListener listener;

    @BeforeEach
    void setUp() {
        listener = new StreamsTestLifecycleListener(api, topicCollector);

        when(api.testSuite().services().stream())
                .thenReturn(Stream.of(serviceInstance0, serviceInstance1));

        when(serviceInstance0.name()).thenReturn("inst0");
        when(serviceInstance1.name()).thenReturn("inst1");
        when(serviceInstance0.descriptor()).thenReturn(Optional.of(descriptor0));
        when(serviceInstance1.descriptor()).thenReturn(Optional.of(descriptor1));
        when(descriptor0.name()).thenReturn("service-0");
        when(descriptor1.name()).thenReturn("service-1");
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
        when(topicCollector.collectTopics(descriptor0)).thenReturn(Stream.of());

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services(), never()).add(any());
    }

    @Test
    void shouldAddKafkaBrokerPerCluster() {
        // Given:
        when(topicCollector.collectTopics(descriptor0)).thenReturn(Stream.of(kafkaResource0));
        when(topicCollector.collectTopics(descriptor1)).thenReturn(Stream.of(kafkaResource1));

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services()).add(new KafkaContainerDef("bob"));
        verify(api.testSuite().services()).add(new KafkaContainerDef("janet"));
    }

    @Test
    void shouldStartKafka() {
        // Given:
        when(topicCollector.collectTopics(descriptor0)).thenReturn(Stream.of(kafkaResource0));

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(api.testSuite().services().add(any())).start();
    }

    @Test
    void shouldStopKafkaOnlyOnce() {
        // Given:
        when(topicCollector.collectTopics(descriptor0)).thenReturn(Stream.of(kafkaResource0));
        listener.beforeSuite(null);

        // When:
        listener.afterSuite(null);
        listener.afterSuite(null);

        // Then:
        verify(api.testSuite().services().add(any())).stop();
    }

    @Test
    void shouldSetKafkaEndpointOnServicesUnderTest() {
        // Given:
        when(topicCollector.collectTopics(descriptor0)).thenReturn(Stream.of(kafkaResource0));
        when(topicCollector.collectTopics(descriptor1))
                .thenReturn(Stream.of(kafkaResource0, kafkaResource1));

        when(api.testSuite().services().add(new KafkaContainerDef("bob")).name())
                .thenReturn("kafka-bob-0");
        when(api.testSuite().services().add(new KafkaContainerDef("janet")).name())
                .thenReturn("kafka-janet-0");

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(serviceInstance0).addEnv("KAFKA_BOB_BOOTSTRAP_SERVERS", "kafka-bob-0:9092");
        verify(serviceInstance1).addEnv("KAFKA_BOB_BOOTSTRAP_SERVERS", "kafka-bob-0:9092");
        verify(serviceInstance1).addEnv("KAFKA_JANET_BOOTSTRAP_SERVERS", "kafka-janet-0:9092");
    }

    @Test
    void shouldSetApplicationIdOnServicesUnderTest() {
        // Given:
        when(topicCollector.collectTopics(descriptor0)).thenReturn(Stream.of(kafkaResource0));
        when(topicCollector.collectTopics(descriptor1)).thenReturn(Stream.of(kafkaResource1));

        // When:
        listener.beforeSuite(null);

        // Then:
        verify(serviceInstance0).addEnv("KAFKA_BOB_APPLICATION_ID", "service-0");
        verify(serviceInstance1).addEnv("KAFKA_JANET_APPLICATION_ID", "service-1");
    }
}
