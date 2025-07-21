/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

import static org.creekservice.internal.kafka.extension.resource.TopicCollector.CollectedTopics;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.internal.kafka.extension.resource.TopicCollector;
import org.creekservice.internal.kafka.streams.test.extension.ClusterEndpointsProvider;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;
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
class StartKafkaTestListenerTest {

    private static final String KAFKA_DOCKER_IMAGE = "some-kafka-docker-image";

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private CreekSystemTest api;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurableServiceInstance serviceInstance0;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private ConfigurableServiceInstance serviceInstance1;

    @Mock private ServiceDescriptor descriptor0;
    @Mock private ServiceDescriptor descriptor1;
    @Mock private TopicCollector topicCollector;
    @Mock private CollectedTopics collectedTopics;
    @Mock private ClusterEndpointsProvider clusterEndpointsProvider;
    @Mock private CreekTestSuite suite;

    private StartKafkaTestListener listener;

    @BeforeEach
    void setUp() {
        listener = new StartKafkaTestListener(api, clusterEndpointsProvider, topicCollector);

        when(api.tests().env().currentSuite().services().stream())
                .thenReturn(Stream.of(serviceInstance0, serviceInstance1));

        when(serviceInstance0.name()).thenReturn("inst0");
        when(serviceInstance1.name()).thenReturn("inst1");
        doReturn(Optional.of(descriptor0)).when(serviceInstance0).descriptor();
        doReturn(Optional.of(descriptor1)).when(serviceInstance1).descriptor();
        when(descriptor0.name()).thenReturn("service-0");
        when(descriptor1.name()).thenReturn("service-1");

        final KafkaOptions testOption = mock(KafkaOptions.class);
        when(testOption.kafkaDockerImage()).thenReturn(KAFKA_DOCKER_IMAGE);
        when(suite.options(KafkaOptions.class)).thenReturn(List.of(testOption));

        when(topicCollector.collectTopics(any())).thenReturn(collectedTopics);
    }

    @Test
    void shouldIgnoreServicesWithoutADescriptor() {
        // Given:
        when(serviceInstance0.descriptor()).thenReturn(Optional.empty());
        when(serviceInstance1.descriptor()).thenReturn(Optional.empty());

        // When:
        listener.beforeSuite(suite);

        // Then:
        verify(topicCollector, never()).collectTopics(any());
        verify(api.tests().env().currentSuite().services(), never()).add(any());
    }

    @Test
    void shouldCollectTopicsFromAllServicesWithDescriptors() {
        // When:
        listener.beforeSuite(suite);

        // Then:
        verify(topicCollector).collectTopics(List.of(descriptor0));
        verify(topicCollector).collectTopics(List.of(descriptor1));
    }

    @Test
    void shouldNotStartKafkaIfServiceDescriptorsHaveNoKafkaResources() {
        // Given:
        when(collectedTopics.clusters()).thenReturn(Set.of());

        // When:
        listener.beforeSuite(suite);

        // Then:
        verify(api.tests().env().currentSuite().services(), never()).add(any());
    }

    @Test
    void shouldAddKafkaBrokerPerCluster() {
        // Given:
        when(collectedTopics.clusters()).thenReturn(Set.of("bob", "janet"));

        // When:
        listener.beforeSuite(suite);

        // Then:
        verify(api.tests().env().currentSuite().services())
                .add(new KafkaContainerDef("bob", KAFKA_DOCKER_IMAGE));
        verify(api.tests().env().currentSuite().services())
                .add(new KafkaContainerDef("janet", KAFKA_DOCKER_IMAGE));
    }

    @Test
    void shouldStartKafka() {
        // Given:
        when(collectedTopics.clusters()).thenReturn(Set.of("bob"));

        // When:
        listener.beforeSuite(suite);

        // Then:
        verify(api.tests().env().currentSuite().services().add(any())).start();
    }

    @Test
    void shouldSetClusterEndpoints() {
        // Given:
        when(collectedTopics.clusters()).thenReturn(Set.of("bob"));

        final ConfigurableServiceInstance kafkaInstance = mock(ConfigurableServiceInstance.class);
        when(api.tests().env().currentSuite().services().add(any())).thenReturn(kafkaInstance);

        when(kafkaInstance.testNetworkHostname()).thenReturn("localhost");
        when(kafkaInstance.testNetworkPort(KafkaContainerDef.TEST_NETWORK_PORT)).thenReturn(27465);

        // When:
        listener.beforeSuite(suite);

        // Then:
        verify(clusterEndpointsProvider).put("bob", Map.of("bootstrap.servers", "localhost:27465"));
    }

    @Test
    void shouldClearClusterEndpoints() {
        // Given:
        when(collectedTopics.clusters()).thenReturn(Set.of("bob"));

        listener.beforeSuite(suite);

        // When:
        listener.afterSuite(null, null);

        // Then:
        verify(clusterEndpointsProvider).put("bob", Map.of());
    }

    @Test
    void shouldStopKafkaOnlyOnce() {
        // Given:
        when(collectedTopics.clusters()).thenReturn(Set.of("bob"));
        listener.beforeSuite(suite);

        // When:
        listener.afterSuite(null, null);
        listener.afterSuite(null, null);

        // Then:
        verify(api.tests().env().currentSuite().services().add(any())).stop();
    }

    @Test
    void shouldSetKafkaEndpointOnServicesUnderTest() {
        // Given:
        when(collectedTopics.clusters()).thenReturn(Set.of("bob", "janet"));

        when(api.tests()
                        .env()
                        .currentSuite()
                        .services()
                        .add(new KafkaContainerDef("bob", KAFKA_DOCKER_IMAGE))
                        .name())
                .thenReturn("kafka-bob-0");
        when(api.tests()
                        .env()
                        .currentSuite()
                        .services()
                        .add(new KafkaContainerDef("janet", KAFKA_DOCKER_IMAGE))
                        .name())
                .thenReturn("kafka-janet-0");

        // When:
        listener.beforeSuite(suite);

        // Then:
        verify(serviceInstance0).addEnv("KAFKA_BOB_BOOTSTRAP_SERVERS", "kafka-bob-0:9092");
        verify(serviceInstance1).addEnv("KAFKA_BOB_BOOTSTRAP_SERVERS", "kafka-bob-0:9092");
        verify(serviceInstance1).addEnv("KAFKA_JANET_BOOTSTRAP_SERVERS", "kafka-janet-0:9092");
    }

    @Test
    void shouldSetApplicationIdOnServicesUnderTest() {
        // Given:
        when(collectedTopics.clusters()).thenReturn(Set.of("bob", "janet"));

        // When:
        listener.beforeSuite(suite);

        // Then:
        verify(serviceInstance0).addEnv("KAFKA_BOB_APPLICATION_ID", "service-0");
        verify(serviceInstance1).addEnv("KAFKA_JANET_APPLICATION_ID", "service-1");
    }
}
