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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.service.ServiceInstance;
import org.creekservice.api.system.test.extension.testsuite.TestLifecycleListener;

public final class StreamsTestLifecycleListener implements TestLifecycleListener {

    private final CreekSystemTest api;
    private int mappedPort = -1;

    public StreamsTestLifecycleListener(final CreekSystemTest api) {
        this.api = requireNonNull(api, "systemTest");
    }

    @Override
    public void beforeSuite(final CreekTestSuite suite) {
        final Map<String, List<ServiceInstance>> clusterServices =
                api.testSuite().services().stream()
                        .flatMap(this::requiredClusters)
                        .collect(
                                groupingBy(
                                        ClusterInstance::cluster,
                                        mapping(ClusterInstance::service, toList())));

        clusterServices.forEach(this::createCluster);
    }

    @Override
    public void afterSuite(final CreekTestSuite suite) {
        // Todo: Stop Kafka instances.
    }

    private Stream<ClusterInstance> requiredClusters(final ServiceInstance service) {
        if (service.descriptor().isEmpty()) {
            // Not a service under test.
            return Stream.of();
        }

        final Set<String> uniqueClusters =
                service.descriptor()
                        .get()
                        .resources()
                        .filter(KafkaTopicDescriptor.class::isInstance)
                        .map(r -> (KafkaTopicDescriptor<?, ?>) r)
                        .map(KafkaTopicDescriptor::cluster)
                        .collect(toSet());

        return uniqueClusters.stream().map(cluster -> new ClusterInstance(cluster, service));
    }

    private void createCluster(
            final String clusterName, final Collection<ServiceInstance> clusterUsers) {

        final ServiceInstance kafka =
                api.testSuite().services().add(new KafkaContainerDef(clusterName));
        kafka.start();

        // Todo: Move into KafkaContainer?
        mappedPort = kafka.mappedPort(KafkaContainerDef.KAFKA_PORT);
        // Todo: kafka.getHost() -> returns 'localhost'

        // Todo: Set env vars on clsuterUsers.
        // Todo: Wire up env vars in the service context:
    }

    String hostEndpoint() {
        return "localhost:" + mappedPort;
    }

    private static final class ClusterInstance {
        private final String cluster;
        private final ServiceInstance service;

        ClusterInstance(final String cluster, final ServiceInstance service) {
            this.cluster = requireNonNull(cluster, "cluster");
            this.service = requireNonNull(service, "service");
        }

        public String cluster() {
            return cluster;
        }

        public ServiceInstance service() {
            return service;
        }
    }
}
// Todo: test
