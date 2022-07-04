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
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.service.ServiceInstance;
import org.creekservice.api.system.test.extension.testsuite.TestLifecycleListener;

public final class StreamsTestLifecycleListener implements TestLifecycleListener {

    private final CreekSystemTest api;
    private final List<ServiceInstance> kafkaInstances = new ArrayList<>();

    public StreamsTestLifecycleListener(final CreekSystemTest api) {
        this.api = requireNonNull(api, "systemTest");
    }

    @Override
    public void beforeSuite(final CreekTestSuite suite) {
        final Map<String, List<ConfigurableServiceInstance>> clusterServices =
                api.testSuite().services().stream()
                        .flatMap(this::requiredClusters)
                        .collect(
                                groupingBy(
                                        ClusterInstance::clusterName,
                                        mapping(ClusterInstance::service, toList())));

        clusterServices.forEach(this::createCluster);
    }

    @Override
    public void afterSuite(final CreekTestSuite suite) {
        kafkaInstances.forEach(ServiceInstance::stop);
        kafkaInstances.clear();
    }

    private Stream<ClusterInstance> requiredClusters(final ConfigurableServiceInstance service) {
        return service.descriptor()
                .map(
                        descriptor ->
                                descriptor
                                        .resources()
                                        .filter(KafkaTopicDescriptor.class::isInstance)
                                        .map(r -> (KafkaTopicDescriptor<?, ?>) r)
                                        .map(KafkaTopicDescriptor::cluster)
                                        .distinct()
                                        .map(
                                                clusterName ->
                                                        new ClusterInstance(clusterName, service)))
                .orElse(Stream.of());
    }

    private void createCluster(
            final String clusterName, final Collection<ConfigurableServiceInstance> clusterUsers) {

        final ServiceInstance kafka =
                api.testSuite().services().add(new KafkaContainerDef(clusterName));
        kafka.start();

        kafkaInstances.add(kafka);

        setEnv(clusterUsers, clusterName, kafka);
    }

    private void setEnv(
            final Collection<ConfigurableServiceInstance> clusterUsers,
            final String clusterName,
            final ServiceInstance kafka) {
        final String prefix = "KAFKA_" + clusterName.toUpperCase() + "_";

        clusterUsers.forEach(
                instance ->
                        instance.addEnv(
                                prefix + "BOOTSTRAP_SERVERS",
                                kafka.name() + ":" + KafkaContainerDef.SERVICE_NETWORK_PORT));

        clusterUsers.forEach(
                instance ->
                        instance.addEnv(
                                prefix + "APPLICATION_ID",
                                instance.descriptor().orElseThrow().name()));
    }

    private static final class ClusterInstance {
        private final String clusterName;
        private final ConfigurableServiceInstance service;

        ClusterInstance(final String clusterName, final ConfigurableServiceInstance service) {
            this.clusterName = validate(requireNonBlank(clusterName, "cluster"));
            this.service = requireNonNull(service, "service");
        }

        public String clusterName() {
            return clusterName;
        }

        public ConfigurableServiceInstance service() {
            return service;
        }

        private static String validate(final String cluster) {
            cluster.chars()
                    .filter(c -> !(Character.isDigit(c) || Character.isAlphabetic(c)))
                    .findFirst()
                    .ifPresent(
                            c -> {
                                throw new IllegalArgumentException(
                                        "Invalid character: '"
                                                + (char) c
                                                + "' found in cluster name: "
                                                + cluster);
                            });

            return cluster;
        }
    }
}
