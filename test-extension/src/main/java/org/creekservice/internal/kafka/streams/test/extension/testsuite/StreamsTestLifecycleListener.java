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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ServiceDescriptor;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.service.ServiceInstance;
import org.creekservice.api.system.test.extension.testsuite.TestLifecycleListener;

public final class StreamsTestLifecycleListener implements TestLifecycleListener {

    private final CreekSystemTest api;
    private final TopicCollector topicCollector;
    private final List<ServiceInstance> kafkaInstances = new ArrayList<>();

    public StreamsTestLifecycleListener(final CreekSystemTest api) {
        this(
                api,
                d ->
                        org
                                .creekservice
                                .internal
                                .kafka
                                .common
                                .resource
                                .TopicCollector
                                .collectTopics(List.of(d))
                                .values()
                                .stream());
    }

    @VisibleForTesting
    StreamsTestLifecycleListener(final CreekSystemTest api, final TopicCollector topicCollector) {
        this.api = requireNonNull(api, "api");
        this.topicCollector = requireNonNull(topicCollector, "topicCollector");
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
                                topicCollector
                                        .collectTopics(descriptor)
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
        final String prefix =
                clusterName.isBlank() ? "KAFKA_" : "KAFKA_" + clusterName.toUpperCase() + "_";

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
            this.clusterName = requireNonNull(clusterName, "cluster");
            this.service = requireNonNull(service, "service");
        }

        public String clusterName() {
            return clusterName;
        }

        public ConfigurableServiceInstance service() {
            return service;
        }
    }

    @VisibleForTesting
    interface TopicCollector {
        Stream<KafkaTopicDescriptor<?, ?>> collectTopics(ServiceDescriptor service);
    }
}
