/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.api.system.test.extension.test.env.listener.TestEnvironmentListener;
import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;
import org.creekservice.api.system.test.extension.test.model.CreekTestSuite;
import org.creekservice.api.system.test.extension.test.model.TestSuiteResult;
import org.creekservice.internal.kafka.extension.resource.TopicCollector;
import org.creekservice.internal.kafka.streams.test.extension.ClusterEndpointsProvider;
import org.creekservice.internal.kafka.streams.test.extension.handler.TestOptionsAccessor;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;

/**
 * A test listener responsible for starting and stopping the Kafka broker instances the test suite
 * requires.
 */
public final class StartKafkaTestListener implements TestEnvironmentListener {

    private final CreekSystemTest api;
    private final TopicCollector topicCollector;
    private final ClusterEndpointsProvider clusterEndpointsProvider;
    private final Map<String, ServiceInstance> kafkaInstances = new HashMap<>();

    /**
     * @param api the system test api.
     * @param clusterEndpointsProvider a provider of cluster endpoint information.
     */
    public StartKafkaTestListener(
            final CreekSystemTest api, final ClusterEndpointsProvider clusterEndpointsProvider) {
        this(api, clusterEndpointsProvider, new TopicCollector());
    }

    @VisibleForTesting
    StartKafkaTestListener(
            final CreekSystemTest api,
            final ClusterEndpointsProvider clusterEndpointsProvider,
            final TopicCollector topicCollector) {
        this.api = requireNonNull(api, "api");
        this.topicCollector = requireNonNull(topicCollector, "topicCollector");
        this.clusterEndpointsProvider =
                requireNonNull(clusterEndpointsProvider, "clusterEndpointsProvider");
    }

    @Override
    public void beforeSuite(final CreekTestSuite suite) {
        final KafkaOptions options = TestOptionsAccessor.get(suite);
        final Map<String, List<ConfigurableServiceInstance>> clusterServices =
                api.tests().env().currentSuite().services().stream()
                        .flatMap(this::requiredClusters)
                        .collect(
                                groupingBy(
                                        ClusterInstance::clusterName,
                                        mapping(ClusterInstance::service, toList())));

        clusterServices.forEach(
                (clusterName, clusterUsers) -> createCluster(clusterName, clusterUsers, options));
    }

    @Override
    public void afterSuite(final CreekTestSuite suite, final TestSuiteResult result) {
        kafkaInstances
                .keySet()
                .forEach(clusterName -> clusterEndpointsProvider.put(clusterName, Map.of()));
        kafkaInstances.values().forEach(ServiceInstance::stop);
        kafkaInstances.clear();
    }

    private Stream<ClusterInstance> requiredClusters(final ConfigurableServiceInstance service) {
        return service.descriptor()
                .map(
                        descriptor ->
                                topicCollector
                                        .collectTopics(List.of(descriptor))
                                        .clusters()
                                        .stream()
                                        .map(
                                                clusterName ->
                                                        new ClusterInstance(clusterName, service)))
                .orElse(Stream.of());
    }

    private void createCluster(
            final String clusterName,
            final Collection<ConfigurableServiceInstance> clusterUsers,
            final KafkaOptions options) {

        final ServiceInstance kafka =
                api.tests()
                        .env()
                        .currentSuite()
                        .services()
                        .add(new KafkaContainerDef(clusterName, options.kafkaDockerImage()));

        kafka.start();

        kafkaInstances.put(clusterName, kafka);

        setEnv(clusterUsers, clusterName, kafka);

        clusterEndpointsProvider.put(
                clusterName,
                Map.of(
                        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                        kafka.testNetworkHostname()
                                + ":"
                                + kafka.testNetworkPort(KafkaContainerDef.TEST_NETWORK_PORT)));
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
            this.clusterName = requireNonBlank(clusterName, "cluster");
            this.service = requireNonNull(service, "service");
        }

        public String clusterName() {
            return clusterName;
        }

        public ConfigurableServiceInstance service() {
            return service;
        }
    }
}
