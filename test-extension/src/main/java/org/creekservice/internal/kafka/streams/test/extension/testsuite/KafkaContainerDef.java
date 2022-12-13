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

import static java.lang.System.lineSeparator;
import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.creekservice.api.system.test.extension.component.definition.ServiceDefinition;
import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance.ExecResult;

final class KafkaContainerDef implements ServiceDefinition {

    public static final int TEST_NETWORK_PORT = 9093;
    public static final int SERVICE_NETWORK_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final String DEFAULT_INTERNAL_PARTITION_COUNT = "1";
    private static final String DEFAULT_INTERNAL_TOPIC_RF = "1";
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(1);

    private final String name;
    private final String kafkaDockerImage;

    KafkaContainerDef(final String clusterName, final String kafkaDockerImage) {
        this.name = "kafka-" + requireNonBlank(clusterName, "clusterName");
        this.kafkaDockerImage = requireNonBlank(kafkaDockerImage, "kafkaDockerImage");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String dockerImage() {
        return kafkaDockerImage;
    }

    @Override
    public void configureInstance(final ConfigurableServiceInstance instance) {
        final List<String> commandLines = new ArrayList<>();
        commandLines.add("#!/bin/bash");
        commandLines.addAll(setUpZooKeeper(instance));
        commandLines.addAll(setUpKafka(instance));

        instance.setCommand("sh", "-c", String.join(lineSeparator(), commandLines));
        instance.setStartupTimeout(STARTUP_TIMEOUT);
        instance.setShutdownTimeout(SHUTDOWN_TIMEOUT);
    }

    @Override
    public void instanceStarted(final ServiceInstance instance) {
        // Now that the instance is started the mapped port can be obtained to allow clients in the
        // test-network to correctly connect:
        final String serviceNwkBootstrap = serviceNetworkListener(instance);
        final String testNwkBootstrap = testNetworkListener(instance);

        final ExecResult result =
                instance.execOnInstance(
                        "kafka-configs",
                        "--alter",
                        "--bootstrap-server",
                        serviceNwkBootstrap,
                        "--entity-type",
                        "brokers",
                        "--entity-name",
                        "1",
                        "--add-config",
                        "advertised.listeners=["
                                + testNwkBootstrap
                                + ","
                                + serviceNwkBootstrap
                                + "]");

        if (result.exitCode() != 0) {
            throw new FailedToConfigureKafkaException(result);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KafkaContainerDef that = (KafkaContainerDef) o;
        return Objects.equals(name, that.name)
                && Objects.equals(kafkaDockerImage, that.kafkaDockerImage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, kafkaDockerImage);
    }

    private static List<String> setUpZooKeeper(final ConfigurableServiceInstance instance) {
        instance.addEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:" + ZOOKEEPER_PORT);

        return List.of(
                "echo 'clientPort=" + ZOOKEEPER_PORT + "' > zookeeper.properties",
                "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties",
                "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties",
                "zookeeper-server-start ./zookeeper.properties &");
    }

    private static List<String> setUpKafka(final ConfigurableServiceInstance instance) {
        instance.addExposedPorts(TEST_NETWORK_PORT)
                // Use two listeners with different names, it forces Kafka to communicate with
                // itself via correct listener when KAFKA_INTER_BROKER_LISTENER_NAME is set,
                // otherwise Kafka will try to use the advertised listener:
                .addEnv(
                        "KAFKA_LISTENERS",
                        "SERVICE_NETWORK://0.0.0.0:"
                                + SERVICE_NETWORK_PORT
                                + ",TEST_NETWORK://0.0.0.0:"
                                + TEST_NETWORK_PORT)
                .addEnv(
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "SERVICE_NETWORK:PLAINTEXT,TEST_NETWORK:PLAINTEXT")
                .addEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "SERVICE_NETWORK")
                // Initially set advertised listeners to just the service-network listener, as
                // port of test-network listener is not known until the service starts:
                .addEnv("KAFKA_ADVERTISED_LISTENERS", serviceNetworkListener(instance))
                .addEnv("KAFKA_BROKER_ID", "1")
                .addEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", DEFAULT_INTERNAL_PARTITION_COUNT)
                .addEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv(
                        "KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS",
                        DEFAULT_INTERNAL_PARTITION_COUNT)
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "")
                .addEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .addEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

        return List.of(
                // Optimization: skip the checks
                "echo '' > /etc/confluent/docker/ensure",
                // Run the original command
                "/etc/confluent/docker/run");
    }

    private static String testNetworkListener(final ServiceInstance instance) {
        return "TEST_NETWORK://"
                + instance.testNetworkHostname()
                + ":"
                + instance.testNetworkPort(TEST_NETWORK_PORT);
    }

    private static String serviceNetworkListener(final ServiceInstance instance) {
        return "SERVICE_NETWORK://"
                + instance.serviceNetworkHostname()
                + ":"
                + SERVICE_NETWORK_PORT;
    }

    private static final class FailedToConfigureKafkaException extends RuntimeException {
        FailedToConfigureKafkaException(final ExecResult result) {
            super(
                    "Failed to configure Kafka's advertised listeners. 'kafka-configs''s exec result: "
                            + result);
        }
    }
}
