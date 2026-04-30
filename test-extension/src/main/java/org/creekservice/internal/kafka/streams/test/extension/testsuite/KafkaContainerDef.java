/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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

import static org.creekservice.api.base.type.Preconditions.requireNonBlank;

import java.time.Duration;
import java.util.Objects;
import org.creekservice.api.system.test.extension.component.definition.ServiceDefinition;
import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance;

final class KafkaContainerDef implements ServiceDefinition {

    public static final int TEST_NETWORK_PORT = 9093;
    public static final int SERVICE_NETWORK_PORT = 9092;
    private static final int CONTROLLER_PORT = 9094;
    private static final String NODE_ID = "1";
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
        instance.addExposedPorts(TEST_NETWORK_PORT)
                .addEnv("KAFKA_NODE_ID", NODE_ID)
                .addEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .addEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .addEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", NODE_ID + "@localhost:" + CONTROLLER_PORT)
                .addEnv(
                        "KAFKA_LISTENERS",
                        "SERVICE_NETWORK://0.0.0.0:"
                                + SERVICE_NETWORK_PORT
                                + ",TEST_NETWORK://0.0.0.0:"
                                + TEST_NETWORK_PORT
                                + ",CONTROLLER://localhost:"
                                + CONTROLLER_PORT)
                .addEnv(
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "SERVICE_NETWORK:PLAINTEXT,TEST_NETWORK:PLAINTEXT,CONTROLLER:PLAINTEXT")
                .addEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "SERVICE_NETWORK")
                .addEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
                .addEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .addEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "")
                .addEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .addEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

        instance.setStartupTimeout(STARTUP_TIMEOUT)
                .setStartupLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1)
                .setShutdownTimeout(SHUTDOWN_TIMEOUT)
                .setCommand(
                        "sh",
                        "-c",
                        "while [ ! -f /tmp/testcontainers_start.sh ]; do sleep 0.1; done;"
                                + " bash /tmp/testcontainers_start.sh");
    }

    /**
     * Called after the container process starts but before the wait strategy completes. The mapped
     * port is now known, so we can build {@code KAFKA_ADVERTISED_LISTENERS} and write the start
     * script that the container's wait loop is polling for.
     */
    @Override
    public void instanceStarting(final ServiceInstance instance) {
        final String clusterId =
                instance.execOnInstance("kafka-storage", "random-uuid").stdout().trim();
        final String advertisedListeners =
                testNetworkListener(instance) + "," + serviceNetworkListener(instance);

        final String startScript =
                "#!/bin/bash\n"
                        + "set -e\n"
                        + "export CLUSTER_ID="
                        + clusterId
                        + "\n"
                        + "export KAFKA_ADVERTISED_LISTENERS="
                        + advertisedListeners
                        + "\n"
                        + "/etc/confluent/docker/configure\n"
                        + "kafka-storage format --ignore-formatted -t \""
                        + clusterId
                        + "\" -c /etc/kafka/kafka.properties\n"
                        + "exec /etc/confluent/docker/launch\n";

        instance.copyFileToContainer(startScript, "/tmp/testcontainers_start.sh", true);
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
}
