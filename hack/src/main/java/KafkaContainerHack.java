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

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.creekservice.api.system.test.extension.service.ServiceDefinition;
import org.creekservice.api.system.test.extension.service.ServiceInstance;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.lineSeparator;

public class KafkaContainerHack extends GenericContainer<KafkaContainerHack> {

    public static final int KAFKA_PORT = 9093;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final String DEFAULT_INTERNAL_TOPIC_RF = "1";

    public KafkaContainerHack() {
        super("confluentinc/cp-kafka:6.1.2");

        modify(this);
    }


    public void modify(final GenericContainer<?> container) {
        final List<String> commandLines = new ArrayList<>();
        commandLines.add("#!/bin/bash");

        setEnv(container);

        commandLines.addAll(setUpZooKeeper(container));

        // Optimization: skip the checks
        commandLines.add("echo '' > /etc/confluent/docker/ensure");

        // Run the original command
        commandLines.add("/etc/confluent/docker/run");

        container.withCommand("sh", "-c", String.join(lineSeparator(), commandLines));
    }

    private static void setEnv(final GenericContainer<?> container) {
        container
                .addExposedPorts(KAFKA_PORT);

        final String hostName = container.getNetworkAliases().get(0);

        container
                // Use two listeners with different names, it will force Kafka to communicate with
                // itself via internal
                // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will try
                // to use the advertised listener
                .withEnv(
                        "KAFKA_LISTENERS",
                        "PLAINTEXT://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:9092")
                .withEnv(
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "BROKER://" + hostName + ":9092")
                .withEnv("KAFKA_BROKER_ID", "1")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF)
                .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", DEFAULT_INTERNAL_TOPIC_RF)
                .withEnv(
                        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF)
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", DEFAULT_INTERNAL_TOPIC_RF)
                .withEnv(
                        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF)
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", DEFAULT_INTERNAL_TOPIC_RF)
                .withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }

    private static List<String> setUpZooKeeper(final GenericContainer<?> container) {
        container
                .addExposedPorts(ZOOKEEPER_PORT); // Todo: Should not be needed.

        container
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:" + ZOOKEEPER_PORT);

        return List.of(
                "echo 'clientPort=" + ZOOKEEPER_PORT + "' > zookeeper.properties",
                "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties",
                "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties",
                "zookeeper-server-start ./zookeeper.properties &");
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        String brokerAdvertisedListener = brokerAdvertisedListener(containerInfo);
        ExecResult result;
        try {
            result = execInContainer(
                    "kafka-configs",
                    "--alter",
                    "--bootstrap-server", brokerAdvertisedListener,
                    "--entity-type", "brokers",
                    "--entity-name", getEnvMap().get("KAFKA_BROKER_ID"),
                    "--add-config",
                    "advertised.listeners=[" + String.join(",", getBootstrapServers(), brokerAdvertisedListener) + "]"
            );
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(result.toString());
        }
    }

    public String getBootstrapServers() {
        return String.format("PLAINTEXT://%s:%s", getHost(), getMappedPort(KAFKA_PORT));
    }

    protected String brokerAdvertisedListener(InspectContainerResponse containerInfo) {
        return String.format("BROKER://%s:%s", containerInfo.getConfig().getHostName(), "9092");
    }
}
