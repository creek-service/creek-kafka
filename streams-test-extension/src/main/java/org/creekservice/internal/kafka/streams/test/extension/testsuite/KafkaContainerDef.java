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
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import org.creekservice.api.system.test.extension.service.ServiceDefinition;
import org.creekservice.api.system.test.extension.service.ServiceInstance;
import org.creekservice.api.system.test.extension.service.ServiceInstance.ConfigureInstance;
import org.creekservice.api.system.test.extension.service.ServiceInstance.ExecResult;

final class KafkaContainerDef implements ServiceDefinition {

    public static final int KAFKA_PORT = 9093;
    public static final int INTERNAL_PORT = 9092;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final String DEFAULT_INTERNAL_TOPIC_RF = "1";

    private final String name;

    KafkaContainerDef(final String clusterName) {
        this.name = "kafka-" + requireNonNull(clusterName, "clusterName");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String dockerImage() {
        // Todo: make version & image of Kafka configurable.
        return "confluentinc/cp-kafka:6.2.4";
    }

    @Override
    public void configureInstance(final ServiceInstance instance) {
        final ConfigureInstance container = instance.configure();

        final List<String> commandLines = new ArrayList<>();
        commandLines.add("#!/bin/bash");

        setUpKafka(container);

        commandLines.addAll(setUpZooKeeper(container));

        // Optimization: skip the checks
        commandLines.add("echo '' > /etc/confluent/docker/ensure");

        // Run the original command
        commandLines.add("/etc/confluent/docker/run");

        container.setCommand("sh", "-c", String.join(lineSeparator(), commandLines));
    }

    @Override
    public void instanceStarted(final ServiceInstance instance) {
        // Now that the instance is started the _actual_ internal host name is available to set in
        // listeners:
        final String internalBootstrap = internalListener(instance);
        final String externalBootstrap = bootstrapServers(instance);

        // Todo: don't think this is needed. Just set in configure using netwrok alias for internal.
        final ExecResult result =
                instance.execOnInstance(
                        "kafka-configs",
                        "--alter",
                        "--bootstrap-server",
                        internalBootstrap,
                        "--entity-type",
                        "brokers",
                        "--entity-name",
                        "1",
                        "--add-config",
                        "advertised.listeners=["
                                + String.join(",", externalBootstrap, internalBootstrap)
                                + "]");

        if (result.exitCode() != 0) {
            throw new IllegalStateException(result.toString());
        }
    }

    private static void setUpKafka(final ConfigureInstance container) {
        container
                .addExposedPorts(KAFKA_PORT)

                // Use two listeners with different names, it will force Kafka to communicate with
                // itself via internal
                // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will try
                // to use the advertised listener
                .addEnv(
                        "KAFKA_LISTENERS",
                        "BROKER://0.0.0.0:" + INTERNAL_PORT + ",PLAINTEXT://0.0.0.0:" + KAFKA_PORT)
                .addEnv(
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .addEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .addEnv("KAFKA_ADVERTISED_LISTENERS", "BROKER://localhost:" + KAFKA_PORT)
                .addEnv("KAFKA_BROKER_ID", "1")
                .addEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", DEFAULT_INTERNAL_TOPIC_RF)
                .addEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "")
                .addEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .addEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }

    // Todo: use to get bootstrap for clients?
    private String bootstrapServers(final ServiceInstance instance) {
        return "PLAINTEXT://" + instance.externalHostName() + ":" + instance.mappedPort(KAFKA_PORT);
    }

    private String internalListener(final ServiceInstance instance) {
        return "BROKER://" + instance.internalHostName() + ":" + INTERNAL_PORT;
    }

    private static List<String> setUpZooKeeper(final ConfigureInstance container) {
        container.addEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:" + ZOOKEEPER_PORT);

        return List.of(
                "echo 'clientPort=" + ZOOKEEPER_PORT + "' > zookeeper.properties",
                "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties",
                "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties",
                "zookeeper-server-start ./zookeeper.properties &");
    }
}

// Todo: test...
