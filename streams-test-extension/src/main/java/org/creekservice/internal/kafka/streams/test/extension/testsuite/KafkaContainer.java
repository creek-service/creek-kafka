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

import java.util.ArrayList;
import java.util.List;
import org.creekservice.api.system.test.extension.service.ServiceContainer;
import org.creekservice.api.system.test.extension.service.ServiceInstance;

public final class KafkaContainer {

    public static final int KAFKA_PORT = 9093;
    private static final int ZOOKEEPER_PORT = 2181;
    private static final String DEFAULT_INTERNAL_TOPIC_RF = "1";

    // Todo: Support Kafka without ZK
    public static ServiceInstance add(final String clusterName, final ServiceContainer services) {
        // Todo: make version & image of Kafka configurable.
        final ServiceInstance instance =
                services.add("kafka-" + clusterName, "confluentinc/cp-kafka:6.2.4");
        final ServiceInstance.Modifier container = instance.modify();

        final List<String> commandLines = new ArrayList<>();
        commandLines.add("#!/bin/bash");

        setEnv(container, instance.name());

        commandLines.addAll(setUpZooKeeper(container));

        // Optimization: skip the checks
        commandLines.add("echo '' > /etc/confluent/docker/ensure");

        // Run the original command
        commandLines.add("/etc/confluent/docker/run");

        container.withCommand("sh", "-c", String.join(lineSeparator(), commandLines));
        return instance;
    }

    private static void setEnv(final ServiceInstance.Modifier container, final String hostName) {
        container
                .withExposedPorts(KAFKA_PORT)

                // Use two listeners with different names, it will force Kafka to communicate with
                // itself via internal
                // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set, otherwise Kafka will try
                // to use the advertised listener
                .withEnv(
                        "KAFKA_LISTENERS",
                        "BROKER://0.0.0.0:9092,PLAINTEXT://0.0.0.0:" + KAFKA_PORT)
                .withEnv(
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "BROKER://" + hostName + ":" + KAFKA_PORT)
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

    private static List<String> setUpZooKeeper(final ServiceInstance.Modifier container) {
        container
                .withExposedPorts(ZOOKEEPER_PORT) // Todo: Should not be needed.
                .withEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:" + ZOOKEEPER_PORT);

        return List.of(
                "echo 'clientPort=" + ZOOKEEPER_PORT + "' > zookeeper.properties",
                "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties",
                "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties",
                "zookeeper-server-start ./zookeeper.properties &");
    }
}

// Todo: test...
