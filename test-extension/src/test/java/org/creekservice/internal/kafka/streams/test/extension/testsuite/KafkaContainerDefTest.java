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
import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance.ExecResult.execResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.EqualsTester;
import java.util.List;
import org.creekservice.api.system.test.extension.test.env.suite.service.ConfigurableServiceInstance;
import org.creekservice.api.system.test.extension.test.env.suite.service.ServiceInstance.ExecResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaContainerDefTest {

    private static final String CLUSTER_NAME = "bob";
    private static final String SERVICE_NWK_NAME = "kafka-bob-2";
    private static final String TEST_NWK_NAME = "localhost";
    private static final String SERVICE_NWK_BOOTSTRAP =
            "SERVICE_NETWORK://" + SERVICE_NWK_NAME + ":9092";
    private static final String TEST_NWK_BOOTSTRAP = "TEST_NETWORK://" + TEST_NWK_NAME + ":123456";

    @Mock private ConfigurableServiceInstance instance;
    @Captor private ArgumentCaptor<String> cmdCaptor;

    private KafkaContainerDef def;

    @BeforeEach
    void setUp() {
        def = new KafkaContainerDef(CLUSTER_NAME);

        when(instance.serviceNetworkHostname()).thenReturn(SERVICE_NWK_NAME);
        when(instance.testNetworkHostname()).thenReturn(TEST_NWK_NAME);
        when(instance.testNetworkPort(9093)).thenReturn(123456);
        when(instance.addEnv(any(), any())).thenReturn(instance);
        when(instance.addExposedPorts(anyInt())).thenReturn(instance);
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        new KafkaContainerDef(CLUSTER_NAME), new KafkaContainerDef(CLUSTER_NAME))
                .addEqualityGroup(new KafkaContainerDef("diff"))
                .testEquals();
    }

    @Test
    void shouldExposeName() {
        assertThat(def.name(), is("kafka-bob"));
    }

    @Test
    void shouldSupportDefaultName() {
        // Given:
        def = new KafkaContainerDef(DEFAULT_CLUSTER_NAME);

        // Then:
        assertThat(def.name(), is("kafka-default"));
    }

    @Test
    void shouldUseRightDockerImage() {
        assertThat(def.dockerImage(), is("confluentinc/cp-kafka:6.2.4"));
    }

    @Test
    void shouldStartKafkaWithScript() {
        // When:
        def.configureInstance(instance);

        // Then:
        final List<String> cmd = cmdLine();
        assertThat(cmd.get(0), is("sh"));
        assertThat(cmd.get(1), is("-c"));
        assertThat(cmd.get(2), startsWith("#!/bin/bash" + lineSeparator()));
    }

    @Test
    void shouldConfigureZooKeeperToStart() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addEnv("KAFKA_ZOOKEEPER_CONNECT", "localhost:2181");

        assertThat(
                cmdLine().get(2),
                containsString(
                        String.join(
                                lineSeparator(),
                                List.of(
                                        "echo 'clientPort=2181' > zookeeper.properties",
                                        "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties",
                                        "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties",
                                        "zookeeper-server-start ./zookeeper.properties &"))));
    }

    @Test
    void shouldConfigureKafkaToStart() {
        // When:
        def.configureInstance(instance);

        // Then:
        assertThat(
                cmdLine().get(2),
                endsWith(
                        String.join(
                                lineSeparator(),
                                List.of(
                                        "echo '' > /etc/confluent/docker/ensure",
                                        "/etc/confluent/docker/run"))));
    }

    @Test
    void shouldConfigureKafkaWithSensibleDefaults() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addEnv("KAFKA_BROKER_ID", "1");
        verify(instance).addEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        verify(instance).addEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
        verify(instance).addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        verify(instance).addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        verify(instance).addEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
        verify(instance).addEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");
        verify(instance).addEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
        verify(instance).addEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
        verify(instance).addEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }

    @Test
    void shouldConfigureKafkaListeners() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance)
                .addEnv(
                        "KAFKA_LISTENERS",
                        "SERVICE_NETWORK://0.0.0.0:9092,TEST_NETWORK://0.0.0.0:9093");
        verify(instance)
                .addEnv(
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "SERVICE_NETWORK:PLAINTEXT,TEST_NETWORK:PLAINTEXT");
        verify(instance).addEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "SERVICE_NETWORK");
        verify(instance).addEnv("KAFKA_ADVERTISED_LISTENERS", SERVICE_NWK_BOOTSTRAP);
    }

    @Test
    void shouldConfigureExposedPort() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addExposedPorts(9093);
    }

    @Test
    void shouldTweakKafkaListenersOnceStarted() {
        // Given:
        when(instance.execOnInstance(any())).thenReturn(execResult(0, "", ""));

        // When:
        def.instanceStarted(instance);

        // Then:
        verify(instance)
                .execOnInstance(
                        "kafka-configs",
                        "--alter",
                        "--bootstrap-server",
                        SERVICE_NWK_BOOTSTRAP,
                        "--entity-type",
                        "brokers",
                        "--entity-name",
                        "1",
                        "--add-config",
                        "advertised.listeners=["
                                + TEST_NWK_BOOTSTRAP
                                + ","
                                + SERVICE_NWK_BOOTSTRAP
                                + "]");
    }

    @Test
    void shouldThrowIfTweakFailed() {
        // Given:
        final ExecResult execResult = execResult(1, "normal output", "error output");
        when(instance.execOnInstance(any())).thenReturn(execResult);

        // When:
        final Exception e =
                assertThrows(RuntimeException.class, () -> def.instanceStarted(instance));

        // Then:
        assertThat(
                e.getMessage(),
                is(
                        "Failed to configure Kafka's advertised listeners. 'kafka-configs''s exec result: "
                                + execResult));
    }

    private List<String> cmdLine() {
        verify(instance).setCommand(cmdCaptor.capture());
        final List<String> cmd = cmdCaptor.getAllValues();
        assertThat(cmd, hasSize(3));
        return cmd;
    }
}
