/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

import static org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.EqualsTester;
import java.time.Duration;
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

    private static final String DOCKER_IMAGE = "kafka-docker-image";
    private static final String CLUSTER_NAME = "bob";
    private static final String SERVICE_NWK_NAME = "kafka-bob-2";
    private static final String TEST_NWK_NAME = "localhost";
    private static final String SERVICE_NWK_BOOTSTRAP =
            "SERVICE_NETWORK://" + SERVICE_NWK_NAME + ":9092";
    private static final String TEST_NWK_BOOTSTRAP = "TEST_NETWORK://" + TEST_NWK_NAME + ":123456";

    private static final String RANDOM_CLUSTER_ID = "xYz1aB2cD3eF4gH5iJ6kLm";

    @Mock private ConfigurableServiceInstance instance;
    @Captor private ArgumentCaptor<String[]> cmdCaptor;

    private KafkaContainerDef def;

    @BeforeEach
    void setUp() {
        def = new KafkaContainerDef(CLUSTER_NAME, DOCKER_IMAGE);

        when(instance.serviceNetworkHostname()).thenReturn(SERVICE_NWK_NAME);
        when(instance.testNetworkHostname()).thenReturn(TEST_NWK_NAME);
        when(instance.testNetworkPort(9093)).thenReturn(123456);
        when(instance.addEnv(any(), any())).thenReturn(instance);
        when(instance.addExposedPorts(anyInt())).thenReturn(instance);
        when(instance.setStartupLogMessage(any(), anyInt())).thenReturn(instance);
        when(instance.setStartupTimeout(any())).thenReturn(instance);
        when(instance.setShutdownTimeout(any())).thenReturn(instance);
        when(instance.setCommand(any(String[].class))).thenReturn(instance);
        when(instance.execOnInstance("kafka-storage", "random-uuid"))
                .thenReturn(ExecResult.execResult(0, RANDOM_CLUSTER_ID + "\n", ""));
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        new KafkaContainerDef(CLUSTER_NAME, DOCKER_IMAGE),
                        new KafkaContainerDef(CLUSTER_NAME, DOCKER_IMAGE))
                .addEqualityGroup(new KafkaContainerDef("diff", DOCKER_IMAGE))
                .addEqualityGroup(new KafkaContainerDef(CLUSTER_NAME, "diff"))
                .testEquals();
    }

    @Test
    void shouldExposeName() {
        assertThat(def.name(), is("kafka-bob"));
    }

    @Test
    void shouldSupportDefaultName() {
        // Given:
        def = new KafkaContainerDef(DEFAULT_CLUSTER_NAME, DOCKER_IMAGE);

        // Then:
        assertThat(def.name(), is("kafka-default"));
    }

    @Test
    void shouldUseRightDockerImage() {
        assertThat(def.dockerImage(), is(DOCKER_IMAGE));
    }

    @Test
    void shouldConfigureKRaftMode() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addEnv("KAFKA_NODE_ID", "1");
        verify(instance).addEnv("KAFKA_PROCESS_ROLES", "broker,controller");
        verify(instance).addEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER");
        verify(instance).addEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094");
    }

    @Test
    void shouldConfigureKafkaToWaitForStartScript() {
        // When:
        def.configureInstance(instance);

        // Then:
        final List<String> cmd = cmdLine();
        assertThat(cmd.get(0), is("sh"));
        assertThat(cmd.get(1), is("-c"));
        assertThat(cmd.get(2), containsString("while [ ! -f /tmp/testcontainers_start.sh ]"));
        assertThat(cmd.get(2), containsString("bash /tmp/testcontainers_start.sh"));
    }

    @Test
    void shouldConfigureKafkaWithSensibleDefaults() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addEnv("KAFKA_NODE_ID", "1");
        verify(instance).addEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        verify(instance).addEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
        verify(instance).addEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1");
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
                        "SERVICE_NETWORK://0.0.0.0:9092,TEST_NETWORK://0.0.0.0:9093,CONTROLLER://localhost:9094");
        verify(instance)
                .addEnv(
                        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "SERVICE_NETWORK:PLAINTEXT,TEST_NETWORK:PLAINTEXT,CONTROLLER:PLAINTEXT");
        verify(instance).addEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "SERVICE_NETWORK");
    }

    @Test
    void shouldConfigureExposedPort() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).addExposedPorts(9093);
    }

    @Test
    void shouldConfigureStartupLogWaitStrategy() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).setStartupLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1);
    }

    @Test
    void shouldSetNinetySecondStartupTimeout() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).setStartupTimeout(Duration.ofSeconds(90));
    }

    @Test
    void shouldConfigureQuickShutdownTimeoutToKeepTestsSpeedy() {
        // When:
        def.configureInstance(instance);

        // Then:
        verify(instance).setShutdownTimeout(Duration.ofSeconds(1));
    }

    @Test
    void shouldWriteStartScriptWithAdvertisedListenersWhenStarting() {
        // When:
        def.instanceStarting(instance);

        // Then:
        final String expectedScript =
                "#!/bin/bash\n"
                        + "set -e\n"
                        + "export CLUSTER_ID="
                        + RANDOM_CLUSTER_ID
                        + "\n"
                        + "export KAFKA_ADVERTISED_LISTENERS="
                        + TEST_NWK_BOOTSTRAP
                        + ","
                        + SERVICE_NWK_BOOTSTRAP
                        + "\n"
                        + "/etc/confluent/docker/configure\n"
                        + "kafka-storage format --ignore-formatted -t \""
                        + RANDOM_CLUSTER_ID
                        + "\" -c /etc/kafka/kafka.properties\n"
                        + "exec /etc/confluent/docker/launch\n";
        verify(instance)
                .copyFileToContainer(
                        eq(expectedScript), eq("/tmp/testcontainers_start.sh"), eq(true));
    }

    private List<String> cmdLine() {
        verify(instance).setCommand(cmdCaptor.capture());
        final List<String[]> cmd = cmdCaptor.getAllValues();
        assertThat(cmd, hasSize(1));
        final List<String> args = List.of(cmd.get(0));
        assertThat(args, hasSize(3));
        return args;
    }
}
