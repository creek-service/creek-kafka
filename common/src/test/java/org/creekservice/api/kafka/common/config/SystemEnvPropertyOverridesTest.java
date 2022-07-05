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

package org.creekservice.api.kafka.common.config;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.creekservice.api.kafka.common.config.SystemEnvPropertyOverrides.systemEnvPropertyOverrides;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable.SetEnvironmentVariables;

class SystemEnvPropertyOverridesTest {

    @Test
    @SetEnvironmentVariables({
        @SetEnvironmentVariable(key = "KAFKA_DEFAULT_BOOTSTRAP_SERVERS", value = "localhost:9092"),
        @SetEnvironmentVariable(key = "KAFKA_DEFAULT_GROUP_ID", value = "a-group")
    })
    void shouldLoadPrefixedPropertiesFromEnvironment() {
        // When:
        final ClustersProperties properties = systemEnvPropertyOverrides().get();

        // Then:
        assertThat(
                properties.get("default"),
                is(Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", GROUP_ID_CONFIG, "a-group")));
    }

    @Test
    @SetEnvironmentVariables({
        @SetEnvironmentVariable(key = "KAFKA_BOB1_BOOTSTRAP_SERVERS", value = "localhost:9092"),
        @SetEnvironmentVariable(key = "KAFKA_JANE29_GROUP_ID", value = "a-group")
    })
    void shouldSupportMultipleClusters() {
        // When:
        final ClustersProperties properties = systemEnvPropertyOverrides().get();

        // Then:
        assertThat(properties.get("bob1"), is(Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")));
        assertThat(properties.get("jane29"), is(Map.of(GROUP_ID_CONFIG, "a-group")));
    }

    @Test
    @SetEnvironmentVariable(key = "NOT_KAFKA_PREFIXED", value = "whatever")
    public void shouldIgnoreNonPrefixedProperties() {
        final ClustersProperties properties = systemEnvPropertyOverrides().get();
        assertThat(properties.get("not").entrySet(), is(empty()));
        assertThat(properties.get("kafka").entrySet(), is(empty()));
        assertThat(properties.get("prefixed").entrySet(), is(empty()));
    }
}
