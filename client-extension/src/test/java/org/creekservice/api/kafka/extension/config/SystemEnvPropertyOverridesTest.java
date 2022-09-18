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

package org.creekservice.api.kafka.extension.config;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.creekservice.api.kafka.extension.config.ClustersProperties.propertiesBuilder;
import static org.creekservice.api.kafka.extension.config.SystemEnvPropertyOverrides.systemEnvPropertyOverrides;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable.SetEnvironmentVariables;

class SystemEnvPropertyOverridesTest {

    @Test
    void shouldSupportCommonKafkaProperties() {
        // Given:
        final SystemEnvPropertyOverrides provider =
                new SystemEnvPropertyOverrides(
                        Map.of(
                                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                                "KAFKA_WHAT_EVER", "meh"));

        // When:
        final ClustersProperties properties = provider.get(Set.of());

        // Then:
        assertThat(
                properties,
                is(
                        propertiesBuilder()
                                .putCommon(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                                .putCommon("what.ever", "meh")
                                .build()));
    }

    @Test
    void shouldSupportSpecificKafkaProperties() {
        // Given:
        final SystemEnvPropertyOverrides provider =
                new SystemEnvPropertyOverrides(
                        Map.of(
                                "KAFKA_CUSTOM_BOOTSTRAP_SERVERS", "localhost:9092",
                                "KAFKA_CUSTOM_WHAT_EVER", "meh"));

        // When:
        final ClustersProperties properties = provider.get(Set.of("custom"));

        // Then:
        assertThat(
                properties,
                is(
                        propertiesBuilder()
                                .put("custom", BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                                .put("custom", "what.ever", "meh")
                                .build()));
    }

    @Test
    void shouldSupportMultipleClusterNames() {
        // Given:
        final SystemEnvPropertyOverrides provider =
                new SystemEnvPropertyOverrides(
                        Map.of(
                                "KAFKA_BOB_BOOTSTRAP_SERVERS", "localhost:9092",
                                "KAFKA_JANE_WHAT_EVER", "meh",
                                "KAFKA_WHAT_EVER", "ha"));

        // When:
        final ClustersProperties properties = provider.get(Set.of("jane", "bob"));

        // Then:
        assertThat(
                properties,
                is(
                        propertiesBuilder()
                                .put("bob", BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                                .put("jane", "what.ever", "meh")
                                .putCommon("what.ever", "ha")
                                .build()));
    }

    @Test
    void shouldFilterOutAnyThatMatchOtherPrefix() {
        // Given:
        final SystemEnvPropertyOverrides provider =
                new SystemEnvPropertyOverrides(
                        Map.of(
                                "KAFKA_BOB_BOOTSTRAP_SERVERS", "localhost:9092",
                                "KAFKA_BOB_TWO_WHAT_EVER", "meh"));

        // When:
        final ClustersProperties properties = provider.get(Set.of("bob", "bob-two"));

        // Then:
        assertThat(
                properties,
                is(
                        propertiesBuilder()
                                .put("bob", BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                                .put("bob-two", "what.ever", "meh")
                                .build()));
    }

    @Test
    void shouldIgnoreEmptyPropertyNames() {
        // Given:
        final SystemEnvPropertyOverrides provider =
                new SystemEnvPropertyOverrides(Map.of("KAFKA_", "blah", "KAFKA_SPECIFIC_", "blah"));

        // When:
        final ClustersProperties properties = provider.get(Set.of("specific"));

        // Then:
        assertThat(properties, is(propertiesBuilder().build()));
    }

    @Test
    void shouldIgnoreNonPrefixedProperties() {
        // Given:
        final SystemEnvPropertyOverrides provider =
                new SystemEnvPropertyOverrides(Map.of("NOT_KAFKA_PREFIXED", "blah"));

        // When:
        final ClustersProperties properties = provider.get(Set.of());

        // Then:
        assertThat(properties, is(propertiesBuilder().build()));
    }

    @Test
    void shouldIgnoreBlankClusterName() {
        // Given:
        final SystemEnvPropertyOverrides provider =
                new SystemEnvPropertyOverrides(
                        Map.of("KAFKA_PROP", "v1", "KAFKA__WEIRD_PROP", "v2"));

        // When:
        final ClustersProperties properties = provider.get(Set.of());

        // Then:
        assertThat(
                properties,
                is(
                        propertiesBuilder()
                                .putCommon("prop", "v1")
                                .putCommon(".weird.prop", "v2")
                                .build()));
    }

    @Test
    @SetEnvironmentVariables({
        @SetEnvironmentVariable(key = "KAFKA_CUSTOM_BOOTSTRAP_SERVERS", value = "localhost:9092"),
        @SetEnvironmentVariable(key = "KAFKA_GROUP_ID", value = "a-group")
    })
    void shouldLoadPropertiesFromEnvironment() {
        // When:
        final ClustersProperties properties = systemEnvPropertyOverrides().get(Set.of("Custom"));

        // Then:
        assertThat(properties.get(""), is(Map.of(GROUP_ID_CONFIG, "a-group")));

        assertThat(
                properties.get("Custom"),
                is(Map.of(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", GROUP_ID_CONFIG, "a-group")));
    }
}
