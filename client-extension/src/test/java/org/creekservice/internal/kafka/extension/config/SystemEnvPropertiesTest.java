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

package org.creekservice.internal.kafka.extension.config;

import static org.creekservice.internal.kafka.extension.config.SystemEnvProperties.propertyName;
import static org.creekservice.internal.kafka.extension.config.SystemEnvProperties.varName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Optional;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.Test;

class SystemEnvPropertiesTest {

    @Test
    void shouldRoundTripCommon() {
        // Given:
        final String clusterName = "";
        final String propertyName = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

        // When:
        final String varName = varName(propertyName, clusterName);
        final Optional<String> result = propertyName(varName, clusterName);

        // Then:
        assertThat(result, is(Optional.of(propertyName)));
        assertThat(varName, is("KAFKA_BOOTSTRAP_SERVERS"));
    }

    @Test
    void shouldRoundTripSpecificCluster() {
        // Given:
        final String clusterName = "specific";
        final String propertyName = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

        // When:
        final String varName = varName(propertyName, clusterName);
        final Optional<String> result = propertyName(varName, clusterName);

        // Then:
        assertThat(result, is(Optional.of(propertyName)));
        assertThat(varName, is("KAFKA_SPECIFIC_BOOTSTRAP_SERVERS"));
    }

    @Test
    void shouldHandleClusterNamesWithHyphens() {
        // Given:
        final String clusterName = "hyphenated-name";
        final String propertyName = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;

        // When:
        final String varName = varName(propertyName, clusterName);
        final Optional<String> result = propertyName(varName, clusterName);

        // Then:
        assertThat(result, is(Optional.of(propertyName)));
        assertThat(varName, is("KAFKA_HYPHENATED_NAME_BOOTSTRAP_SERVERS"));
    }

    @Test
    void shouldReturnEmptyIfNotPrefixed() {
        assertThat(propertyName("NOT_KAFKA_PREFIXED", ""), is(Optional.empty()));
        assertThat(propertyName("NOT_KAFKA_BOB_PREFIXED", "bob"), is(Optional.empty()));
    }

    @Test
    void shouldReturnEmptyIfResultWouldBeEmptyPropertyName() {
        assertThat(propertyName("KAFKA_", ""), is(Optional.empty()));
        assertThat(propertyName("KAFKA_BOB_", "bob"), is(Optional.empty()));
    }
}
