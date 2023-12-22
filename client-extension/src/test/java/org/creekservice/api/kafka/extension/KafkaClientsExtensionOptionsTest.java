/*
 * Copyright 2022-2023 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class KafkaClientsExtensionOptionsTest {

    private KafkaClientsExtensionOptions.Builder builder;

    @BeforeEach
    void setUp() {
        builder = KafkaClientsExtensionOptions.builder();
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        KafkaClientsExtensionOptions.builder().build(),
                        KafkaClientsExtensionOptions.builder().build())
                .addEqualityGroup(
                        KafkaClientsExtensionOptions.builder()
                                .withKafkaPropertiesOverrides(clusterNames -> Map.of())
                                .build())
                .addEqualityGroup(
                        KafkaClientsExtensionOptions.builder().withKafkaProperty("k", "v").build())
                .addEqualityGroup(
                        KafkaClientsExtensionOptions.builder()
                                .withTypeOverride(String.class, "diff")
                                .build())
                .testEquals();
    }

    @Test
    void shouldThrowNPEs() {
        final NullPointerTester tester =
                new NullPointerTester().setDefault(String.class, "not empty");

        tester.testAllPublicInstanceMethods(KafkaClientsExtensionOptions.builder());
        tester.testAllPublicInstanceMethods(KafkaClientsExtensionOptions.builder().build());
    }

    @Test
    void shouldDefaultToEarliest() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build(Set.of())
                        .get("any")
                        .get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                is("earliest"));
    }

    @Test
    void shouldDefaultToAllAcks() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build(Set.of())
                        .get("any")
                        .get(ProducerConfig.ACKS_CONFIG),
                is("all"));
    }

    @Test
    void shouldDefaultToSnappyCompression() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build(Set.of())
                        .get("any")
                        .get(ProducerConfig.COMPRESSION_TYPE_CONFIG),
                is("snappy"));
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "localhost:8766")
    void shouldLoadKafkaPropertyOverridesFromTheEnvironmentByDefault() {
        // Given:
        final KafkaClientsExtensionOptions options = builder.build();

        // When:
        final ClustersProperties props = options.propertiesBuilder().build(Set.of());

        // Then:
        assertThat(props.get("any"), hasEntry("bootstrap.servers", "localhost:8766"));
    }

    @Test
    void shouldLoadKafkaPropertyOverridesFromAlternateProvider() {
        // Given:
        final KafkaPropertyOverrides overridesProvider = cluster -> Map.of("a", "b");
        final KafkaClientsExtensionOptions options =
                builder.withKafkaPropertiesOverrides(overridesProvider).build();

        // When:
        final ClustersProperties props = options.propertiesBuilder().build(Set.of());

        // Then:
        assertThat(props.get("any"), hasEntry("a", "b"));
    }

    @Test
    void shouldSetKafkaProperty() {
        // When:
        builder.withKafkaProperty("name", "value");

        // Then:
        assertThat(
                builder.build().propertiesBuilder().build(Set.of()).get("any"),
                hasEntry("name", "value"));
    }

    @Test
    void shouldSetKafkaPropertyForSpecificCluster() {
        // When:
        builder.withKafkaProperty("bob", "name", "value");

        // Then:
        assertThat(
                builder.build().propertiesBuilder().build(Set.of()).get("bob"),
                hasEntry("name", "value"));
        assertThat(
                builder.build().propertiesBuilder().build(Set.of()).get("any"),
                not(hasEntry("name", "value")));
    }
}
