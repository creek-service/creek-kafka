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

package org.creekservice.api.kafka.streams.extension;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.creekservice.api.kafka.common.config.ClustersProperties.propertiesBuilder;
import static org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.DEFAULT_CLUSTER_NAME;
import static org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions.DEFAULT_STREAMS_CLOSE_TIMEOUT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.testing.EqualsTester;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.creekservice.api.kafka.streams.extension.exception.StreamsExceptionHandlers;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creekservice.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultLifecycleObserver;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultStateRestoreObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

class KafkaStreamsExtensionOptionsTest {

    private KafkaStreamsExtensionOptions.Builder builder;

    @BeforeEach
    void setUp() {
        builder = KafkaStreamsExtensionOptions.builder();
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        KafkaStreamsExtensionOptions.builder().build(),
                        KafkaStreamsExtensionOptions.builder()
                                .withStreamsCloseTimeout(DEFAULT_STREAMS_CLOSE_TIMEOUT)
                                .withLifecycleObserver(new DefaultLifecycleObserver())
                                .withStateRestoreObserver(new DefaultStateRestoreObserver())
                                .build())
                .addEqualityGroup(
                        KafkaStreamsExtensionOptions.builder()
                                .withKafkaPropertiesOverrides(
                                        () -> propertiesBuilder().put("default", "k", "v").build())
                                .build(),
                        KafkaStreamsExtensionOptions.builder()
                                .withKafkaProperty("default", "k", "v")
                                .build())
                .addEqualityGroup(
                        KafkaStreamsExtensionOptions.builder()
                                .withKafkaPropertiesOverrides(
                                        () -> propertiesBuilder().putCommon("k", "v").build())
                                .build(),
                        KafkaStreamsExtensionOptions.builder().withKafkaProperty("k", "v").build())
                .addEqualityGroup(
                        KafkaStreamsExtensionOptions.builder()
                                .withStreamsCloseTimeout(Duration.ofSeconds(1))
                                .build())
                .addEqualityGroup(
                        KafkaStreamsExtensionOptions.builder()
                                .withLifecycleObserver(mock(LifecycleObserver.class))
                                .build())
                .addEqualityGroup(
                        KafkaStreamsExtensionOptions.builder()
                                .withStateRestoreObserver(mock(StateRestoreObserver.class))
                                .build())
                .addEqualityGroup(
                        KafkaStreamsExtensionOptions.builder()
                                .withMetricsPublishing(mock(KafkaMetricsPublisherOptions.class))
                                .build())
                .testEquals();
    }

    @Test
    void shouldDefaultToEarliest() {
        assertThat(
                builder.build().properties("any").get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                is("earliest"));
    }

    @Test
    void shouldDefaultToAllAcks() {
        assertThat(builder.build().properties("any").get(ProducerConfig.ACKS_CONFIG), is("all"));
    }

    @Test
    void shouldDefaultToSnappyCompression() {
        assertThat(
                builder.build().properties("any").get(ProducerConfig.COMPRESSION_TYPE_CONFIG),
                is("snappy"));
    }

    @Test
    void shouldDefaultToThreeReplicasForInternalStreamsTopics() {
        assertThat(
                builder.build().properties("any").get(StreamsConfig.REPLICATION_FACTOR_CONFIG),
                is(3));
    }

    @Test
    void shouldDefaultToExactlyOnce() {
        assertThat(
                builder.build().properties("any").get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
                is(StreamsConfig.EXACTLY_ONCE_BETA));
    }

    @Test
    void shouldDefaultToCommitInterval() {
        assertThat(
                builder.build().properties("any").get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG),
                is(1000));
    }

    @Test
    void shouldDefaultExceptionHandler() {
        assertThat(
                builder.build()
                        .properties("any")
                        .get(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG),
                is(StreamsExceptionHandlers.LogAndFailProductionExceptionHandler.class));
    }

    @Test
    @SetEnvironmentVariable.SetEnvironmentVariables({
        @SetEnvironmentVariable(key = "KAFKA_DEFAULT_BOOTSTRAP_SERVERS", value = "localhost:9092"),
        @SetEnvironmentVariable(key = "KAFKA_OTHER_ACKS", value = "1")
    })
    void shouldLoadKafkaPropertyOverridesFromTheEnvironmentByDefault() {
        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(
                options.properties(DEFAULT_CLUSTER_NAME),
                hasEntry(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
        assertThat(options.properties("other"), hasEntry(ProducerConfig.ACKS_CONFIG, "1"));
    }

    @Test
    void shouldLoadKafkaPropertyOverridesFromAlternateProvider() {
        // Given:
        builder.withKafkaPropertiesOverrides(
                () -> propertiesBuilder().put("bob", "name", "value").build());

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.properties("bob"), hasEntry("name", "value"));
    }

    @Test
    void shouldSetKafkaProperty() {
        // Given:
        builder.withKafkaProperty("name", "value");

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.properties(DEFAULT_CLUSTER_NAME), hasEntry("name", "value"));
    }

    @Test
    void shouldSetKafkaPropertyForSpecificCluster() {
        // Given:
        builder.withKafkaProperty("bob", "name", "value");

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.properties("bob"), hasEntry("name", "value"));
    }

    @Test
    void shouldExposeKafkaPropertiesAsMap() {
        // Given:
        builder.withKafkaProperty("name", "value");

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.propertyMap(DEFAULT_CLUSTER_NAME), hasEntry("name", "value"));
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_DEFAULT_BOOTSTRAP_SERVERS", value = "localhost:9092")
    void shouldOverrideSetKafkaProperties() {
        // Given:
        builder.withKafkaProperty(BOOTSTRAP_SERVERS_CONFIG, "wrong!");

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(
                options.properties(DEFAULT_CLUSTER_NAME),
                hasEntry(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
    }

    @Test
    void shouldNotExposeMutableProperties() {
        // Given:
        builder.withKafkaProperty("name", "value");
        final KafkaStreamsExtensionOptions options = builder.build();

        // When:
        options.properties(DEFAULT_CLUSTER_NAME).put("mutate", 10);

        // Then:
        assertThat(options.properties(DEFAULT_CLUSTER_NAME), not(hasKey("mutate")));
    }

    @Test
    void shouldDefaultCloseTimeout() {
        assertThat(builder.build().streamsCloseTimeout(), is(DEFAULT_STREAMS_CLOSE_TIMEOUT));
    }

    @Test
    void shouldCustomizeCloseTimeout() {
        // Given:
        builder.withStreamsCloseTimeout(Duration.ofHours(1));

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.streamsCloseTimeout(), is(Duration.ofHours(1)));
    }

    @Test
    void shouldDefaultLifecycleObserver() {
        assertThat(builder.build().lifecycleObserver(), is(new DefaultLifecycleObserver()));
    }

    @Test
    void shouldCustomizeLifecycleObserver() {
        // Given:
        final LifecycleObserver observer = mock(LifecycleObserver.class);
        builder.withLifecycleObserver(observer);

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.lifecycleObserver(), is(observer));
    }

    @Test
    void shouldDefaultRestoreObserver() {
        assertThat(builder.build().restoreObserver(), is(new DefaultStateRestoreObserver()));
    }

    @Test
    void shouldCustomizeRestoreObserver() {
        // Given:
        final StateRestoreObserver observer = mock(StateRestoreObserver.class);
        builder.withStateRestoreObserver(observer);

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.restoreObserver(), is(observer));
    }

    @Test
    void shouldDefaultMetricsPublishing() {
        assertThat(
                builder.build().metricsPublishing(),
                is(KafkaMetricsPublisherOptions.builder().build()));
    }

    @Test
    void shouldCustomizeMetricsPublishing() {
        // Given:
        final KafkaMetricsPublisherOptions metrics = mock(KafkaMetricsPublisherOptions.class);
        builder.withMetricsPublishing(metrics);

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.metricsPublishing(), is(metrics));
    }

    @Test
    void shouldCustomizeMetricsPublishingWithBuilder() {
        // Given:
        final KafkaMetricsPublisherOptions metrics = mock(KafkaMetricsPublisherOptions.class);
        final KafkaMetricsPublisherOptions.Builder metricsBuilder =
                mock(KafkaMetricsPublisherOptions.Builder.class);
        when(metricsBuilder.build()).thenReturn(metrics);

        // When:
        final KafkaStreamsExtensionOptions options =
                builder.withMetricsPublishing(metricsBuilder).build();

        // Then:
        assertThat(options.metricsPublishing(), is(metrics));
    }
}
