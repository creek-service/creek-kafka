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

import static org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions.DEFAULT_STREAMS_CLOSE_TIMEOUT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import java.time.Duration;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.creekservice.api.kafka.common.config.KafkaPropertyOverrides;
import org.creekservice.api.kafka.common.config.SystemEnvPropertyOverrides;
import org.creekservice.api.kafka.streams.extension.exception.StreamsExceptionHandlers;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creekservice.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultLifecycleObserver;
import org.creekservice.internal.kafka.streams.extension.observation.DefaultStateRestoreObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
                                        (final Set<String> clusterNames) -> null)
                                .build())
                .addEqualityGroup(
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
    void shouldThrowNPEs() {
        final NullPointerTester tester =
                new NullPointerTester().setDefault(String.class, "not empty");

        tester.testAllPublicInstanceMethods(KafkaStreamsExtensionOptions.builder());
        tester.testAllPublicInstanceMethods(KafkaStreamsExtensionOptions.builder().build());
    }

    @Test
    void shouldDefaultToEarliest() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build()
                        .get("any")
                        .get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                is("earliest"));
    }

    @Test
    void shouldDefaultToAllAcks() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build()
                        .get("any")
                        .get(ProducerConfig.ACKS_CONFIG),
                is("all"));
    }

    @Test
    void shouldDefaultToSnappyCompression() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build()
                        .get("any")
                        .get(ProducerConfig.COMPRESSION_TYPE_CONFIG),
                is("snappy"));
    }

    @Test
    void shouldDefaultToThreeReplicasForInternalStreamsTopics() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build()
                        .get("any")
                        .get(StreamsConfig.REPLICATION_FACTOR_CONFIG),
                is(3));
    }

    @Test
    void shouldDefaultToExactlyOnce() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build()
                        .get("any")
                        .get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
                is(StreamsConfig.EXACTLY_ONCE_BETA));
    }

    @Test
    void shouldDefaultToCommitInterval() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build()
                        .get("any")
                        .get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG),
                is(1000));
    }

    @Test
    void shouldDefaultExceptionHandler() {
        assertThat(
                builder.build()
                        .propertiesBuilder()
                        .build()
                        .get("any")
                        .get(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG),
                is(StreamsExceptionHandlers.LogAndFailProductionExceptionHandler.class));
    }

    @Test
    void shouldLoadKafkaPropertyOverridesFromTheEnvironmentByDefault() {
        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.propertyOverrides(), is(instanceOf(SystemEnvPropertyOverrides.class)));
    }

    @Test
    void shouldLoadKafkaPropertyOverridesFromAlternateProvider() {
        // Given:
        final KafkaPropertyOverrides overridesProvider = mock(KafkaPropertyOverrides.class);

        // When:
        builder.withKafkaPropertiesOverrides(overridesProvider);

        // Then:
        assertThat(builder.build().propertyOverrides(), is(sameInstance(overridesProvider)));
    }

    @Test
    void shouldSetKafkaProperty() {
        // When:
        builder.withKafkaProperty("name", "value");

        // Then:
        assertThat(
                builder.build().propertiesBuilder().build().get("any"), hasEntry("name", "value"));
    }

    @Test
    void shouldSetKafkaPropertyForSpecificCluster() {
        // When:
        builder.withKafkaProperty("bob", "name", "value");

        // Then:
        assertThat(
                builder.build().propertiesBuilder().build().get("bob"), hasEntry("name", "value"));
        assertThat(
                builder.build().propertiesBuilder().build().get("any"),
                not(hasEntry("name", "value")));
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
