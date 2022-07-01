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
import static org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions.DEFAULT_STREAMS_CLOSE_TIMEOUT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.testing.EqualsTester;
import java.time.Duration;
import java.util.Map;
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
                                .withKafkaPropertiesOverrides(() -> Map.of("k", "v"))
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
    void shouldDefaultToNoKafkaPropertiesForNow() {
        assertThat(builder.build().properties().keySet(), is(empty()));
        assertThat(builder.build().propertyMap().keySet(), is(empty()));
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "localhost:9092")
    void shouldLoadKafkaPropertyOverridesFromTheEnvironmentByDefault() {
        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.properties(), hasEntry(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
    }

    @Test
    void shouldLoadKafkaPropertyOverridesFromAlternateProvider() {
        // Given:
        builder.withKafkaPropertiesOverrides(() -> Map.of("name", "value"));

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.properties(), hasEntry("name", "value"));
    }

    @Test
    void shouldSetKafkaProperty() {
        // Given:
        builder.withKafkaProperty("name", "value");

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.properties(), hasEntry("name", "value"));
    }

    @Test
    void shouldExposeKafkaPropertiesAsMap() {
        // Given:
        builder.withKafkaProperty("name", "value");

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.propertyMap(), hasEntry("name", "value"));
    }

    @Test
    @SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVERS", value = "localhost:9092")
    void shouldOverrideSetKafkaProperties() {
        // Given:
        builder.withKafkaProperty(BOOTSTRAP_SERVERS_CONFIG, "wrong!");

        // When:
        final KafkaStreamsExtensionOptions options = builder.build();

        // Then:
        assertThat(options.properties(), hasEntry(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
    }

    @Test
    void shouldNotExposeMutableProperties() {
        // Given:
        builder.withKafkaProperty("name", "value");
        final KafkaStreamsExtensionOptions options = builder.build();

        // When:
        options.properties().put("mutate", 10);

        // Then:
        assertThat(options.properties(), not(hasKey("mutate")));
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
