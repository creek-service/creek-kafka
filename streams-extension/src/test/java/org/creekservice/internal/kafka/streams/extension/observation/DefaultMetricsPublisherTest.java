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

package org.creekservice.internal.kafka.streams.extension.observation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsFilter;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@SuppressWarnings("ResultOfMethodCallIgnored")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DefaultMetricsPublisherTest {

    private static final Duration PERIOD = Duration.ofSeconds(10);
    private static final MetricName NAME_1 = new MetricName("m1", "g1", "d1", Map.of("tk0", "tv0"));
    private static final MetricName NAME_2 = new MetricName("m2", "g1", "d1", Map.of());

    @Mock private KafkaMetricsPublisherOptions options;
    @Mock private ScheduledExecutorService scheduler;
    @Mock private Supplier<Map<MetricName, ? extends Metric>> metricsSupplier;
    @Mock private KafkaMetricsFilter filter;
    @Mock private Metric metric1;
    @Mock private Metric metric2;
    private final TestStructuredLogger logger = TestStructuredLogger.create();
    private DefaultMetricsPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new DefaultMetricsPublisher(options, logger, scheduler);

        when(options.publishPeriod()).thenReturn(PERIOD);
        when(options.metricsFilter()).thenReturn(filter);

        when(filter.includeMetric(any())).thenReturn(true);
        when(filter.includeValue(any())).thenReturn(true);

        when(metric1.metricValue()).thenReturn("v1");
        when(metric2.metricValue()).thenReturn("v2");

        givenMetrics(Map.of(NAME_1, metric1, NAME_2, metric2));
    }

    @Test
    void shouldScheduleLogPublishing() {
        // When:
        publisher.schedule(metricsSupplier);

        // Then:
        verify(scheduler)
                .scheduleAtFixedRate(
                        any(),
                        eq(PERIOD.toMillis()),
                        eq(PERIOD.toMillis()),
                        eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldNotScheduleIfDisabled() {
        // Given:
        when(options.publishPeriod()).thenReturn(Duration.ZERO);

        // When:
        publisher.schedule(metricsSupplier);

        // Then:
        verify(scheduler, never()).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
    }

    @Test
    void shouldLogEmptyMetrics() {
        // Given:
        givenMetrics(Map.of());
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(logger.textEntries(), contains("INFO: {message=Kafka metrics}"));
    }

    @Test
    void shouldLogPublishingFailures() {
        // Given:
        when(metricsSupplier.get()).thenThrow(new RuntimeException("Boom"));
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(
                logger.textEntries(),
                contains(
                        "ERROR: {message=Logging of Kafka metrics failed}"
                                + " java.lang.RuntimeException: Boom"));
    }

    @Test
    void shouldShutdownAndAwaitTerminationOnClose() throws Exception {
        // Given:
        when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);

        // When:
        publisher.close();

        // Then:
        final InOrder inOrder = inOrder(scheduler);
        inOrder.verify(scheduler).shutdown();
        inOrder.verify(scheduler).awaitTermination(PERIOD.toMillis() * 2, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldNotLogWarningOnCleanClose() throws Exception {
        // Given:
        when(scheduler.awaitTermination(anyLong(), any())).thenReturn(true);

        // When:
        publisher.close();

        // Then:
        assertThat(logger.textEntries(), is(empty()));
    }

    @Test
    void shouldLogWarningOnCloseTimeout() throws Exception {
        // Given:
        when(scheduler.awaitTermination(anyLong(), any())).thenReturn(false);

        // When:
        publisher.close();

        // Then:
        assertThat(
                logger.textEntries(),
                contains("WARN: {message=Timed out shutting down scheduler}"));
    }

    @Test
    void shouldInterrupt() throws Exception {
        // Given:
        when(scheduler.awaitTermination(anyLong(), any())).thenThrow(new InterruptedException());

        // When:
        publisher.close();

        // Then:
        assertThat("should be interrupted", Thread.currentThread().isInterrupted());
    }

    @Test
    void shouldLogMetrics() {
        // Given:
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(
                logger.textEntries(), contains("INFO: {g1={m1=v1, m2=v2}, message=Kafka metrics}"));
    }

    @Test
    void shouldFilterMetricsByName() {
        // Given:
        when(filter.includeMetric(NAME_2)).thenReturn(false);
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(logger.textEntries().get(0), not(containsString("m2=v2")));
    }

    @Test
    void shouldFilterMetricsByValue() {
        // Given:
        when(filter.includeValue("v1")).thenReturn(false);
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(logger.textEntries().get(0), not(containsString("m1=v1")));
    }

    @Test
    void shouldNamespaceMetricsByGroup() {
        // Given:
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(logger.textEntries().get(0), containsString("g1={m1=v1, m2=v2}"));
    }

    @Test
    void shouldNamespaceMetricsByTags() {
        // Given:
        when(filter.filterTags(NAME_1.tags())).thenReturn(Stream.of("ns1", "ns2"));
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(logger.textEntries().get(0), containsString("ns1={ns2={m1=v1}}"));
    }

    @Test
    void shouldSanitizeMetricName() {
        // Given:
        givenMetrics(Map.of(new MetricName("With$Invalid:Chars", "g1", "d", Map.of()), metric1));
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(logger.textEntries().get(0), containsString("With_Invalid_Chars=v1"));
    }

    @Test
    void shouldSanitizeMetricGroup() {
        // Given:
        givenMetrics(Map.of(new MetricName("m1", "With$Invalid:Chars", "d", Map.of()), metric1));
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(logger.textEntries().get(0), containsString("With_Invalid_Chars={m1=v1}"));
    }

    @Test
    void shouldSanitizeNamespaces() {
        // Given:
        when(filter.filterTags(NAME_1.tags())).thenReturn(Stream.of("With$Invalid:Chars"));
        final Runnable logTask = givenScheduled();

        // When:
        logTask.run();

        // Then:
        assertThat(logger.textEntries().get(0), containsString("With_Invalid_Chars={m1=v1}"));
    }

    private Runnable givenScheduled() {
        final ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        publisher.schedule(metricsSupplier);
        verify(scheduler).scheduleAtFixedRate(taskCaptor.capture(), anyLong(), anyLong(), any());
        return taskCaptor.getValue();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void givenMetrics(final Map<MetricName, Metric> metrics) {
        when(metricsSupplier.get()).thenReturn((Map) metrics);
    }
}
