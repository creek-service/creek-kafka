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

package org.creekservice.internal.kafka.streams.extension;

import static org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver.ExitCode.EXCEPTION_THROWN_STOPPING;
import static org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver.ExitCode.OK;
import static org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver.ExitCode.STREAMS_TIMED_OUT_CLOSING;
import static org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver.ExitCode.STREAM_APP_FAILED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;
import org.creekservice.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creekservice.api.kafka.streams.extension.observation.KafkaMetricsPublisherOptions;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creekservice.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.creekservice.internal.kafka.streams.extension.KafkaStreamsExecutor.MetricsPublisherFactory;
import org.creekservice.internal.kafka.streams.extension.observation.MetricsPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaStreamsExecutorTest {

    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(124456);

    @Mock private LifecycleObserver lifecycleObserver;
    @Mock private StateRestoreObserver restoreObserver;
    @Mock private MetricsPublisherFactory metricsPublisherFactory;
    @Mock private MetricsPublisher metricsPublisher;
    @Mock private StreamsShutdownHook shutdownHook;
    @Mock private KafkaStreamsExecutor.ShutdownMethod shutdownMethod;
    @Mock private KafkaMetricsPublisherOptions metricsOptions;
    @Mock private Runnable loggingCloseDelay;
    @Mock private KafkaStreams app;

    @Captor
    private ArgumentCaptor<Supplier<Map<MetricName, ? extends Metric>>> metricsSupplierCaptor;

    private CompletableFuture<Void> shutdownFuture;
    private KafkaStreamsExecutor executor;

    @BeforeEach
    void setUp() {
        shutdownFuture = new CompletableFuture<>();

        final KafkaStreamsExtensionOptions options =
                KafkaStreamsExtensionOptions.builder()
                        .withStateRestoreObserver(restoreObserver)
                        .withLifecycleObserver(lifecycleObserver)
                        .withStreamsCloseTimeout(CLOSE_TIMEOUT)
                        .withMetricsPublishing(metricsOptions)
                        .build();

        executor =
                new KafkaStreamsExecutor(
                        options,
                        metricsPublisherFactory,
                        shutdownHook,
                        shutdownMethod,
                        shutdownFuture,
                        loggingCloseDelay);

        when(app.close(any(Duration.class))).thenReturn(true);
        when(metricsPublisherFactory.create(any())).thenReturn(metricsPublisher);
    }

    @Test
    void shouldObserveStarting() {
        // Given:
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(app, lifecycleObserver);
        inOrder.verify(lifecycleObserver).starting();
        inOrder.verify(app).start();
    }

    @Test
    void shouldAddLifecycleStateListener() {
        // Given:
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(app);
        inOrder.verify(app).setStateListener(isA(StreamsStateListener.class));
        inOrder.verify(app).start();
    }

    @Test
    void shouldAddStateRestoreListener() {
        // Given:
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(app);
        inOrder.verify(app).setGlobalStateRestoreListener(isA(RestoreListener.class));
        inOrder.verify(app).start();
    }

    @Test
    void shouldAddShutdownHook() {
        // Given:
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(app, shutdownHook);
        inOrder.verify(shutdownHook).apply(app, shutdownFuture);
        inOrder.verify(app).start();
    }

    @Test
    void shouldScheduleMetricsPublishing() {
        // Given:
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(app, metricsPublisherFactory, metricsPublisher);
        inOrder.verify(metricsPublisherFactory).create(metricsOptions);
        inOrder.verify(metricsPublisher).schedule(any());
        inOrder.verify(app).start();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldGetFreshMetricsOnEachPublish() {
        // Given:
        final Supplier<Map<MetricName, ? extends Metric>> supplier = metricsSupplier();

        final Metric metric = mock(Metric.class);
        final Map<MetricName, Metric> metrics1 =
                Map.of(new MetricName("A", "g", "d", Map.of()), metric);
        final Map<MetricName, Metric> metrics2 =
                Map.of(new MetricName("B", "g", "d", Map.of()), metric);

        when(app.metrics()).thenReturn((Map) metrics1, (Map) metrics2);

        // When:
        final Map<MetricName, ? extends Metric> result1 = supplier.get();
        final Map<MetricName, ? extends Metric> result2 = supplier.get();

        // Then:
        verify(app, times(2)).metrics();
        assertThat(result1, is(metrics1));
        assertThat(result2, is(metrics2));
    }

    @Test
    void shouldCloseMetricsPublisher() {
        // Given:
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(lifecycleObserver, metricsPublisher);
        inOrder.verify(lifecycleObserver).stopped(any());
        inOrder.verify(metricsPublisher).close();
    }

    @Test
    void shouldCloseMetricsPublisherOnFailure() {
        // Given:
        shutdownFuture.completeExceptionally(new RuntimeException("Big Bada Boom"));

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(lifecycleObserver, metricsPublisher);
        inOrder.verify(lifecycleObserver).stopped(any());
        inOrder.verify(metricsPublisher).close();
    }

    @Test
    void shouldCloseStreamsAppAndExitOnGracefulShutdown() {
        // Given:
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(app, shutdownMethod);
        inOrder.verify(app).close(CLOSE_TIMEOUT);
        inOrder.verify(shutdownMethod).shutdownApp(OK.asInt());
    }

    @Test
    void shouldCloseStreamsAppAndExitOnUngracefulShutdown() {
        // Given:
        shutdownFuture.completeExceptionally(new RuntimeException("Big Bada Boom"));

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(app, shutdownMethod);
        inOrder.verify(app).close(CLOSE_TIMEOUT);
        inOrder.verify(shutdownMethod).shutdownApp(STREAM_APP_FAILED.asInt());
    }

    @Test
    void shouldReportAppCloseTimeout() {
        // Given:
        when(app.close(any(Duration.class))).thenReturn(false);
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        verify(shutdownMethod).shutdownApp(STREAMS_TIMED_OUT_CLOSING.asInt());
    }

    @Test
    void shouldReportExceptionThrownOnShutdown() {
        // Given:
        when(app.close(any(Duration.class))).thenThrow(new RuntimeException("Big Bada Boom"));
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        verify(shutdownMethod).shutdownApp(EXCEPTION_THROWN_STOPPING.asInt());
    }

    @Test
    void shouldObserveFailure() {
        // Given:
        final RuntimeException e = new RuntimeException("Big Bada Boom");
        shutdownFuture.completeExceptionally(e);

        // When:
        executor.execute(app);

        // Then:
        verify(lifecycleObserver).failed(e);
    }

    @Test
    void shouldObserveExceptionThrownOnShutdown() {
        // Given:
        final RuntimeException e = new RuntimeException("Big Bada Boom");
        when(app.close(any(Duration.class))).thenThrow(e);
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        verify(lifecycleObserver).stopFailed(e);
    }

    @Test
    void shouldGiveKafkaStreamsChanceToLogFailureCause() {
        // Given:
        shutdownFuture.completeExceptionally(new RuntimeException("Big Bada Boom"));

        // When:
        executor.execute(app);

        // Then:
        verify(loggingCloseDelay).run();
    }

    private Supplier<Map<MetricName, ? extends Metric>> metricsSupplier() {
        shutdownFuture.complete(null);
        executor.execute(app);
        verify(metricsPublisher).schedule(metricsSupplierCaptor.capture());
        return metricsSupplierCaptor.getValue();
    }
}
