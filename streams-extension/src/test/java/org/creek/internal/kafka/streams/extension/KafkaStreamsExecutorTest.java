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

package org.creek.internal.kafka.streams.extension;

import static org.creek.api.kafka.streams.observation.LifecycleObserver.ExitCode.EXCEPTION_THROWN_STOPPING;
import static org.creek.api.kafka.streams.observation.LifecycleObserver.ExitCode.OK;
import static org.creek.api.kafka.streams.observation.LifecycleObserver.ExitCode.STREAMS_TIMED_OUT_CLOSING;
import static org.creek.api.kafka.streams.observation.LifecycleObserver.ExitCode.STREAM_APP_FAILED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.streams.KafkaStreams;
import org.creek.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creek.api.kafka.streams.observation.LifecycleObserver;
import org.creek.api.kafka.streams.observation.StateRestoreObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
    @Mock private StreamsShutdownHook shutdownHook;
    @Mock private KafkaStreamsExecutor.ShutdownMethod shutdownMethod;
    @Mock private Runnable loggingCloseDelay;
    @Mock private KafkaStreams app;
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
                        .build();

        executor =
                new KafkaStreamsExecutor(
                        options, shutdownHook, shutdownMethod, shutdownFuture, loggingCloseDelay);

        when(app.close(any())).thenReturn(true);
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
    void shouldAddAppStateListener() {
        // Given:
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        final InOrder inOrder = Mockito.inOrder(app);
        inOrder.verify(app).setStateListener(isA(LifecycleListener.class));
        inOrder.verify(app).start();
    }

    @Test
    void shouldAddAppStateRestoreListener() {
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
        when(app.close(any())).thenReturn(false);
        shutdownFuture.complete(null);

        // When:
        executor.execute(app);

        // Then:
        verify(shutdownMethod).shutdownApp(STREAMS_TIMED_OUT_CLOSING.asInt());
    }

    @Test
    void shouldReportExceptionThrownOnShutdown() {
        // Given:
        when(app.close(any())).thenThrow(new RuntimeException("Big Bada Boom"));
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
        when(app.close(any())).thenThrow(e);
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
}
