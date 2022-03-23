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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.streams.KafkaStreams;
import org.creek.api.base.annotation.VisibleForTesting;
import org.creek.api.kafka.streams.extension.KafkaStreamsExtensionOptions;
import org.creek.api.kafka.streams.observation.KafkaMetricsPublisherOptions;
import org.creek.api.kafka.streams.observation.LifecycleObserver;
import org.creek.api.kafka.streams.observation.LifecycleObserver.ExitCode;
import org.creek.internal.kafka.streams.extension.observation.DefaultMetricsPublisher;
import org.creek.internal.kafka.streams.extension.observation.MetricsPublisher;

/** Executes a {@link KafkaStreams} app. */
public final class KafkaStreamsExecutor {

    private static final Duration LOGGING_STREAMS_CLOSE_DELAY = Duration.ofSeconds(1);

    private final KafkaStreamsExtensionOptions options;
    private final MetricsPublisherFactory metricsPublisherFactory;
    private final StreamsShutdownHook shutdownHook;
    private final ShutdownMethod shutdownMethod;
    private final CompletableFuture<Void> shutdownFuture;
    private final Runnable loggingCloseDelay;

    public KafkaStreamsExecutor(final KafkaStreamsExtensionOptions options) {
        this(
                options,
                DefaultMetricsPublisher::new,
                new DefaultStreamsShutdownHook(),
                System::exit,
                new CompletableFuture<>(),
                KafkaStreamsExecutor
                        ::giveStreamsTimeToLogCauseAsLoggingIsDoneAfterStateChangeCallbackIsCalled);
    }

    @VisibleForTesting
    KafkaStreamsExecutor(
            final KafkaStreamsExtensionOptions options,
            final MetricsPublisherFactory metricsPublisherFactory,
            final StreamsShutdownHook shutdownHook,
            final ShutdownMethod shutdownMethod,
            final CompletableFuture<Void> shutdownFuture,
            final Runnable loggingCloseDelay) {
        this.options = requireNonNull(options, "options");
        this.metricsPublisherFactory =
                requireNonNull(metricsPublisherFactory, "metricsPublisherSupplier");
        this.shutdownHook = requireNonNull(shutdownHook, "shutdownHook");
        this.shutdownMethod = requireNonNull(shutdownMethod, "shutdownHandler");
        this.shutdownFuture = requireNonNull(shutdownFuture, "future");
        this.loggingCloseDelay = requireNonNull(loggingCloseDelay, "loggingCloseDelay");
    }

    public void execute(final KafkaStreams streamsApp) {

        final LifecycleObserver observer = options.lifecycleObserver();

        ExitCode exitCode = ExitCode.EXCEPTION_THROWN_STARTING;

        try (MetricsPublisher metricsPublisher =
                metricsPublisherFactory.create(options.metricsPublishing())) {
            observer.starting();

            metricsPublisher.schedule(streamsApp::metrics);

            streamsApp.setStateListener(new LifecycleListener(observer, shutdownFuture));

            streamsApp.setGlobalStateRestoreListener(
                    new RestoreListener(options.restoreObserver()));

            shutdownHook.apply(streamsApp, shutdownFuture);

            streamsApp.start();

            final ExitCode initialExitCode = waitForShutdown(observer);
            exitCode = shutdownStreams(streamsApp, initialExitCode, observer);
        } finally {
            shutdownMethod.shutdownApp(exitCode.asInt());
        }
    }

    private ExitCode waitForShutdown(final LifecycleObserver observer) {
        try {
            shutdownFuture.get();
            return ExitCode.OK;
        } catch (final Exception e) {
            observer.failed(e instanceof ExecutionException ? e.getCause() : e);
            loggingCloseDelay.run();
            return ExitCode.STREAM_APP_FAILED;
        }
    }

    private static void giveStreamsTimeToLogCauseAsLoggingIsDoneAfterStateChangeCallbackIsCalled() {
        try {
            Thread.sleep(LOGGING_STREAMS_CLOSE_DELAY.toMillis());
        } catch (final InterruptedException e) {
            // Exiting anyway...
        }
    }

    private ExitCode shutdownStreams(
            final KafkaStreams streamsApp,
            final ExitCode exitCode,
            final LifecycleObserver observer) {
        try {
            final Duration closeTimeout = options.streamsCloseTimeout();
            observer.stopping(exitCode);
            final boolean shutdown = streamsApp.close(closeTimeout);
            if (!shutdown) {
                observer.stopTimedOut(closeTimeout);
                return ExitCode.STREAMS_TIMED_OUT_CLOSING;
            }

            observer.stopped(exitCode);
            return exitCode;
        } catch (final Exception e) {
            observer.stopFailed(e);
            return ExitCode.EXCEPTION_THROWN_STOPPING;
        }
    }

    @VisibleForTesting
    interface ShutdownMethod {
        void shutdownApp(int exitCode);
    }

    @VisibleForTesting
    interface MetricsPublisherFactory {
        MetricsPublisher create(KafkaMetricsPublisherOptions options);
    }
}
