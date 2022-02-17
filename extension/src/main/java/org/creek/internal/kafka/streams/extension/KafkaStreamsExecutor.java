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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.creek.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creek.api.kafka.streams.extension.observation.StateRestoreObserver;

public final class KafkaStreamsExecutor {

    // Todo(ac): Logging https://github.com/creek-service/creek-kafka/issues/16
    // Todo(ac): Metrics publishing https://github.com/creek-service/creek-kafka/issues/14

    public enum ExitCode {
        OK(0),
        EXCEPTION_THROWN_EXECUTING(-1),
        STREAM_APP_FAILED(-2),
        KAFKA_STREAMS_CLOSE_TIMED_OUT(-3),
        EXCEPTION_THROWN_STOPPING(-4);

        private final int code;

        ExitCode(final int code) {
            this.code = code;
        }
    }

    public static final Duration LOGGING_STREAMS_CLOSE_DELAY = Duration.ofSeconds(1);

    private final KafkaStreamsExtensionOptions options;
    private final StreamsShutdownHook shutdownHook;
    private final ShutdownHandler shutdownHandler;
    private final CompletableFuture<Void> shutdownFuture;

    public KafkaStreamsExecutor(final KafkaStreamsExtensionOptions options) {
        this(
                options,
                new DefaultStreamsShutdownHook(),
                System::exit,
                new CompletableFuture<>());
    }

    // Todo(ac): @VisibleForTesting
    KafkaStreamsExecutor(
            final KafkaStreamsExtensionOptions options,
            final StreamsShutdownHook shutdownHook,
            final ShutdownHandler shutdownHandler,
            final CompletableFuture<Void> shutdownFuture) {
        this.options = requireNonNull(options, "options");
        this.shutdownHook = requireNonNull(shutdownHook, "shutdownHook");
        this.shutdownHandler = requireNonNull(shutdownHandler, "shutdownHandler");
        this.shutdownFuture = requireNonNull(shutdownFuture, "future");
    }

    public void execute(final KafkaStreams streamsApp) {

        final LifecycleObserver lifecycleObserver = options.lifecycleObserver();

        ExitCode exitCode = ExitCode.EXCEPTION_THROWN_EXECUTING;

        try {

            lifecycleObserver.starting();

            streamsApp.setStateListener(new LifecycleListener(lifecycleObserver, shutdownFuture));

            streamsApp.setGlobalStateRestoreListener(
                    new StateRestoreListenerShim(options.stateRestoreObserver()));

            shutdownHook.addHookFor(streamsApp, shutdownFuture);

            streamsApp.start();

            final ExitCode initialExitCode = waitForShutdown();
            exitCode = shutdownStreams(streamsApp, initialExitCode, lifecycleObserver);
        } finally {
            shutdownHandler.shutdownApp(exitCode.code);
        }
    }

    private ExitCode waitForShutdown() {
        try {
            shutdownFuture.get();
            return ExitCode.OK;
        } catch (final ExecutionException e) {
            giveStreamsTimeToLogCauseAsLoggingIsDoneAfterStateChangeCallbackIsCalled();
            return ExitCode.STREAM_APP_FAILED;
        } catch (final InterruptedException e) {
            return ExitCode.STREAM_APP_FAILED;
        }
    }

    private void giveStreamsTimeToLogCauseAsLoggingIsDoneAfterStateChangeCallbackIsCalled() {
        try {
            Thread.sleep(LOGGING_STREAMS_CLOSE_DELAY.toMillis());
        } catch (final InterruptedException e) {
            // Exiting anyway...
        }
    }

    private ExitCode shutdownStreams(
            final KafkaStreams streamsApp,
            final ExitCode initialExitCode,
            final LifecycleObserver observer) {
        try {
            final Duration closeTimeout = options.streamsCloseTimeout();
            observer.stopping(initialExitCode.code);
            final boolean shutdown = streamsApp.close(closeTimeout);

            if (shutdown) {
                observer.stopped(initialExitCode.code);
            } else {
                observer.stopTimedOut(closeTimeout);
            }

            return shutdown ? initialExitCode : ExitCode.KAFKA_STREAMS_CLOSE_TIMED_OUT;
        } catch (final Exception e) {
            observer.stopFailed(e);
            return ExitCode.EXCEPTION_THROWN_STOPPING;
        }
    }

    private static class StateRestoreListenerShim implements StateRestoreListener {

        private final StateRestoreObserver observer;

        StateRestoreListenerShim(final StateRestoreObserver observer) {
            this.observer = requireNonNull(observer, "observer");
        }

        @Override
        public void onRestoreStart(
                final TopicPartition topicPartition,
                final String storeName,
                final long startingOffset,
                final long endingOffset) {
            observer.restoreStarted(
                    topicPartition.topic(),
                    topicPartition.partition(),
                    storeName,
                    startingOffset,
                    endingOffset);
        }

        @Override
        public void onBatchRestored(
                final TopicPartition topicPartition,
                final String storeName,
                final long batchEndOffset,
                final long restored) {}

        @Override
        public void onRestoreEnd(
                final TopicPartition topicPartition,
                final String storeName,
                final long totalRestored) {
            observer.restoreFinished(
                    topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
        }
    }

    interface ShutdownHandler {
        void shutdownApp(int exitCode);
    }
}
