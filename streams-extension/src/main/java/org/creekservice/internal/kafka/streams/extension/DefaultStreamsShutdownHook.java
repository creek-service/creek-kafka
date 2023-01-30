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

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.creekservice.api.base.annotation.VisibleForTesting;

/** Hooks into unhandled exceptions thrown by the streams app and normal JVM shutdown. */
public final class DefaultStreamsShutdownHook implements StreamsShutdownHook {

    private final Runtime runtime;

    DefaultStreamsShutdownHook() {
        this(Runtime.getRuntime());
    }

    @VisibleForTesting
    DefaultStreamsShutdownHook(final Runtime runtime) {
        this.runtime = requireNonNull(runtime, "runtime");
    }

    public void apply(final KafkaStreams streams, final CompletableFuture<Void> forceShutdown) {
        requireNonNull(streams, "streams");
        streams.setUncaughtExceptionHandler(e -> unhandledStreamsException(e, forceShutdown));
        runtime.addShutdownHook(new Thread(() -> runtimeShutdown(forceShutdown)));
    }

    private static StreamThreadExceptionResponse unhandledStreamsException(
            final Throwable e, final CompletableFuture<?> future) {
        future.completeExceptionally(e);
        return SHUTDOWN_CLIENT;
    }

    private static void runtimeShutdown(final CompletableFuture<?> future) {
        future.complete(null);
    }
}
