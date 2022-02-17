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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.streams.KafkaStreams;
import org.creek.api.kafka.streams.extension.observation.LifecycleObserver;

// Todo(ac): Test
final class LifecycleListener implements KafkaStreams.StateListener {

    private final LifecycleObserver observer;
    private final CompletableFuture<Void> forceShutdown;
    private final AtomicBoolean starting = new AtomicBoolean(true);

    LifecycleListener(
            final LifecycleObserver observer, final CompletableFuture<Void> forceShutdown) {
        this.observer = requireNonNull(observer, "observer");
        this.forceShutdown = requireNonNull(forceShutdown, "forceShutdown");
    }

    @Override
    public void onChange(final KafkaStreams.State newState, final KafkaStreams.State oldState) {
        switch (newState) {
            case REBALANCING:
                observer.rebalancing();
                break;
            case RUNNING:
                if (starting.getAndSet(false)) {
                    observer.started();
                }
                observer.running();
                break;
            case ERROR:
                observer.failed();
                forceShutdown.completeExceptionally(new StreamThreadFailedException());
                break;
            default:
                break;
        }
    }

    // Todo(ac): @VisibleForTesting
    static class StreamThreadFailedException extends RuntimeException {
        StreamThreadFailedException() {
            super("A Stream thread transitioned into the ERROR state");
        }
    }
}
