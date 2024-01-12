/*
 * Copyright 2022-2024 Creek Contributors (https://github.com/creek-service)
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

import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.KafkaStreams.State.PENDING_SHUTDOWN;
import static org.apache.kafka.streams.KafkaStreams.State.REBALANCING;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.streams.KafkaStreams;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StreamsStateListenerTest {

    private static final KafkaStreams.State IGNORED = null;

    @Mock private LifecycleObserver observer;
    private CompletableFuture<Void> forceShutdown;
    private StreamsStateListener listener;

    @BeforeEach
    void setUp() {
        forceShutdown = new CompletableFuture<>();
        listener = new StreamsStateListener(observer, forceShutdown);
    }

    @Test
    void shouldTransitionToStartedThenRunning() {
        // When:
        listener.onChange(RUNNING, IGNORED);

        // Then:
        final InOrder inOrder = Mockito.inOrder(observer);
        inOrder.verify(observer).started();
        inOrder.verify(observer).running();
        assertThat(forceShutdown.isDone(), is(false));
    }

    @Test
    void shouldOnlyTransitionToStartedOnce() {
        // Given:
        listener.onChange(RUNNING, IGNORED);
        listener.onChange(REBALANCING, IGNORED);

        // When:
        listener.onChange(RUNNING, IGNORED);

        // Then:
        verify(observer, times(1)).started();
    }

    @Test
    void shouldTransitionToRebalancing() {
        // When:
        listener.onChange(REBALANCING, IGNORED);

        // Then:
        verify(observer).rebalancing();
        assertThat(forceShutdown.isDone(), is(false));
    }

    @Test
    void shouldTransitionToFailed() {
        // When:
        listener.onChange(ERROR, IGNORED);

        // Then:
        final Exception e = assertThrows(ExecutionException.class, forceShutdown::get);
        assertThat(
                e.getCause().getMessage(),
                is(
                        "The Kafka Streams app entered the ERROR state. See Kafka Streams logs for"
                                + " more info."));
    }

    @Test
    void shouldIgnoreOtherTransitions() {
        // When:
        listener.onChange(PENDING_SHUTDOWN, IGNORED);

        // Then:
        verifyNoMoreInteractions(observer);
        assertThat(forceShutdown.isDone(), is(false));
    }
}
