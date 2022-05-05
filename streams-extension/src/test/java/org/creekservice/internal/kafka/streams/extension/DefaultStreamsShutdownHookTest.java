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

package org.creekservice.internal.kafka.streams.extension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultStreamsShutdownHookTest {

    @Mock private KafkaStreams streams;
    @Mock private Runtime runtime;
    @Captor private ArgumentCaptor<StreamsUncaughtExceptionHandler> ehCaptor;
    @Captor private ArgumentCaptor<Thread> threadCaptor;
    private final CompletableFuture<Void> future = new CompletableFuture<>();
    private DefaultStreamsShutdownHook hooker;

    @BeforeEach
    void setUp() {
        hooker = new DefaultStreamsShutdownHook(runtime);
    }

    @Test
    void shouldAddStreamsExceptionHandler() {
        // When:
        hooker.apply(streams, future);

        // Then:
        verify(streams).setUncaughtExceptionHandler(any(StreamsUncaughtExceptionHandler.class));
    }

    @Test
    void shouldNotAddShutdownHookOnConstruction() {
        verify(runtime, never()).addShutdownHook(threadCaptor.capture());
    }

    @Test
    void shouldAddShutdownHook() {
        // When:
        hooker.apply(streams, future);

        // Then:
        verify(runtime).addShutdownHook(any());
    }

    @Test
    void shouldCompleteFutureExceptionallyOnStreamsException() {
        // Given:
        final StreamsUncaughtExceptionHandler handler = streamsExceptionHandler();
        final Throwable cause = new RuntimeException("Big Badda Boom");

        // When:
        handler.handle(cause);

        // Then:
        final Exception e = assertThrows(ExecutionException.class, future::get);
        assertThat(e.getCause(), is(sameInstance(cause)));
    }

    @Test
    void shouldCompleteFutureNormallyOnAppShutdown() throws Exception {
        // Given:
        final Thread thread = shutdownHook();

        // When:
        thread.start();
        thread.join();

        // Then:
        assertThat(future.get(), is(nullValue()));
    }

    private StreamsUncaughtExceptionHandler streamsExceptionHandler() {
        hooker.apply(streams, future);
        verify(streams).setUncaughtExceptionHandler(ehCaptor.capture());
        return ehCaptor.getValue();
    }

    private Thread shutdownHook() {
        hooker.apply(streams, future);
        verify(runtime).addShutdownHook(threadCaptor.capture());
        return threadCaptor.getValue();
    }
}
