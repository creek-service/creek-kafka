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

package org.creekservice.internal.kafka.streams.extension.observation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import com.google.common.testing.EqualsTester;
import java.time.Duration;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver.ExitCode;
import org.creekservice.api.test.observability.logging.structured.TestStructuredLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultLifecycleObserverTest {

    private static final Throwable CAUSE = mock(Throwable.class, "boom");

    private TestStructuredLogger testLogger;
    private DefaultLifecycleObserver observer;

    @BeforeEach
    void setUp() {
        testLogger = TestStructuredLogger.create();
        observer = new DefaultLifecycleObserver(testLogger);
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(new DefaultLifecycleObserver(), new DefaultLifecycleObserver())
                .addEqualityGroup("diff")
                .testEquals();
    }

    @Test
    void shouldImplementToString() {
        assertThat(new DefaultLifecycleObserver().toString(), is("DefaultLifecycleObserver"));
    }

    @Test
    void shouldLogStarting() {
        // When:
        observer.starting();

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "INFO: {lifecycle=creek.lifecycle.service.starting, message=Kafka streams"
                                + " app state change}"));
    }

    @Test
    void shouldLogRebalancing() {
        // When:
        observer.rebalancing();

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "INFO: {lifecycle=creek.lifecycle.service.rebalancing, message=Kafka"
                                + " streams app state change}"));
    }

    @Test
    void shouldLogRunning() {
        // When:
        observer.running();

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "INFO: {lifecycle=creek.lifecycle.service.running, message=Kafka streams"
                                + " app state change}"));
    }

    @Test
    void shouldLogStarted() {
        // When:
        observer.started();

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "INFO: {lifecycle=creek.lifecycle.service.started, message=Kafka streams"
                                + " app state change}"));
    }

    @Test
    void shouldLogFailed() {
        // When:
        observer.failed(CAUSE);

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains("ERROR: {message=Failure of streams app detected.} boom"));
    }

    @Test
    void shouldLogStopping() {
        // When:
        observer.stopping(ExitCode.OK);

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "INFO: {exitCode=OK(0), lifecycle=creek.lifecycle.service.stopping,"
                                + " message=Kafka streams app state change}"));
    }

    @Test
    void shouldLogStopTimedOut() {
        // When:
        observer.stopTimedOut(Duration.ofHours(12));

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "WARN: {closeTimeout=PT12H, message=Failed to stop the Kafka Streams app"
                                + " within the configured timeout, i.e. streams.close() returned"
                                + " false}"));
    }

    @Test
    void shouldLogStopFailed() {
        // When:
        observer.stopFailed(CAUSE);

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "ERROR: {message=Failed to stop the Kafka Streams app as streams.close()"
                                + " threw an exception} boom"));
    }

    @Test
    void shouldLogStopped() {
        // When:
        observer.stopped(ExitCode.EXCEPTION_THROWN_STOPPING);

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "INFO: {"
                                + "exitCode=EXCEPTION_THROWN_STOPPING(-4), "
                                + "lifecycle=creek.lifecycle.service.stopped, "
                                + "message=Kafka streams app state change"
                                + "}"));
    }
}
