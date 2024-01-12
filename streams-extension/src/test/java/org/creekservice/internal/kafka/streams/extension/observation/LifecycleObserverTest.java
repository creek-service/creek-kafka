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

package org.creekservice.internal.kafka.streams.extension.observation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Duration;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver;
import org.creekservice.api.kafka.streams.extension.observation.LifecycleObserver.ExitCode;
import org.junit.jupiter.api.Test;

class LifecycleObserverTest {

    @Test
    void shouldIncludeExitCodeNumericInText() {
        assertThat(ExitCode.STREAM_APP_FAILED.toString(), is("STREAM_APP_FAILED(-2)"));
    }

    @Test
    void shouldNotBlowUpOnDefaultMethods() {
        // Given:
        final LifecycleObserver observer = new LifecycleObserver() {};

        // When:
        observer.starting();
        observer.started();
        observer.rebalancing();
        observer.running();
        observer.failed(new RuntimeException());
        observer.stopping(ExitCode.EXCEPTION_THROWN_STOPPING);
        observer.stopTimedOut(Duration.ZERO);
        observer.stopFailed(new RuntimeException());
        observer.stopped(ExitCode.STREAM_APP_FAILED);

        // Then: did not throw.
    }
}
