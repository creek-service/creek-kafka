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

package org.creek.internal.kafka.streams.extension.observation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import org.creek.api.test.observability.logging.structured.TestStructuredLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultStateRestoreObserverTest {

    private TestStructuredLogger testLogger;
    private DefaultStateRestoreObserver observer;

    @BeforeEach
    void setUp() {
        testLogger = TestStructuredLogger.create();
        observer = new DefaultStateRestoreObserver(testLogger);
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        new DefaultStateRestoreObserver(), new DefaultStateRestoreObserver())
                .addEqualityGroup("diff")
                .testEquals();
    }

    @Test
    void shouldImplementToString() {
        assertThat(new DefaultStateRestoreObserver().toString(), is("DefaultStateRestoreObserver"));
    }

    @Test
    void shouldLogStarting() {
        // When:
        observer.restoreStarted("topic-a", 1, "store-b", 2, 44);

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "INFO: {endOffset=44, message=Restore of state store partition starting, "
                                + "partition=1, startOffset=2, store=store-b, topic=topic-a}"));
    }

    @Test
    void shouldLogFinished() {
        // When:
        observer.restoreFinished("topic-a", 1, "store-b", 29);

        // Then:
        assertThat(
                testLogger.textEntries(),
                contains(
                        "INFO: {message=Restore of state store partition finished, "
                                + "partition=1, restored=29, store=store-b, topic=topic-a}"));
    }
}
