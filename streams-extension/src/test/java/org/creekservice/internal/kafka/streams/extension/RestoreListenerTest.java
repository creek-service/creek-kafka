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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.apache.kafka.common.TopicPartition;
import org.creekservice.api.kafka.streams.extension.observation.StateRestoreObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RestoreListenerTest {

    private static final String TOPIC = "some-topic";
    private static final String STORE = "some-store";

    @Mock private StateRestoreObserver observer;
    private RestoreListener listener;

    @BeforeEach
    void setUp() {
        listener = new RestoreListener(observer);
    }

    @Test
    void shouldInvokeObserverOnStart() {
        // When:
        listener.onRestoreStart(new TopicPartition(TOPIC, 1), STORE, 23, 276454);

        // Then:
        verify(observer).restoreStarted(TOPIC, 1, STORE, 23, 276454);
    }

    @Test
    void shouldInvokeObserverOnEnd() {
        // When:
        listener.onRestoreEnd(new TopicPartition(TOPIC, 1), STORE, 895);

        // Then:
        verify(observer).restoreFinished(TOPIC, 1, STORE, 895);
    }

    @Test
    void shouldNotInvokeObserverOnBatch() {
        // When:
        listener.onBatchRestored(new TopicPartition(TOPIC, 1), STORE, 0, 0);

        // Then:
        verifyNoInteractions(observer);
    }
}
