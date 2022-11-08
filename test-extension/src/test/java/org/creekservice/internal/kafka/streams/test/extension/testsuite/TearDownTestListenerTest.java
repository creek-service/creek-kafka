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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TearDownTestListenerTest {

    @Mock private ClientsExtension clientsExt;
    private TearDownTestListener listener;

    @BeforeEach
    void setUp() {
        listener = new TearDownTestListener(clientsExt);
    }

    @Test
    void shouldCloseAfterSuite() {
        // When:
        listener.afterSuite(null, null);

        // Then:
        verify(clientsExt).close(Duration.ofMinutes(1));
    }

    @Test
    void shouldSwallowExceptions() {
        // Given:
        doThrow(new RuntimeException("boom")).when(clientsExt).close(any());

        // When:
        listener.afterSuite(null, null);

        // Then: did not throw.
    }
}
