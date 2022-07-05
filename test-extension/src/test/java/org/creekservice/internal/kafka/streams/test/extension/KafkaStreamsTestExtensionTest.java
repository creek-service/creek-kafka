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

package org.creekservice.internal.kafka.streams.test.extension;

import static org.creekservice.api.system.test.test.util.CreekSystemTestExtensionTester.extensionTester;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.StreamsTestLifecycleListener;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.ValidatingTestListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaStreamsTestExtensionTest {

    @Mock(answer = RETURNS_DEEP_STUBS)
    private CreekSystemTest api;

    private KafkaStreamsTestExtension ext;

    @BeforeEach
    void setUp() {
        ext = new KafkaStreamsTestExtension();
    }

    @Test
    void shouldExposeExtension() {
        assertThat(
                extensionTester().accessibleExtensions(),
                hasItem(instanceOf(KafkaStreamsTestExtension.class)));
    }

    @Test
    void shouldExposeName() {
        assertThat(ext.name(), is("creek-kafka-streams"));
    }

    @Test
    void shouldAppendValidatingListenerBeforeMainListener() {
        // When:
        ext.initialize(api);

        // Then:
        final InOrder inOrder = inOrder(api.testSuite().listener());
        inOrder.verify(api.testSuite().listener()).append(isA(ValidatingTestListener.class));
        inOrder.verify(api.testSuite().listener()).append(isA(StreamsTestLifecycleListener.class));
    }

    @Test
    void shouldAppendTestSuiteListener() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.testSuite().listener()).append(isA(StreamsTestLifecycleListener.class));
    }
}
