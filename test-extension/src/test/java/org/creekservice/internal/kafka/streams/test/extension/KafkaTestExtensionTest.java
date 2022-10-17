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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.creekservice.api.kafka.extension.KafkaClientsExtensionProvider;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.internal.kafka.extension.ClientsExtension;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicExpectationHandler;
import org.creekservice.internal.kafka.streams.test.extension.handler.TopicInputHandler;
import org.creekservice.internal.kafka.streams.test.extension.model.KafkaOptions;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicExpectation;
import org.creekservice.internal.kafka.streams.test.extension.model.TopicInput;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.StartKafkaTestListener;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.TearDownTestListener;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.TopicValidatingListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaTestExtensionTest {

    @Mock(answer = RETURNS_DEEP_STUBS)
    private CreekSystemTest api;

    @Mock(answer = RETURNS_DEEP_STUBS)
    private CreekSystemTest.ExtensionAccessor extensions;

    @Mock private ClientsExtension clientEx;

    private KafkaTestExtension ext;

    @BeforeEach
    void setUp() {
        ext = new KafkaTestExtension();

        when(api.extensions()).thenReturn(extensions);
        doReturn(clientEx).when(extensions).ensureExtension(KafkaClientsExtensionProvider.class);
    }

    @Test
    void shouldExposeExtension() {
        assertThat(
                extensionTester().accessibleExtensions(),
                hasItem(instanceOf(KafkaTestExtension.class)));
    }

    @Test
    void shouldHaveCorrectName() {
        assertThat(ext.name(), is("org.creekservice.kafka.test"));
    }

    @Test
    void shouldInitializeServiceExtension() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.extensions()).ensureExtension(KafkaClientsExtensionProvider.class);
    }

    @Test
    void shouldAppendTestListenerToStartKafka() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.tests().env().listeners()).append(isA(StartKafkaTestListener.class));
    }

    @Test
    void shouldAppendTestListenerToTearDownClients() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.tests().env().listeners()).append(isA(TearDownTestListener.class));
    }

    @Test
    void shouldAppendTopicValidatingListenerToTearDownClients() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.tests().env().listeners()).append(isA(TopicValidatingListener.class));
    }

    @Test
    void shouldRegisterTopicInputModel() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.tests().model().addInput(eq(TopicInput.class), isA(TopicInputHandler.class)))
                .withName("creek/kafka-topic@1");
    }

    @Test
    void shouldRegisterTopicExpectationModel() {
        // When:
        ext.initialize(api);

        // Then:
        verify(
                        api.tests()
                                .model()
                                .addExpectation(
                                        eq(TopicExpectation.class),
                                        isA(TopicExpectationHandler.class)))
                .withName("creek/kafka-topic@1");
    }

    @Test
    void shouldRegisterOptionModel() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.tests().model().addOption(eq(KafkaOptions.class)))
                .withName("creek/kafka-options@1");
    }
}
