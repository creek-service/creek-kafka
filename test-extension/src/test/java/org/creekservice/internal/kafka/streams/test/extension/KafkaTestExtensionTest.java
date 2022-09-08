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
import static org.mockito.Mockito.verify;

import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ResourceHandler;
import org.creekservice.api.system.test.extension.CreekSystemTest;
import org.creekservice.internal.kafka.common.resource.TopicResourceHandler;
import org.creekservice.internal.kafka.streams.test.extension.testsuite.StartKafkaTestListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaTestExtensionTest {

    @Mock(answer = RETURNS_DEEP_STUBS)
    private CreekSystemTest api;

    private KafkaTestExtension ext;

    @BeforeEach
    void setUp() {
        ext = new KafkaTestExtension();
    }

    @Test
    void shouldExposeExtension() {
        assertThat(
                extensionTester().accessibleExtensions(),
                hasItem(instanceOf(KafkaTestExtension.class)));
    }

    @Test
    void shouldExposeName() {
        assertThat(ext.name(), is("creek-kafka"));
    }

    @Test
    void shouldAppendTestListenerToStartKafka() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.test().env().listener()).append(isA(StartKafkaTestListener.class));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    void shouldRegisterResourceHandler() {
        // When:
        ext.initialize(api);

        // Then:
        verify(api.component().model())
                .addResource(
                        eq(KafkaTopicDescriptor.class),
                        (ResourceHandler) isA(TopicResourceHandler.class));
    }
}
