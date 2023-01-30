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

package org.creekservice.internal.kafka.extension.resource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.testing.EqualsTester;
import java.util.List;
import org.creekservice.api.kafka.extension.client.TopicClient;
import org.creekservice.api.kafka.metadata.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicOutput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicResourceHandlerTest {
    @Mock private TopicClient topicClient;
    @Mock private KafkaTopicInput<?, ?> notCreatable;
    @Mock private OwnedKafkaTopicOutput<?, ?> creatable;
    private TopicResourceHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TopicResourceHandler(topicClient);
    }

    @Test
    void shouldImplementHashCodeAndEquals() {
        new EqualsTester()
                .addEqualityGroup(
                        new TopicResourceHandler(topicClient),
                        new TopicResourceHandler(topicClient))
                .addEqualityGroup(new TopicResourceHandler(mock(TopicClient.class)))
                .testEquals();
    }

    @Test
    void shouldThrowIfNotCreatable() {
        // When:
        final Exception e =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> handler.ensure(List.of(notCreatable)));

        // Then:
        assertThat(e.getMessage(), startsWith("Topic descriptor is not creatable"));
    }

    @Test
    void shouldEnsureCreatableTopicsExist() {
        // When:
        handler.ensure(List.of(creatable));

        // Then:
        verify(topicClient).ensure(List.of(creatable));
    }
}
