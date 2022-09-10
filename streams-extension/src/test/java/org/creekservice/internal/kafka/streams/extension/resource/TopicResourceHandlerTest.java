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

package org.creekservice.internal.kafka.streams.extension.resource;

import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.List;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.common.resource.KafkaResourceValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TopicResourceHandlerTest {

    @Mock private KafkaResourceValidator validator;
    @Mock private KafkaTopicDescriptor<?, ?> descriptor;
    private TopicResourceHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TopicResourceHandler(validator);
    }

    @Test
    void shouldValidateGroup() {
        // Given:
        final Collection<? extends KafkaTopicDescriptor<?, ?>> group = List.of(descriptor);

        // When:
        handler.validate(group);

        // Then:
        verify(validator).validateGroup(group);
    }
}
