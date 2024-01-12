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

package org.creekservice.api.kafka.metadata.topic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;

class KafkaTopicDescriptorTest {

    @Test
    void shouldCreateUniqueKafkaTopicResourceId() {
        assertThat(
                KafkaTopicDescriptor.resourceId("cluster-a", "topic-b").toString(),
                is("kafka-topic://cluster-a/topic-b"));
    }

    @Test
    void shouldDefaultResourceId() {
        assertThat(
                descriptor("cluster-a", "topic-b").id().toString(),
                is("kafka-topic://cluster-a/topic-b"));
    }

    @Test
    void shouldHandleEmptyClusterName() {
        assertThat(descriptor("", "topic-b").id().toString(), is("kafka-topic:///topic-b"));
    }

    @Test
    void shouldThrowOnInvalidId() {
        // Given:
        final KafkaTopicDescriptor<Void, Void> descriptor = descriptor("%%%%", "%%%%");

        // When:
        final Exception e = assertThrows(IllegalArgumentException.class, descriptor::id);

        // Then:
        assertThat(e.getCause(), instanceOf(URISyntaxException.class));
        assertThat(e.getMessage(), containsString("Malformed escape pair"));
    }

    @Test
    void shouldKnowIfPartIsKeyPart() {
        assertTrue(KafkaTopicDescriptor.PartDescriptor.Part.key.isKey());
        assertFalse(KafkaTopicDescriptor.PartDescriptor.Part.value.isKey());
    }

    private KafkaTopicDescriptor<Void, Void> descriptor(final String cluster, final String name) {
        return new KafkaTopicDescriptor<>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String cluster() {
                return cluster;
            }

            @Override
            public PartDescriptor<Void> key() {
                return null;
            }

            @Override
            public PartDescriptor<Void> value() {
                return null;
            }
        };
    }
}
