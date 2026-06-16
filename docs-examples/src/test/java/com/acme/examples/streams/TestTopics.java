/*
 * Copyright 2023-2026 Creek Contributors (https://github.com/creek-service)
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

package com.acme.examples.streams;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.creekservice.api.kafka.extension.KafkaClientsExtension;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;

// begin-snippet: all
public final class TestTopics {

    @SuppressWarnings("resource")
    public static <K, V> TestInputTopic<K, V> inputTopic(
            final KafkaTopicDescriptor<K, V> topicDescriptor,
            final KafkaClientsExtension ext,
            final TopologyTestDriver testDriver) {
        final KafkaTopic<K, V> topic = ext.topic(topicDescriptor);
        return testDriver.createInputTopic(
                topicDescriptor.name(), topic.keySerde().serializer(), topic.valueSerde().serializer());
    }

    @SuppressWarnings("resource")
    public static <K, V> TestOutputTopic<K, V> outputTopic(
            final KafkaTopicDescriptor<K, V> topicDescriptor,
            final KafkaClientsExtension ext,
            final TopologyTestDriver testDriver) {
        final KafkaTopic<K, V> topic = ext.topic(topicDescriptor);
        return testDriver.createOutputTopic(
                topicDescriptor.name(), topic.keySerde().deserializer(), topic.valueSerde().deserializer());
    }

    private TestTopics() {}
}
// end-snippet
