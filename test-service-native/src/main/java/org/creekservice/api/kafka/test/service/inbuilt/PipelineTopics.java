/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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
package org.creekservice.api.kafka.test.service.inbuilt;

import static org.creekservice.internal.kafka.test.service.inbuilt.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.creekservice.internal.kafka.test.service.inbuilt.TopicDescriptors.inputTopic;
import static org.creekservice.internal.kafka.test.service.inbuilt.TopicDescriptors.outputTopic;

import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;

/** Topic A is owned by A, topic C by B. The owner of B varies by pipeline pair. */
public final class PipelineTopics {
    public static final OwnedKafkaTopicInput<String, String> OWNED_INPUT_A =
            inputTopic("topic-a", String.class, String.class, withPartitions(1));
    public static final OwnedKafkaTopicOutput<String, String> OWNED_OUTPUT_B =
            outputTopic("topic-b", String.class, String.class, withPartitions(1));
    public static final OwnedKafkaTopicInput<String, String> OWNED_INPUT_B =
            inputTopic("topic-b", String.class, String.class, withPartitions(1));
    public static final OwnedKafkaTopicOutput<String, String> OWNED_OUTPUT_C =
            outputTopic("topic-c", String.class, String.class, withPartitions(1));

    private PipelineTopics() {}
}
