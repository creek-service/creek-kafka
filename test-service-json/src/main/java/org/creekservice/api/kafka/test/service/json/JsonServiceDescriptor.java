/*
 * Copyright 2026-2026 Creek Contributors (https://github.com/creek-service)
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
package org.creekservice.api.kafka.test.service.json;

import static org.creekservice.internal.kafka.test.service.inbuilt.TopicDescriptors.TopicConfigBuilder.withPartitions;
import static org.creekservice.internal.kafka.test.service.inbuilt.TopicDescriptors.inputTopic;
import static org.creekservice.internal.kafka.test.service.inbuilt.TopicDescriptors.outputTopic;

import java.util.List;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.kafka.test.service.json.model.OutputValue;
import org.creekservice.api.platform.metadata.ComponentInput;
import org.creekservice.api.platform.metadata.ComponentOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;

public final class JsonServiceDescriptor implements ServiceDescriptor {
    public JsonServiceDescriptor() {}

    public static final OwnedKafkaTopicInput<String, OutputValue> INPUT =
            inputTopic("json-input", String.class, OutputValue.class, withPartitions(1));
    public static final OwnedKafkaTopicOutput<String, OutputValue> OUTPUT =
            outputTopic("json-output", String.class, OutputValue.class, withPartitions(1));

    @Override
    public String dockerImage() {
        return "ghcr.io/creek-service/creek-kafka-test-service-json";
    }

    @Override
    public List<ComponentInput> inputs() {
        return List.of(INPUT);
    }

    @Override
    public List<ComponentOutput> outputs() {
        return List.of(OUTPUT);
    }
}
