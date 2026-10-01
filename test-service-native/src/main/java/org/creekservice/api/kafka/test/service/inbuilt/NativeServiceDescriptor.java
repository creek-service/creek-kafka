/*
 * Copyright 2022-2026 Creek Contributors (https://github.com/creek-service)
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
import static org.creekservice.internal.kafka.test.service.inbuilt.TopicDescriptors.outputTopic;

import java.util.Collection;
import java.util.List;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicInput;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.platform.metadata.ComponentInput;
import org.creekservice.api.platform.metadata.ComponentOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;

/** Service descriptor that makes use of native Kafka serde. */
public final class NativeServiceDescriptor implements ServiceDescriptor {

    public static final KafkaTopicInput<String, Long> InputTopic =
            UpstreamAggregateDescriptor.Output.toInput();

    public static final OwnedKafkaTopicOutput<Long, String> OutputTopic =
            outputTopic("output", Long.class, String.class, withPartitions(1));

    public NativeServiceDescriptor() {}

    @Override
    public String dockerImage() {
        return "ghcr.io/creek-service/creek-kafka-test-service-native";
    }

    @Override
    public Collection<ComponentInput> inputs() {
        return List.of(InputTopic);
    }

    @Override
    public Collection<ComponentOutput> outputs() {
        return List.of(OutputTopic);
    }
}
