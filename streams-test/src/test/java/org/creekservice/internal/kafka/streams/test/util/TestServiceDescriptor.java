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

package org.creekservice.internal.kafka.streams.test.util;

import static org.creekservice.internal.kafka.streams.test.util.TopicConfigBuilder.withPartitions;
import static org.creekservice.internal.kafka.streams.test.util.TopicDescriptors.inputTopic;
import static org.creekservice.internal.kafka.streams.test.util.TopicDescriptors.outputTopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicInput;
import org.creekservice.api.kafka.metadata.OwnedKafkaTopicOutput;
import org.creekservice.api.platform.metadata.ComponentInput;
import org.creekservice.api.platform.metadata.ComponentOutput;
import org.creekservice.api.platform.metadata.ServiceDescriptor;

public final class TestServiceDescriptor implements ServiceDescriptor {

    private static final List<ComponentInput> INPUTS = new ArrayList<>();
    private static final List<ComponentOutput> OUTPUTS = new ArrayList<>();

    public static final OwnedKafkaTopicInput<String, Long> InputTopic =
            register(inputTopic("input", String.class, Long.class, withPartitions(1)));

    public static final OwnedKafkaTopicOutput<String, Long> OutputTopic =
            register(outputTopic("output", String.class, long.class, withPartitions(1)));

    @Override
    public String dockerImage() {
        return "test-service";
    }

    @Override
    public Collection<ComponentInput> inputs() {
        return List.copyOf(INPUTS);
    }

    @Override
    public Collection<ComponentOutput> outputs() {
        return List.copyOf(OUTPUTS);
    }

    private static <T extends ComponentInput> T register(final T input) {
        INPUTS.add(input);
        return input;
    }

    private static <T extends ComponentOutput> T register(final T output) {
        OUTPUTS.add(output);
        return output;
    }
}
