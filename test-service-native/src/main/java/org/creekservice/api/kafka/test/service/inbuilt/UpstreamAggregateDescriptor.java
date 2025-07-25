/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.creekservice.api.kafka.metadata.topic.OwnedKafkaTopicOutput;
import org.creekservice.api.platform.metadata.AggregateDescriptor;
import org.creekservice.api.platform.metadata.ComponentOutput;

/** Would normally be in a different jar, but for this test service we'll just have it here. */
public final class UpstreamAggregateDescriptor implements AggregateDescriptor {

    private static final List<ComponentOutput> OUTPUTS = new ArrayList<>();

    public static final OwnedKafkaTopicOutput<String, Long> Output =
            register(outputTopic("input", String.class, long.class, withPartitions(3)));

    public UpstreamAggregateDescriptor() {}

    @Override
    public Collection<ComponentOutput> outputs() {
        return List.copyOf(OUTPUTS);
    }

    private static <T extends ComponentOutput> T register(final T output) {
        OUTPUTS.add(output);
        return output;
    }
}
