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

import static java.util.Objects.requireNonNull;
import static org.creekservice.api.base.type.CodeLocation.codeLocation;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.streams.extension.client.TopicClient;
import org.creekservice.api.platform.metadata.ResourceHandler;
import org.creekservice.internal.kafka.common.resource.KafkaTopicDescriptors;

public class TopicResourceHandler implements ResourceHandler<KafkaTopicDescriptor<?, ?>> {

    private final TopicClient topicClient;

    public TopicResourceHandler(final TopicClient topicClient) {
        this.topicClient = requireNonNull(topicClient, "topicClient");
    }

    @Override
    public void ensure(final Collection<? extends KafkaTopicDescriptor<?, ?>> resources) {
        final List<? extends CreatableKafkaTopic<?, ?>> creatable =
                resources.stream()
                        .map(TopicResourceHandler::toCreatable)
                        .collect(Collectors.toList());

        topicClient.ensure(creatable);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TopicResourceHandler that = (TopicResourceHandler) o;
        return Objects.equals(topicClient, that.topicClient);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicClient);
    }

    private static CreatableKafkaTopic<?, ?> toCreatable(final KafkaTopicDescriptor<?, ?> topic) {
        if (topic instanceof CreatableKafkaTopic) {
            return (CreatableKafkaTopic<?, ?>) topic;
        }
        throw new IllegalArgumentException(
                "Topic descriptor is not creatable: "
                        + KafkaTopicDescriptors.asString(topic)
                        + " ("
                        + codeLocation(topic)
                        + ")");
    }
}
