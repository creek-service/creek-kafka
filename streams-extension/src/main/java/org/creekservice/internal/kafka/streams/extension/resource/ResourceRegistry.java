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

import java.net.URI;
import java.util.Map;
import org.creekservice.api.kafka.common.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.internal.kafka.common.resource.KafkaTopicDescriptors;

public final class ResourceRegistry {

    private final Map<URI, Topic<?, ?>> topics;

    ResourceRegistry(final Map<URI, Topic<?, ?>> topics) {
        this.topics = Map.copyOf(requireNonNull(topics, "topics"));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public <K, V> KafkaTopic<K, V> topic(final KafkaTopicDescriptor<K, V> def) {
        final Topic<?, ?> found = topics.get(def.id());
        if (found == null) {
            throw new UnknownTopicException(def.id());
        }

        if (!KafkaTopicDescriptors.matches(found.descriptor(), def)) {
            throw new TopicDescriptorMismatchException(def, found.descriptor());
        }

        return (KafkaTopic) found;
    }

    private static final class UnknownTopicException extends IllegalArgumentException {
        UnknownTopicException(final URI id) {
            super("Unknown topic. No topic has the supplied id. id=" + id);
        }
    }

    private static final class TopicDescriptorMismatchException extends IllegalArgumentException {
        TopicDescriptorMismatchException(
                final KafkaTopicDescriptor<?, ?> supplied, final KafkaTopicDescriptor<?, ?> found) {
            super(
                    "The supplied topic descriptor does not match the topic descriptor found when inspecting components."
                            + " supplied="
                            + KafkaTopicDescriptors.asString(supplied)
                            + " actual="
                            + KafkaTopicDescriptors.asString(found));
        }
    }
}
