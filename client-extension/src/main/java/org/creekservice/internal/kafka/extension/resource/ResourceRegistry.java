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

package org.creekservice.internal.kafka.extension.resource;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.Map;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;

/** A registry of all known Kafka resources. */
public final class ResourceRegistry {

    private final Map<URI, Topic<?, ?>> topics;

    ResourceRegistry(final Map<URI, Topic<?, ?>> topics) {
        this.topics = Map.copyOf(requireNonNull(topics, "topics"));
    }

    /**
     * Get a topic resource from its descriptor
     *
     * @param def the descriptor
     * @param <K> the key type
     * @param <V> the value type
     * @return the resource.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <K, V> KafkaTopic<K, V> topic(final KafkaTopicDescriptor<K, V> def) {
        final Topic<?, ?> found = find(def.id());

        if (!KafkaTopicDescriptors.matches(found.descriptor(), def)) {
            throw new TopicDescriptorMismatchException(def, found.descriptor());
        }

        return (KafkaTopic) found;
    }

    /**
     * Get a topic resource from a cluster and topic name.
     *
     * @param cluster the cluster name.
     * @param topic the topic name.
     * @return the resource.
     */
    public KafkaTopic<?, ?> topic(final String cluster, final String topic) {
        return find(KafkaTopicDescriptor.resourceId(cluster, topic));
    }

    private Topic<?, ?> find(final URI id) {
        final Topic<?, ?> found = topics.get(id);
        if (found == null) {
            throw new UnknownTopicException(id);
        }
        return found;
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
