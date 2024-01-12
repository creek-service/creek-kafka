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

package org.creekservice.internal.kafka.extension.resource;

import static java.util.Objects.requireNonNull;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.extension.resource.KafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;

/** A registry of all known Kafka resources. */
public final class ResourceRegistry implements TopicRegistrar, TopicRegistry {

    private final Map<URI, KafkaTopic<?, ?>> topics = new HashMap<>();
    private final KafkaResourceValidator validator;

    public ResourceRegistry() {
        this(new KafkaResourceValidator());
    }

    @VisibleForTesting
    ResourceRegistry(final KafkaResourceValidator validator) {
        this.validator = requireNonNull(validator, "validator");
    }

    @Override
    public void register(final KafkaTopic<?, ?> topic) {
        topics.compute(
                topic.descriptor().id(),
                (id, existing) -> {
                    if (existing != null) {
                        throw new ResourceAlreadyRegistered(id);
                    }
                    return topic;
                });
    }

    /**
     * Get a topic resource from its descriptor
     *
     * @param def the descriptor
     * @param <K> the key type
     * @param <V> the value type
     * @return the resource.
     */
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <K, V> KafkaTopic<K, V> topic(final KafkaTopicDescriptor<K, V> def) {
        final KafkaTopic<?, ?> found = find(def.id());

        validator.validateGroup(List.of(def, found.descriptor()));

        return (KafkaTopic) found;
    }

    /**
     * Get a topic resource from a cluster and topic name.
     *
     * @param cluster the cluster name.
     * @param topic the topic name.
     * @return the resource.
     */
    @Override
    public KafkaTopic<?, ?> topic(final String cluster, final String topic) {
        return find(KafkaTopicDescriptor.resourceId(cluster, topic));
    }

    private KafkaTopic<?, ?> find(final URI id) {
        final KafkaTopic<?, ?> found = topics.get(id);
        if (found == null) {
            throw new UnknownTopicException(id);
        }
        return found;
    }

    private static final class ResourceAlreadyRegistered extends IllegalStateException {
        ResourceAlreadyRegistered(final URI id) {
            super("Resource already registered with id=" + id);
        }
    }

    private static final class UnknownTopicException extends IllegalArgumentException {
        UnknownTopicException(final URI id) {
            super("Unknown topic. No topic has the supplied id. id=" + id);
        }
    }
}
