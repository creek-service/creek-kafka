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

package org.creekservice.internal.kafka.extension.resource;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.base.type.Lists;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;

/**
 * Collects {@link KafkaTopicDescriptor topic descriptors} from {@link ComponentDescriptor component
 * descriptors}
 */
public final class TopicCollector {

    /**
     * Collect topic descriptors from the supplied {@code components}.
     *
     * @param components the components to extract topics from.
     * @return Object that can be queried for information on the collected topics.
     */
    public CollectedTopics collectTopics(
            final Collection<? extends ComponentDescriptor> components) {

        final Map<URI, List<KafkaTopicDescriptor<?, ?>>> found =
                components.stream()
                        .flatMap(ComponentDescriptor::resources)
                        .filter(KafkaTopicDescriptor.class::isInstance)
                        .map(d -> (KafkaTopicDescriptor<?, ?>) d)
                        .collect(groupingBy(KafkaTopicDescriptor::id));

        found.values().forEach(TopicCollector::throwOnDescriptorMismatch);

        return new CollectedTopics(found);
    }

    private static void throwOnDescriptorMismatch(final List<KafkaTopicDescriptor<?, ?>> defs) {

        final List<KafkaTopicDescriptor<?, ?>> reduced =
                defs.stream()
                        .reduce(
                                new ArrayList<>(1),
                                TopicCollector::accumulateTopics,
                                Lists::combineList);

        if (reduced.size() != 1) {
            throw new TopicDescriptorMismatchException(defs);
        }
    }

    private static List<KafkaTopicDescriptor<?, ?>> accumulateTopics(
            final List<KafkaTopicDescriptor<?, ?>> collected,
            final KafkaTopicDescriptor<?, ?> def) {
        final Optional<KafkaTopicDescriptor<?, ?>> matching =
                collected.stream().filter(d -> KafkaTopicDescriptors.matches(d, def)).findAny();

        if (matching.isEmpty()) {
            collected.add(def);
        } else if (def instanceof CreatableKafkaTopic
                && !(matching.get() instanceof CreatableKafkaTopic)) {
            collected.remove(matching.get());
            collected.add(def);
        }

        return collected;
    }

    private static final class TopicDescriptorMismatchException extends RuntimeException {
        TopicDescriptorMismatchException(
                final List<? extends KafkaTopicDescriptor<?, ?>> descriptors) {
            super(
                    "Topic descriptor mismatch: multiple topic descriptors share the same topic name, but have different attributes."
                            + System.lineSeparator()
                            + descriptors.stream()
                                    .map(KafkaTopicDescriptors::asString)
                                    .collect(Collectors.joining(System.lineSeparator())));
        }
    }

    /** Holds the result of a topic collection. */
    public static class CollectedTopics {
        private final Map<URI, List<KafkaTopicDescriptor<?, ?>>> topics;

        @VisibleForTesting
        CollectedTopics(final Map<URI, List<KafkaTopicDescriptor<?, ?>>> found) {
            this.topics =
                    requireNonNull(found, "found").entrySet().stream()
                            .collect(
                                    Collectors.toUnmodifiableMap(
                                            Map.Entry::getKey, e -> List.copyOf(e.getValue())));
        }

        /** @return the set of cluster names found in all collected topics. */
        public Set<String> clusters() {
            return topics.values().stream()
                    .map(list -> list.get(0))
                    .map(KafkaTopicDescriptor::cluster)
                    .collect(Collectors.toSet());
        }

        /** @return stream of topic id to a list of all the topic's descriptors. */
        public Stream<Map.Entry<URI, List<KafkaTopicDescriptor<?, ?>>>> stream() {
            return topics.entrySet().stream();
        }

        /**
         * Get all known topic descriptors for the supplied topic resource id
         *
         * @param topicResourceId the resource id of the topic to look up.
         * @return the list of descriptors
         * @throws RuntimeException on unknown topic id.
         */
        public List<KafkaTopicDescriptor<?, ?>> getAll(final URI topicResourceId) {
            final List<KafkaTopicDescriptor<?, ?>> descriptors = topics.get(topicResourceId);
            if (descriptors == null) {
                throw new UnknownTopicOrPartitionException("Unknown topic id: " + topicResourceId);
            }
            return descriptors;
        }
    }
}
