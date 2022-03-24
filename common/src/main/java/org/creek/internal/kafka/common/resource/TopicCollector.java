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

package org.creek.internal.kafka.common.resource;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.creek.api.base.type.Lists;
import org.creek.api.kafka.metadata.CreatableKafkaTopic;
import org.creek.api.kafka.metadata.KafkaTopicDescriptor;
import org.creek.api.platform.metadata.ComponentDescriptor;

public final class TopicCollector {

    private TopicCollector() {}

    /**
     * Collect all topic descriptors, grouping and de-dupping by name.
     *
     * @param components the components to extract topics from.
     * @return the map of topics by name.
     */
    public static Map<String, KafkaTopicDescriptor<?, ?>> collectTopics(
            final Collection<? extends ComponentDescriptor> components) {
        return components.stream()
                .flatMap(ComponentDescriptor::resources)
                .filter(KafkaTopicDescriptor.class::isInstance)
                .map(d -> (KafkaTopicDescriptor<?, ?>) d)
                .collect(
                        groupingBy(
                                KafkaTopicDescriptor::name,
                                collectingAndThen(
                                        Collectors.toList(),
                                        TopicCollector::throwOnDescriptorMismatch)));
    }

    private static KafkaTopicDescriptor<?, ?> throwOnDescriptorMismatch(
            final List<? extends KafkaTopicDescriptor<?, ?>> defs) {
        final List<KafkaTopicDescriptor<?, ?>> reduced =
                defs.stream()
                        .reduce(
                                new ArrayList<>(1),
                                TopicCollector::accumulateTopics,
                                Lists::combineList);

        if (reduced.size() != 1) {
            throw new TopicDescriptorMismatchException(defs);
        }
        return reduced.get(0);
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
}
