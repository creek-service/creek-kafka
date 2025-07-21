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

package org.creekservice.internal.kafka.extension.resource;

import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.creekservice.api.base.type.CodeLocation.codeLocation;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.creekservice.api.kafka.metadata.topic.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicConfig;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;

/**
 * Validator of Kafka based service resources, e.g. {@link KafkaTopicDescriptor}, etc.
 *
 * <p>Resources such as {@link KafkaTopicDescriptor} are just interfaces. Services are free to
 * implement however they choose. Because of this, validation is a good thing!
 */
final class KafkaResourceValidator {

    /**
     * Validate a group of resources that all describe the same topic, i.e. all have the same id.
     *
     * <p>Also validates the descriptors themselves are valid.
     *
     * @param resourceGroup the group of descriptors to validate.
     * @throws RuntimeException on issues
     */
    void validateGroup(final Collection<? extends KafkaTopicDescriptor<?, ?>> resourceGroup) {
        resourceGroup.forEach(KafkaResourceValidator::validateTopic);
        validateTopicGroup(resourceGroup);
    }

    private static void validateTopicGroup(
            final Collection<? extends KafkaTopicDescriptor<?, ?>> resourceGroup) {
        final Map<Integer, ? extends KafkaTopicDescriptor<?, ?>> uniqueIgnoringConfig =
                resourceGroup.stream()
                        .collect(
                                Collectors.toMap(
                                        KafkaResourceValidator::uniqueHashIgnoringConfig,
                                        Function.identity(),
                                        (v0, v1) -> v0));

        if (uniqueIgnoringConfig.size() > 1) {
            throw InconsistentResourceGroupException.mismatch(
                    resourceGroup, uniqueIgnoringConfig.values());
        }

        final Map<Integer, ? extends KafkaTopicDescriptor<?, ?>> uniqueConfig =
                resourceGroup.stream()
                        .filter(CreatableKafkaTopic.class::isInstance)
                        .map(topic -> (CreatableKafkaTopic<?, ?>) topic)
                        .collect(
                                Collectors.toMap(
                                        KafkaResourceValidator::uniqueHashOfConfig,
                                        Function.identity(),
                                        (v0, v1) -> v0));

        if (uniqueConfig.size() > 1) {
            throw InconsistentResourceGroupException.mismatch(resourceGroup, uniqueConfig.values());
        }
    }

    private static void validateTopic(final KafkaTopicDescriptor<?, ?> descriptor) {
        validateClusterName(descriptor);
        requireNonNull("id()", descriptor.id(), descriptor);
        requireNonBlank("name()", descriptor.name(), descriptor);
        validatePart("key()", descriptor.key(), descriptor);
        validatePart("value()", descriptor.value(), descriptor);

        if (descriptor instanceof CreatableKafkaTopic) {
            final CreatableKafkaTopic<?, ?> creatable = (CreatableKafkaTopic<?, ?>) descriptor;
            requireNonNull("config()", creatable.config(), descriptor);
        }
    }

    private static void validateClusterName(final KafkaTopicDescriptor<?, ?> descriptor) {
        final String cluster = requireNonBlank("cluster()", descriptor.cluster(), descriptor);

        cluster.chars()
                .filter(c -> !(Character.isDigit(c) || Character.isAlphabetic(c) || c == '-'))
                .findFirst()
                .ifPresent(
                        c -> {
                            throw new InvalidTopicDescriptorException(
                                    "cluster() is invalid: illegal character '"
                                            + (char) c
                                            + "'. Only alpha-numerics and '-' are supported.",
                                    descriptor);
                        });
    }

    private static void validatePart(
            final String name,
            final KafkaTopicDescriptor.PartDescriptor<?> part,
            final KafkaTopicDescriptor<?, ?> descriptor) {
        requireNonNull(name, part, descriptor);
        requireNonNull(name + ".name()", part.name(), descriptor);
        requireNonNull(name + ".format()", part.format(), descriptor);
        requireNonNull(name + ".type()", part.type(), descriptor);
        requireNonNull(name + ".resources()", part.resources(), descriptor);
        requireNonNull(name + ".topic()", part.topic(), descriptor);

        if (part.topic() != descriptor) {
            throw new InvalidTopicDescriptorException(
                    name + ".topic() does not return the owning topic", descriptor);
        }

        final Set<URI> topicResources =
                part.topic().resources().map(ResourceDescriptor::id).collect(toSet());
        final String missing =
                part.resources()
                        .map(ResourceDescriptor::id)
                        .filter(
                                res ->
                                        !topicResources.contains(
                                                res)) // Intentional identify comparison
                        .map(Object::toString)
                        .collect(joining(","));
        if (!missing.isEmpty()) {
            throw new InvalidTopicDescriptorException(
                    name
                            + ".topic().resources() does not include all "
                            + name
                            + " resources, missing: "
                            + missing,
                    descriptor);
        }
    }

    private static void requireNonNull(
            final String name, final Object value, final KafkaTopicDescriptor<?, ?> descriptor) {
        if (value == null) {
            throw new InvalidTopicDescriptorException(name + " is null", descriptor);
        }
    }

    private static String requireNonBlank(
            final String name, final String value, final KafkaTopicDescriptor<?, ?> descriptor) {
        requireNonNull(name, value, descriptor);
        if (value.isBlank()) {
            throw new InvalidTopicDescriptorException(name + " is blank", descriptor);
        }
        return value;
    }

    private static int uniqueHashIgnoringConfig(final KafkaTopicDescriptor<?, ?> topic) {
        return Objects.hash(
                topic.id(),
                topic.name(),
                topic.cluster(),
                uniqueHash(topic.key()),
                uniqueHash(topic.value()),
                topic.resources().map(ResourceDescriptor::id).collect(toList()));
    }

    private static int uniqueHashOfConfig(final CreatableKafkaTopic<?, ?> topic) {
        return Objects.hash(topic.config().partitions(), topic.config().config());
    }

    private static int uniqueHash(final KafkaTopicDescriptor.PartDescriptor<?> part) {
        return Objects.hash(
                part.name(), part.format(), part.type()
                // Known to be valid link to parent:
                // part.topic()
                // Known to be inc. in of topic().resources():
                // part.resources()
                );
    }

    private static String format(final KafkaTopicDescriptor<?, ?> topic) {
        final StringJoiner joiner =
                new StringJoiner(", ", topic.getClass().getSimpleName() + "[", "]")
                        .add("id: " + formatId(topic))
                        .add("name: " + topic.name())
                        .add("cluster: " + topic.cluster())
                        .add("key: " + (topic.key() == null ? "null" : format(topic.key())))
                        .add("value: " + (topic.value() == null ? "null" : format(topic.value())));

        if (topic instanceof CreatableKafkaTopic) {
            joiner.add(
                    "config: "
                            + Optional.of((CreatableKafkaTopic<?, ?>) topic)
                                    .map(CreatableKafkaTopic::config)
                                    .map(KafkaResourceValidator::format)
                                    .orElse("null"));
        }

        return joiner.toString();
    }

    private static String formatId(final KafkaTopicDescriptor<?, ?> topic) {
        try {
            return topic.id().toString();
        } catch (final Exception e) {
            return "invalid";
        }
    }

    private static String format(final KafkaTopicDescriptor.PartDescriptor<?> part) {
        final StringJoiner joiner =
                new StringJoiner(", ", part.getClass().getSimpleName() + "[", "]")
                        .add("format: " + part.format())
                        .add("type: " + (part.type() == null ? "null" : part.type().getName()));

        return joiner.toString();
    }

    private static String format(final KafkaTopicConfig config) {
        final StringJoiner joiner =
                new StringJoiner(", ", config.getClass().getSimpleName() + "[", "]")
                        .add("partitions: " + config.partitions())
                        .add("config: " + config.config());

        return joiner.toString();
    }

    private static final class InvalidTopicDescriptorException extends RuntimeException {
        InvalidTopicDescriptorException(
                final String msg, final KafkaTopicDescriptor<?, ?> descriptor) {
            super("Invalid topic descriptor: " + msg + lineSeparator() + format(descriptor));
        }
    }

    private static final class InconsistentResourceGroupException extends RuntimeException {
        InconsistentResourceGroupException(final String msg) {
            super(msg);
        }

        static InconsistentResourceGroupException mismatch(
                final Collection<? extends KafkaTopicDescriptor<?, ?>> resourceGroup,
                final Collection<? extends KafkaTopicDescriptor<?, ?>> unique) {
            final String descriptors =
                    unique.stream()
                            .map(t -> "\t" + format(t) + " (" + codeLocation(t) + ")")
                            .collect(
                                    Collectors.joining(
                                            lineSeparator(),
                                            "[" + lineSeparator(),
                                            lineSeparator() + "]"));

            return new InconsistentResourceGroupException(
                    "Resource descriptors for the same resource disagree on the details."
                            + " id: "
                            + resourceGroup.iterator().next().id()
                            + ", descriptors: "
                            + descriptors);
        }
    }
}
