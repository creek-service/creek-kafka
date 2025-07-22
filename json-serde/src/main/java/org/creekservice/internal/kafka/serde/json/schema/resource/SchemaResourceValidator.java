/*
 * Copyright 2023-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.serde.json.schema.resource;

import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.creekservice.api.base.type.CodeLocation.codeLocation;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;
import org.creekservice.api.kafka.metadata.schema.SchemaDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;

/**
 * Validator of schema resources, e.g. {@link SchemaDescriptor}, etc.
 *
 * <p>Resources such as {@link SchemaDescriptor} are just interfaces. Services are free to implement
 * however they choose. Because of this, validation is a good thing!
 */
final class SchemaResourceValidator {

    /**
     * Validate a group of resources that all describe the same schema, i.e. all have the same id.
     *
     * <p>Also validates the descriptors themselves are valid.
     *
     * @param resourceGroup the group of descriptors to validate.
     * @throws RuntimeException on issues
     */
    void validateGroup(final Collection<? extends SchemaDescriptor<?>> resourceGroup) {
        resourceGroup.forEach(this::validateSchema);

        validateSchemaGroup(resourceGroup);
    }

    private void validateSchema(final SchemaDescriptor<?> res) {
        validatePart(res);
        requireNonNull("id()", res.id(), res);
        requireNonBlank("schemaRegistryName()", res.schemaRegistryName(), res);
        requireNonNull("resources()", res.resources(), res);
    }

    private static void validatePart(final SchemaDescriptor<?> res) {
        requireNonNull("part()", res.part(), res);
        requireNonNull("part().resources()", res.part().resources(), res);
        requireNonNull("part().topic()", res.part().topic(), res);
        requireNonNull("part().topic().id()", res.part().topic().id(), res);

        // Check schema is returned by the owing part as a resource dependency:
        res.part()
                .resources()
                .filter(r -> res == r) // Intentional identify comparison
                .findAny()
                .orElseThrow(
                        () ->
                                new InvalidSchemaDescriptorException(
                                        "part().resources() does not include this schema resource",
                                        res));
    }

    private void validateSchemaGroup(
            final Collection<? extends SchemaDescriptor<?>> resourceGroup) {
        final Map<Integer, ? extends SchemaDescriptor<?>> unique =
                resourceGroup.stream()
                        .collect(
                                toMap(
                                        SchemaResourceValidator::uniqueHash,
                                        Function.identity(),
                                        (v0, v1) -> v0,
                                        LinkedHashMap::new));

        if (unique.size() > 1) {
            throw new InconsistentResourceGroupException(unique.values());
        }
    }

    private static void requireNonNull(
            final String name, final Object value, final SchemaDescriptor<?> descriptor) {
        if (value == null) {
            throw new InvalidSchemaDescriptorException(name + " is null", descriptor);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void requireNonBlank(
            final String name, final String value, final SchemaDescriptor<?> descriptor) {
        requireNonNull(name, value, descriptor);
        if (value.isBlank()) {
            throw new InvalidSchemaDescriptorException(name + " is blank", descriptor);
        }
    }

    private static int uniqueHash(final SchemaDescriptor<?> schema) {
        return Objects.hash(
                schema.schemaRegistryName(),
                schema.part().name(),
                schema.part().topic().id(),
                schema.resources().map(ResourceDescriptor::id).collect(toList()));
    }

    private static String formatId(final SchemaDescriptor<?> descriptor) {
        try {
            return descriptor.id().toString();
        } catch (final Exception e) {
            return "unknown";
        }
    }

    private static final class InvalidSchemaDescriptorException extends RuntimeException {
        InvalidSchemaDescriptorException(final String msg, final SchemaDescriptor<?> descriptor) {
            super(formatMsg(msg, descriptor));
        }

        private static String formatMsg(final String msg, final SchemaDescriptor<?> descriptor) {
            final Optional<PartDescriptor<?>> part = Optional.ofNullable(descriptor.part());
            final String partName =
                    part.map(PartDescriptor::name).map(Objects::toString).orElse("null");
            final String topicName =
                    part.map(PartDescriptor::topic)
                            .map(KafkaTopicDescriptor::id)
                            .map(Objects::toString)
                            .orElse("unknown");

            return "Invalid schema descriptor: "
                    + msg
                    + ", id: "
                    + formatId(descriptor)
                    + ", topic: "
                    + topicName
                    + ", part: "
                    + partName
                    + ", location: "
                    + codeLocation(descriptor);
        }
    }

    private static final class InconsistentResourceGroupException extends RuntimeException {
        InconsistentResourceGroupException(final Collection<? extends SchemaDescriptor<?>> unique) {
            super(
                    "Resource descriptors for the same resource disagree on the details"
                            + ", id: "
                            + unique.iterator().next().id()
                            + ", descriptors: "
                            + format(unique));
        }

        private static String format(final Collection<? extends SchemaDescriptor<?>> unique) {
            return unique.stream()
                    .map(t -> "\t" + formatDescriptor(t))
                    .collect(
                            joining(lineSeparator(), "[" + lineSeparator(), lineSeparator() + "]"));
        }

        private static String formatDescriptor(final SchemaDescriptor<?> schema) {
            return new StringJoiner(", ", schema.getClass().getSimpleName() + "[", "]")
                    .add("id: " + formatId(schema))
                    .add("topic: " + schema.part().topic().id())
                    .add("schemaRegistryName: " + schema.schemaRegistryName())
                    .add("part: " + schema.part().name())
                    .add(
                            "resources: "
                                    + schema.resources()
                                            .map(ResourceDescriptor::id)
                                            .collect(toList()))
                    .add("location: " + codeLocation(schema))
                    .toString();
        }
    }
}
