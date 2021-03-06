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

package org.creekservice.internal.kafka.common.resource;


import java.util.stream.Stream;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;

/**
 * Validator of Kafka based service resources, e.g. {@link KafkaTopicDescriptor}, etc.
 *
 * <p>Resources such as {@link KafkaTopicDescriptor} are just interfaces. Services are free to
 * implement however they choose. Because of this, validation is a good thing!
 */
public final class KafkaResourceValidator {

    /**
     * Validate Kafka resources used by each of the supplied {@code components}
     *
     * @param components the components to validate
     */
    public void validate(final Stream<? extends ComponentDescriptor> components) {
        components
                .flatMap(ComponentDescriptor::resources)
                .filter(KafkaTopicDescriptor.class::isInstance)
                .map(d -> (KafkaTopicDescriptor<?, ?>) d)
                .forEach(KafkaResourceValidator::validate);
    }

    private static void validate(final KafkaTopicDescriptor<?, ?> descriptor) {
        requireNonBlank("name()", descriptor.name(), descriptor);
        validateClusterName(descriptor);
        validatePart("key()", descriptor.key(), descriptor);
        validatePart("value()", descriptor.value(), descriptor);
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
        requireNonNull(name + ".type()", part.type(), descriptor);
        requireNonNull(name + ".format()", part.format(), descriptor);
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
        if (descriptor.name().isBlank()) {
            throw new InvalidTopicDescriptorException(name + " is blank", descriptor);
        }
        return value;
    }

    private static final class InvalidTopicDescriptorException extends RuntimeException {
        InvalidTopicDescriptorException(
                final String msg, final KafkaTopicDescriptor<?, ?> descriptor) {
            super(
                    "Invalid topic descriptor: "
                            + msg
                            + System.lineSeparator()
                            + KafkaTopicDescriptors.asString(descriptor));
        }
    }
}
