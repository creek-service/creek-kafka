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

import java.util.Objects;
import java.util.StringJoiner;
import org.creekservice.api.kafka.metadata.CreatableKafkaTopic;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;

/** Util class for working with implementations of {@link KafkaTopicDescriptor} */
public final class KafkaTopicDescriptors {

    private KafkaTopicDescriptors() {}
    /**
     * Comparison method that avoids code relying on different implementations of {@link
     * KafkaTopicDescriptor} implementing {@code equals} correctly.
     *
     * @param left left descriptor
     * @param right right descriptor
     * @return {@code true} if {@code left} and {@code right} are equivalent.
     */
    public static boolean matches(
            final KafkaTopicDescriptor<?, ?> left, final KafkaTopicDescriptor<?, ?> right) {
        if (left == right) {
            return true;
        }

        if (!Objects.equals(left.id(), right.id())) {
            return false;
        }

        if (!Objects.equals(left.name(), right.name())) {
            return false;
        }

        if (!Objects.equals(left.cluster(), right.cluster())) {
            return false;
        }

        if (!matches(left.key(), right.key())) {
            return false;
        }

        if (!matches(left.value(), right.value())) {
            return false;
        }

        final boolean leftCreatable = left instanceof CreatableKafkaTopic;
        final boolean rightCreatable = right instanceof CreatableKafkaTopic;
        if (leftCreatable && rightCreatable) {
            return KafkaTopicConfigs.matches(
                    ((CreatableKafkaTopic<?, ?>) left).config(),
                    ((CreatableKafkaTopic<?, ?>) right).config());
        }

        return true;
    }

    /**
     * Convert topic details to string.
     *
     * <p>Used when logging topic details. Avoids the need for every implementor of this type to
     * define {@code toString}.
     *
     * @param topic the topic to convert
     * @return string representation
     */
    public static String asString(final KafkaTopicDescriptor<?, ?> topic) {

        final StringJoiner joiner =
                new StringJoiner(", ", topic.getClass().getSimpleName() + "[", "]")
                        .add("id=" + topic.id())
                        .add("name=" + topic.name())
                        .add("cluster=" + topic.cluster())
                        .add("key=" + (topic.key() == null ? "null" : asString(topic.key())))
                        .add("value=" + (topic.value() == null ? "null" : asString(topic.value())));

        if (topic instanceof CreatableKafkaTopic) {
            final CreatableKafkaTopic<?, ?> creatable = (CreatableKafkaTopic<?, ?>) topic;
            joiner.add(
                    "config="
                            + (creatable.config() == null
                                    ? "null"
                                    : KafkaTopicConfigs.asString(creatable.config())));
        }

        return joiner.toString();
    }

    /**
     * Compute hash code of topic descriptor.
     *
     * <p>Avoids Creek being at the mercy of implementers of the interface.
     *
     * @param topic the topic descriptor.
     * @return the hash code.
     */
    public static int hashCode(final KafkaTopicDescriptor<?, ?> topic) {

        if (topic instanceof CreatableKafkaTopic) {
            final CreatableKafkaTopic<?, ?> creatable = (CreatableKafkaTopic<?, ?>) topic;
            return Objects.hash(
                    creatable.id(),
                    creatable.name(),
                    hashCode(creatable.key()),
                    hashCode(creatable.value()),
                    KafkaTopicConfigs.hashCode(creatable.config()));
        }

        return hashCodeIgnoringConfig(topic);
    }

    /**
     * Compute hash code of topic descriptor, ignoring any config.
     *
     * @param topic the topic descriptor.
     * @return the hash code.
     */
    public static int hashCodeIgnoringConfig(final KafkaTopicDescriptor<?, ?> topic) {
        return Objects.hash(
                topic.id(), topic.name(), hashCode(topic.key()), hashCode(topic.value()));
    }

    /**
     * Comparison method that avoids code relying on different implementations of {@link
     * KafkaTopicDescriptor.PartDescriptor} implementing {@code equals} correctly.
     *
     * @param left left descriptor
     * @param right right descriptor
     * @return {@code true} if {@code left} and {@code right} are equivalent.
     */
    public static boolean matches(
            final KafkaTopicDescriptor.PartDescriptor<?> left,
            final KafkaTopicDescriptor.PartDescriptor<?> right) {
        return Objects.equals(left.format(), right.format())
                && Objects.equals(left.type(), right.type());
    }

    /**
     * Convert part descriptor to a string.
     *
     * <p>Used when logging topic details. Avoids the need for every implementor of this type to
     * define {@code toString}.
     *
     * @param part the part to convert
     * @return string representation
     */
    public static String asString(final KafkaTopicDescriptor.PartDescriptor<?> part) {
        final StringJoiner joiner =
                new StringJoiner(", ", part.getClass().getSimpleName() + "[", "]")
                        .add("format=" + part.format())
                        .add("type=" + (part.type() == null ? "null" : part.type().getName()));

        return joiner.toString();
    }

    /**
     * Compute hash code of part.
     *
     * <p>Avoids Creek being at the mercy of implementers of the interface.
     *
     * @param part the part.
     * @return the hash code.
     */
    public static int hashCode(final KafkaTopicDescriptor.PartDescriptor<?> part) {
        return Objects.hash(part.format(), part.type());
    }
}
