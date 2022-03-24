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


import java.util.Objects;
import java.util.StringJoiner;
import org.creek.api.kafka.metadata.CreatableKafkaTopic;
import org.creek.api.kafka.metadata.KafkaTopicConfig;
import org.creek.api.kafka.metadata.KafkaTopicDescriptor;

public final class KafkaTopicDescriptors {

    private KafkaTopicDescriptors() {}
    /**
     * Comparison method that avoids code relying on different implementations of {@link
     * KafkaTopicDescriptor} implementing {@code equals} correctly.
     *
     * @return {@code true} if {@code left} and {@code right} are equivalent.
     */
    public static boolean matches(
            final KafkaTopicDescriptor<?, ?> left, final KafkaTopicDescriptor<?, ?> right) {
        if (left == right) {
            return true;
        }

        if (!Objects.equals(left.name(), right.name())) {
            return false;
        }

        if (!matches(left.key(), right.key())) {
            return false;
        }

        if (!matches(left.value(), right.value())) {
            return false;
        }

        final boolean leftOwned = left instanceof CreatableKafkaTopic;
        final boolean rightOwned = right instanceof CreatableKafkaTopic;
        if (leftOwned && rightOwned) {
            return KafkaTopicConfig.matches(
                    ((CreatableKafkaTopic<?, ?>) left).config(),
                    ((CreatableKafkaTopic<?, ?>) right).config());
        }

        return true;
    }

    /**
     * Convert topic details to string.
     *
     * <p>Used when logging topic details.
     * Avoids the need for every implementor of this type to define {@code toString).
     *
     * @param topic the topic to convert
     * @return string representation
     */
    public static String asString(final KafkaTopicDescriptor<?, ?> topic) {

        final StringJoiner joiner =
                new StringJoiner(", ", topic.getClass().getSimpleName() + "[", "]")
                        .add("name=" + topic.name())
                        .add("key=" + asString(topic.key()))
                        .add("value=" + asString(topic.value()));

        if (topic instanceof CreatableKafkaTopic) {
            final CreatableKafkaTopic<?, ?> owned = (CreatableKafkaTopic<?, ?>) topic;
            joiner.add("config=" + KafkaTopicConfig.asString(owned.config()));
        }

        return joiner.toString();
    }

    /**
     * Comparison method that avoids code relying on different implementations of {@link
     * KafkaTopicDescriptor.PartDescriptor} implementing {@code equals} correctly.
     *
     * @return {@code true} if {@code left} and {@code right} are equivalent.
     */
    public static boolean matches(
            final KafkaTopicDescriptor.PartDescriptor<?> left,
            final KafkaTopicDescriptor.PartDescriptor<?> right) {
        if (left == right) {
            return true;
        }

        return Objects.equals(left.format(), right.format())
                && Objects.equals(left.type(), right.type());
    }

    /**
     * Convert part descriptor to a string.
     *
     * <p>Used when logging topic details.
     * Avoids the need for every implementor of this type to define {@code toString).
     *
     * @param part the part to convert
     * @return string representation
     */
    public static String asString(final KafkaTopicDescriptor.PartDescriptor<?> part) {
        final StringJoiner joiner =
                new StringJoiner(", ", part.getClass().getSimpleName() + "[", "]")
                        .add("format=" + part.format())
                        .add("type=" + part.type().getName());

        return joiner.toString();
    }
}
