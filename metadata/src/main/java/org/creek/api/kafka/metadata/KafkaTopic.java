/*
 * Copyright 2021-2022 Creek Contributors (https://github.com/creek-service)
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

package org.creek.api.kafka.metadata;


import java.util.StringJoiner;

/**
 * Represents a Kafka topic.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KafkaTopic<K, V> {

    /** @return name of the topic as it is in Kafka. */
    String getTopicName();

    /**
     * The type serialized in the topic's record keys.
     *
     * <p>Can be:
     *
     * <ul>
     *   <li>{@code Void.class} if the topic does not have a key.
     *   <li>Built-in types.
     *   <li>Custom type.
     * </ul>
     *
     * @return The type of the key.
     */
    Class<K> getKeyType();

    /**
     * The type serialized in the topic's record values.
     *
     * <ul>
     *   <li>{@code Void.class} if the topic does not have a value.
     *   <li>Built-in types.
     *   <li>Custom type.
     * </ul>
     *
     * @return the type of the value.
     */
    Class<V> getValueType();

    /** @return {@code true} if {@code left} and {@code right} are equivalent. */
    static boolean matches(final KafkaTopic<?, ?> left, final KafkaTopic<?, ?> right) {
        final boolean basics =
                left.getTopicName().equals(right.getTopicName())
                        && left.getKeyType().equals(right.getKeyType())
                        && left.getValueType().equals(right.getValueType());

        if (!basics) {
            return false;
        }

        final boolean leftOwned = left instanceof CreatableKafkaTopic;
        final boolean rightOwned = right instanceof CreatableKafkaTopic;
        if (leftOwned != rightOwned) {
            return false;
        }

        return !leftOwned
                || KafkaTopicConfig.matches(
                        ((CreatableKafkaTopic<?, ?>) left).getConfig(),
                        ((CreatableKafkaTopic<?, ?>) right).getConfig());
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
    static String asString(final KafkaTopic<?, ?> topic) {

        final StringJoiner joiner =
                new StringJoiner(", ", topic.getClass().getSimpleName() + "[", "]")
                        .add("topicName=" + topic.getTopicName())
                        .add("keyType=" + topic.getKeyType().getName())
                        .add("valueType=" + topic.getValueType().getName());

        if (topic instanceof CreatableKafkaTopic) {
            final CreatableKafkaTopic<?, ?> owned = (CreatableKafkaTopic<?, ?>) topic;
            joiner.add("config=" + KafkaTopicConfig.asString(owned.getConfig()));
        }

        return joiner.toString();
    }
}
