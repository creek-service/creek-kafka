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

package org.creekservice.api.kafka.metadata;


import org.creekservice.api.platform.metadata.ResourceDescriptor;

/**
 * Metadata defining a Kafka topic.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KafkaTopicDescriptor<K, V> extends ResourceDescriptor {

    /** @return name of the topic as it is in Kafka. */
    String name();

    /**
     * The logical name of the Kafka cluster the topic resides in.
     *
     * <p>This name is used to look up connection details for the cluster.
     *
     * @return the logical Kafka cluster name.
     */
    default String cluster() {
        return "default";
    }

    /** @return metadata about the topic's key: */
    PartDescriptor<K> key();

    /** @return metadata about the topic's value: */
    PartDescriptor<V> value();

    /** Descriptor for part of a topic's record. */
    interface PartDescriptor<T> {
        /**
         * The serialization format used to serialize this part of the record.
         *
         * <p>A serde provider with a matching serialization format must be available at runtime.
         *
         * @return the part's serialization format.
         */
        SerializationFormat format();

        /**
         * The type serialized in this part of the record.
         *
         * <p>The types supported will depend on the {@link #format()}.
         *
         * @return The part's java type.
         */
        Class<T> type();
    }
}
