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

import java.net.URI;
import java.net.URISyntaxException;
import org.creekservice.api.platform.metadata.ResourceDescriptor;

/**
 * Metadata defining a Kafka topic.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface KafkaTopicDescriptor<K, V> extends ResourceDescriptor {

    /** The default cluster name to use if one is not provided. */
    String DEFAULT_CLUSTER_NAME = "default";

    /** The schema name to use in resource URIs for Kafka optics. */
    String SCHEME = "kafka-topic";

    @Override
    default URI id() {
        return resourceId(cluster(), name());
    }

    /**
     * @return name of the topic as it is in Kafka.
     */
    String name();

    /**
     * The logical name of the Kafka cluster the topic resides in.
     *
     * <p>This name is used to look up connection details for the cluster.
     *
     * <p>The name should be limited to alphanumeric characters and hyphen {@code -}.
     *
     * @return the logical Kafka cluster name.
     */
    default String cluster() {
        return DEFAULT_CLUSTER_NAME;
    }

    /**
     * @return metadata about the topic's key:
     */
    PartDescriptor<K> key();

    /**
     * @return metadata about the topic's value:
     */
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

    /**
     * Construct a unique and consistent resource id for a topic.
     *
     * @param cluster the logical name of the cluster the topic is on.
     * @param topic the name of the topic.
     * @return the unique resource id.
     */
    static URI resourceId(final String cluster, final String topic) {
        try {
            return new URI(SCHEME, cluster, "/" + topic, null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
