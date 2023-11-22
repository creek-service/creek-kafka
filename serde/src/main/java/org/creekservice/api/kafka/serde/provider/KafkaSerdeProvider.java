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

package org.creekservice.api.kafka.serde.provider;

import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor;
import org.creekservice.api.kafka.metadata.KafkaTopicDescriptor.PartDescriptor;
import org.creekservice.api.kafka.metadata.SerializationFormat;

// begin-snippet: kafka-serde-provider
/**
 * Base type for extensions that provide Kafka serde
 *
 * <p>Creek loads extensions using the standard {@link java.util.ServiceLoader}. To be loaded by
 * Creek the provider must be registered in either the {@code module-info.java} file as a {@code
 * provider} of {@link KafkaSerdeProvider} and/or have a suitable entry in the {@code
 * META-INFO.services} directory.
 */
public interface KafkaSerdeProvider {

    enum TopicPart {
        key,
        value
    }

    /**
     * @return the <i>unique</i> serialization format the serde provides.
     */
    SerializationFormat format();

    /**
     * Ensure any resources associated with a topic part are registered, e.g. schemas registered in
     * the appropriate schema store.
     *
     * <p>The method allows serde providers to optionally create / register resources associated
     * with a topic's key or value. Implementations should only ensure resources for the supplied
     * {@code topicPart}.
     *
     * @param part the descriptor for the topic part.
     * @param topicPart Identifies if {@code part} is a key or value part.
     * @param topic the topic the {@code part} belongs to.
     * @param clusterProperties the properties of the cluster the {@code topic} belongs to.
     */
    default void ensureTopicPartResources(
            PartDescriptor<?> part,
            TopicPart topicPart,
            KafkaTopicDescriptor<?, ?> topic,
            Map<String, Object> clusterProperties) {}

    /**
     * Get the serde for the supplied Kafka topic {@code part}.
     *
     * <p>{@link Serde#configure} will be called on the returned serde.
     *
     * @param <T> the type of the part.
     * @param part the descriptor for the topic part.
     * @return the serde to use to serialize and deserialize the part.
     */
    <T> Serde<T> create(PartDescriptor<T> part);
}
// end-snippet
