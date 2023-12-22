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

import java.util.Collection;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
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

    /**
     * @return the <i>unique</i> serialization format the serde provides.
     */
    SerializationFormat format();

    /**
     * Initialise a {@link SerdeProvider} for the supplied {@code clusterName}.
     *
     * <p>This method exists to allow serde providers to initialise per-Kafka-cluster state. For
     * example, to cache any required schemas.
     *
     * @param clusterName the logical name of the cluster, as used in topic descriptors.
     * @param params provides access to additional types and information in an extendable way.
     * @return an initialised serde provider.
     */
    SerdeProvider initialize(String clusterName, InitializeParams params);

    /** Extendable way of providing additional information to the {@link #initialize} method. */
    interface InitializeParams {

        /**
         * Retrieve the override instance for the supplied {@code type}, if one is set.
         *
         * @param type the type to look up.
         * @return the instance to use, if set, otherwise {@link Optional#empty()}.
         * @param <T> the type to look up.
         */
        <T> Optional<T> typeOverride(Class<T> type);
    }

    /** Initialised, per-cluster, provider of serde instances. */
    interface SerdeProvider {
        /**
         * Ensure any resources associated with a topic part are registered, e.g. schemas registered
         * in the appropriate schema store.
         *
         * <p>The method allows serde providers to optionally create / register resources associated
         * with a topic's key or value. Implementations should only ensure resources for the
         * supplied {@code topicPart}.
         *
         * @param parts the descriptors for the topic parts.
         */
        default void ensureExternalResources(Collection<? extends PartDescriptor<?>> parts) {}

        /**
         * Get the serde for the supplied Kafka topic {@code part}.
         *
         * <p>{@link Serde#configure} will be called on the returned serde.
         *
         * @param <T> the type of the part.
         * @param part the descriptor for the topic part.
         * @return the serde to use to serialize and deserialize the part.
         */
        <T> Serde<T> createSerde(PartDescriptor<T> part);
    }
}
// end-snippet
