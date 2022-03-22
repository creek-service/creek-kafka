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

package org.creek.api.kafka.serde.provider;


import org.apache.kafka.common.serialization.Serde;
import org.creek.api.kafka.metadata.KafkaTopicDescriptor;
import org.creek.api.kafka.metadata.SerializationFormat;

/**
 * Base type for extensions that provide Kafka serde
 *
 * <p>Creek loads extensions using the standard {@link java.util.ServiceLoader}. To be loaded by
 * Creek the provider must be registered in either the {@code module-info.java} file as a {@code
 * provider} of {@link KafkaSerdeProvider} and/or have a suitable entry in the {@code
 * META-INFO.services} directory.
 */
public interface KafkaSerdeProvider {

    /** @return the serialization format the serde provides. */
    SerializationFormat format();

    /**
     * Get the serde for the supplied Kafka record {@code part}.
     *
     * @param part the part descriptor
     * @param <T> the type of the key.
     * @return the serde.
     */
    <T> Serde<T> create(KafkaTopicDescriptor.PartDescriptor<T> part);
}
