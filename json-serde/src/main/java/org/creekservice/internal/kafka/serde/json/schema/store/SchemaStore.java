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

package org.creekservice.internal.kafka.serde.json.schema.store;

import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor.PartDescriptor;

/** Provides access to a schema registry for registering and loading JSON schemas. */
public interface SchemaStore {

    /**
     * Load the schema for the given topic part from the classpath and register it in the schema
     * registry, checking compatibility with any existing versions.
     *
     * @param <T> the Java type of the topic part.
     * @param part the topic part descriptor.
     * @return the registered schema.
     */
    <T> RegisteredSchema<T> registerFromClasspath(PartDescriptor<T> part);

    /**
     * Load the schema for the given topic part from the classpath and retrieve its registration
     * details from the schema registry.
     *
     * @param <T> the Java type of the topic part.
     * @param part the topic part descriptor.
     * @return the registered schema.
     */
    <T> RegisteredSchema<T> loadFromClasspath(PartDescriptor<T> part);
}
