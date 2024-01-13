/*
 * Copyright 2023-2024 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.metadata.schema;

import java.net.URI;
import java.net.URISyntaxException;
import org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor;
import org.creekservice.api.platform.metadata.ResourceDescriptor;

/**
 * Base type for all schema descriptors held in the Confluent schema registry.
 *
 * @param <T> the type the schema describes
 */
public interface SchemaDescriptor<T> extends ResourceDescriptor {

    /** The default schema registry instance name to use if one is not provided. */
    String DEFAULT_SCHEMA_REGISTRY_NAME = "default";

    /**
     * @return the logical name for the schema registry this schema should be registered in.
     */
    default String schemaRegistryName() {
        return DEFAULT_SCHEMA_REGISTRY_NAME;
    }

    /**
     * @return the part of the topic this instance describes the schema for.
     */
    KafkaTopicDescriptor.PartDescriptor<T> part();

    /**
     * Construct a unique and consistent resource id for a schema.
     *
     * @param schemaRegistryInstance the logical name of the schema registry instance.
     * @param part the part of the topic this schema describes.
     * @return the unique resource id.
     */
    static URI resourceId(
            final String schemaRegistryInstance,
            final KafkaTopicDescriptor.PartDescriptor<?> part) {
        try {
            return new URI(
                    "schema",
                    schemaRegistryInstance,
                    "/" + part.topic().name() + "/" + part.name(),
                    null);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
