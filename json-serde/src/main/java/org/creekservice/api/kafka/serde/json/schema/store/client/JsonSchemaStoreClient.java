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

package org.creekservice.api.kafka.serde.json.schema.store.client;

import java.util.List;
import org.creekservice.api.kafka.serde.json.schema.ProducerSchema;
import org.creekservice.api.kafka.serde.json.schema.store.endpoint.SchemaStoreEndpoints;

/** Client for interacting with a JSON Schema store. */
public interface JsonSchemaStoreClient {

    /**
     * Disable the broken schema registry compatability checks for JSON.
     *
     * @param subject the subject to disable checks for.
     */
    void disableCompatability(String subject);

    /**
     * Ensure the supplied {@code schema} is registered under the supplied {@code subject}.
     *
     * @param subject the subject to register under.
     * @param schema the schema to register.
     * @return the id of the registered schema.
     * @throws RuntimeException on error.
     */
    int register(String subject, ProducerSchema schema);

    /**
     * Retrieve the id the supplied {@code schema} under the supplied {@code subject}.
     *
     * <p>The schema needs to have previously been registered.
     *
     * @param subject the subject to search under.
     * @param schema the schema to search for.
     * @return the id of the registered schema.
     * @throws RuntimeException on error.
     */
    int registeredId(String subject, ProducerSchema schema);

    /**
     * Retrieve all schemas registered under the supplied {@code subject}.
     *
     * @param subject the subject to look-up.
     * @return the list of current schemas versions.
     * @throws RuntimeException on error.
     */
    List<VersionedSchema> allVersions(String subject);

    /** Versioned schema data */
    interface VersionedSchema {

        /**
         * @return version of the schema for a specific subject
         */
        int version();

        /**
         * @return schema
         */
        ProducerSchema schema();
    }

    /**
     * Factory for creating client instances.
     *
     * <p>This type can be customised via the {@link
     * org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions.Builder#withTypeOverride}
     * method. Pass {@code JsonSchemaStoreClient.Factory.class} as the first param and a custom
     * implementation as the second.
     *
     * <p>If not customised, the default {@link
     * org.creekservice.internal.kafka.serde.json.schema.store.client.DefaultJsonSchemaRegistryClient}
     * will be used.
     */
    interface Factory {

        /**
         * Create a schema store client.
         *
         * @param schemaRegistryName the logical name of the scheme registry, as used in topic
         *     descriptors.
         * @param endpoint the endpoint information for the remote store.
         * @return a new store client
         */
        JsonSchemaStoreClient create(String schemaRegistryName, SchemaStoreEndpoints endpoint);
    }
}
