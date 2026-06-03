/*
 * Copyright 2026 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.internal.kafka.streams.test.extension.testsuite;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.api.kafka.metadata.schema.SchemaDescriptor;
import org.creekservice.api.platform.metadata.ComponentDescriptor;
import org.creekservice.api.platform.metadata.ResourceCollection;

/**
 * Collects {@link SchemaDescriptor schema descriptors} from {@link ComponentDescriptor component
 * descriptors}
 */
public final class SchemaCollector {

    /**
     * Collect schema registry information from the supplied {@code components}.
     *
     * @param components the components to extract schema registry information from.
     * @return Object that can be queried for information on the collected schema registries.
     */
    public CollectedSchemaRegistries collectSchemaRegistries(
            final Collection<? extends ComponentDescriptor> components) {

        final Map<String, String> registryToCluster = new HashMap<>();

        components.stream()
                .flatMap(ResourceCollection::collectResources)
                .filter(SchemaDescriptor.class::isInstance)
                .map(d -> (SchemaDescriptor<?>) d)
                .forEach(
                        schema ->
                                registryToCluster.merge(
                                        schema.schemaRegistryName(),
                                        schema.part().topic().cluster(),
                                        (existing, newCluster) -> {
                                            if (existing.equals(newCluster)) {
                                                return existing;
                                            }
                                            throw new IllegalArgumentException(
                                                    "Schema registry '"
                                                            + schema.schemaRegistryName()
                                                            + "' is associated with multiple"
                                                            + " Kafka clusters: '"
                                                            + existing
                                                            + "' and '"
                                                            + newCluster
                                                            + "'");
                                        }));

        return new CollectedSchemaRegistries(registryToCluster);
    }

    /** Holds the result of a schema registry collection. */
    public static final class CollectedSchemaRegistries {
        private final Map<String, String> registryToCluster;

        @VisibleForTesting
        CollectedSchemaRegistries(final Map<String, String> registryToCluster) {
            this.registryToCluster =
                    Map.copyOf(requireNonNull(registryToCluster, "registryToCluster"));
        }

        /**
         * @return the set of schema registry instance names found in all collected schemas.
         */
        public Set<String> registryNames() {
            return registryToCluster.keySet();
        }

        /**
         * Get the Kafka cluster name associated with a schema registry.
         *
         * @param registryName the schema registry instance name.
         * @return the cluster name.
         * @throws UnknownSchemaRegistryException if the registry name is not known.
         */
        public String clusterFor(final String registryName) {
            final String cluster =
                    registryToCluster.get(requireNonNull(registryName, "registryName"));
            if (cluster == null) {
                throw new UnknownSchemaRegistryException(registryName);
            }
            return cluster;
        }

        /**
         * @return true if no schema registries were found.
         */
        public boolean isEmpty() {
            return registryToCluster.isEmpty();
        }
    }

    private static final class UnknownSchemaRegistryException extends RuntimeException {
        UnknownSchemaRegistryException(final String registryName) {
            super(
                    "Unknown schema registry: '"
                            + registryName
                            + "'. Known registries: check component descriptors include topics"
                            + " that reference this registry.");
        }
    }
}
