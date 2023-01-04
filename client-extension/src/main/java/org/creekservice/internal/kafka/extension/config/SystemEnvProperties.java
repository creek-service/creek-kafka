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

package org.creekservice.internal.kafka.extension.config;

import java.util.Optional;

/**
 * Util class for mapping between Kafka Client Property names and system environment variable names.
 */
public final class SystemEnvProperties {

    /** The environment variable name prefix for kafka configuration. */
    public static final String KAFKA_PREFIX = "KAFKA_";

    private SystemEnvProperties() {}

    /**
     * Get the environment variable name prefix for the supplied {@code clusterName}
     *
     * @param clusterName the cluster name, as returned by {@link
     *     org.creekservice.api.kafka.metadata.KafkaTopicDescriptor#cluster()}
     * @return the environment variable name prefix.
     */
    public static String prefix(final String clusterName) {
        return clusterName.isBlank()
                ? KAFKA_PREFIX
                : KAFKA_PREFIX + clusterName.replaceAll("-", "_").toUpperCase() + "_";
    }

    /**
     * Convert a Kafka client {@code propertyName} into a variable name for the supplied {@code
     * clusterName}
     *
     * <p>If cluster name is empty then the variable name returned will be a common name, i.e.
     * affecting all clusters.
     *
     * @param propertyName the property name to convert, e.g. {@code "bootstrap.servers"}
     * @param clusterName the cluster name, may be empty if the variable should affect all clusters.
     * @return the name to use for the environment name
     */
    public static String varName(final String propertyName, final String clusterName) {
        return prefix(clusterName) + propertyName.replaceAll("\\.", "_").toUpperCase();
    }

    /**
     * Convert an environment {@code variableName} into a Kafka client property name for the
     * supplied {@code clusterName}
     *
     * @param variableName the environment variable name to convert.
     * @param clusterName the cluster name, may be empty if the variable affects all clusters.
     * @return the Kafka client property name.
     */
    public static Optional<String> propertyName(
            final String variableName, final String clusterName) {
        final String prefix = prefix(clusterName);
        if (!variableName.startsWith(prefix)) {
            return Optional.empty();
        }
        final String propName = variableName.substring(prefix.length());
        return propName.isEmpty()
                ? Optional.empty()
                : Optional.of(propName.replaceAll("_", ".").toLowerCase());
    }
}
