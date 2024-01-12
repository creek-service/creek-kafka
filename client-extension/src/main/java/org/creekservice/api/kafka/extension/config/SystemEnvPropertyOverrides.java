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

package org.creekservice.api.kafka.extension.config;

import static org.creekservice.api.kafka.extension.config.ClustersProperties.propertiesBuilder;
import static org.creekservice.internal.kafka.extension.config.SystemEnvProperties.prefix;
import static org.creekservice.internal.kafka.extension.config.SystemEnvProperties.propertyName;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.creekservice.api.base.annotation.VisibleForTesting;
import org.creekservice.internal.kafka.extension.config.SystemEnvProperties;

/**
 * Loads Kafka client property overrides from environment variables.
 *
 * <p>Variables can either target a specific cluster name or be common across all clusters.
 *
 * <p>Common variables, not targeted at a specific cluster, or where the service only accesses a
 * single cluster (which is the most common pattern), should have variable names in the format:
 * {@code KAFKA_&lt;PROPERTY_NAME&gt;} where:
 *
 * <ul>
 *   <li><i>PROPERTY_NAME</i> is Kafka client property name, in uppercase, with periods {@code .}
 *       replaced with underscores {@code _}
 * </ul>
 *
 * <p>For example, config {@code boostrap.servers} can be set with a variable name of {@code
 * KAFKA_BOOTSTRAP_SERVERS}.
 *
 * <p>In the unusual situation that a service access multiple Kafka clusters, a variable can target
 * at a specific Kafka cluster. Such variables should have a name in the format: {@code
 * KAFKA_&lt;CLUSTER_NAME&gt;_&lt;PROPERTY_NAME&gt;} where:
 *
 * <ul>
 *   <li><i>CLUSTER_NAME</i> is the specific name of the Kafka cluster, as returned by {@link
 *       org.creekservice.api.kafka.metadata.topic.KafkaTopicDescriptor#cluster()}), in uppercase
 *       and any dashes {@code -} replaced with underscores {@code _}.
 *   <li><i>PROPERTY_NAME</i> is Kafka client property name, in uppercase, with periods {@code .}
 *       replaced with underscores {@code _}
 * </ul>
 *
 * <p>For example, config {@code boostrap.servers} for the {@code main-cluster} cluster can be set
 * with a variable name of {@code KAFKA_MAIN_CLUSTER_BOOTSTRAP_SERVERS}.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class SystemEnvPropertyOverrides implements KafkaPropertyOverrides {

    private final Map<String, ?> env;
    private Optional<ClustersProperties> props = Optional.empty();

    /**
     * Factory method
     *
     * @return kafka overrides.
     */
    public static KafkaPropertyOverrides systemEnvPropertyOverrides() {
        return new SystemEnvPropertyOverrides(System.getenv());
    }

    @VisibleForTesting
    SystemEnvPropertyOverrides(final Map<String, ?> env) {
        this.env =
                env.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(SystemEnvProperties.KAFKA_PREFIX))
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public void init(final Set<String> clusterNames) {

        final Set<String> specificPrefixes =
                clusterNames.stream()
                        .map(SystemEnvProperties::prefix)
                        .collect(Collectors.toUnmodifiableSet());

        final ClustersProperties.Builder properties = propertiesBuilder();

        extractCommon(specificPrefixes, properties);
        clusterNames.forEach(name -> extractSpecific(name, specificPrefixes, properties));

        props = Optional.of(properties.build(clusterNames));
    }

    @Override
    public Map<String, ?> get(final String clusterName) {
        return props.orElseThrow(() -> new IllegalStateException("init not called"))
                .get(clusterName);
    }

    @Override
    public boolean equals(final Object o) {
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass());
    }

    private void extractCommon(
            final Set<String> excludedPrefixes, final ClustersProperties.Builder properties) {
        env.entrySet().stream()
                .filter(e -> notExcludedPrefix(e.getKey(), excludedPrefixes))
                .forEach(
                        e ->
                                propertyName(e.getKey(), "")
                                        .ifPresent(
                                                propName ->
                                                        properties.putCommon(
                                                                propName, e.getValue())));
    }

    private void extractSpecific(
            final String clusterName,
            final Set<String> specificPrefixes,
            final ClustersProperties.Builder properties) {

        final String requiredPrefix = prefix(clusterName);
        final Set<String> excludedPrefixes = excludedPrefixes(specificPrefixes, requiredPrefix);

        env.entrySet().stream()
                .filter(e -> e.getKey().startsWith(requiredPrefix))
                .filter(e -> notExcludedPrefix(e.getKey(), excludedPrefixes))
                .forEach(
                        e ->
                                propertyName(e.getKey(), clusterName)
                                        .ifPresent(
                                                propName ->
                                                        properties.put(
                                                                clusterName,
                                                                propName,
                                                                e.getValue())));
    }

    private static Set<String> excludedPrefixes(
            final Set<String> specificPrefixes, final String allowedPrefix) {
        final Set<String> excludedPrefixes = new HashSet<>(specificPrefixes);
        excludedPrefixes.removeIf(allowedPrefix::startsWith);
        return Set.copyOf(excludedPrefixes);
    }

    private static boolean notExcludedPrefix(final String key, final Set<String> prefixes) {
        return prefixes.stream().noneMatch(key::startsWith);
    }
}
