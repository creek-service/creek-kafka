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

package org.creekservice.api.kafka.common.config;

import static java.util.regex.Pattern.compile;
import static org.creekservice.api.kafka.common.config.ClustersProperties.propertiesBuilder;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads Kafka client property overrides from environment variables.
 *
 * <p>Variable names are made up of three parts:
 *
 * <ol>
 *   <li>A {@code KAFKA_} prefix
 *   <li>The uppercase cluster name, (as referenced from {@link
 *       org.creekservice.api.kafka.metadata.KafkaTopicDescriptor#cluster()}).
 *   <li>The Kafka client property name, in uppercase, with periods replaced with underscores {@code
 *       _}
 * </ol>
 *
 * <p>For example, config {@code boostrap.servers} for the {@code default} cluster can be set with a
 * variable name of {@code KAFKA_DEFAULT_BOOTSTRAP_SERVERS}.
 */
public final class SystemEnvPropertyOverrides implements KafkaPropertyOverrides {

    private static final Pattern KAFKA_PROPERTY_PATTERN =
            compile("KAFKA_(?<cluster>[^_]*)_(?<name>.*)");

    public static KafkaPropertyOverrides systemEnvPropertyOverrides() {
        return new SystemEnvPropertyOverrides();
    }

    private SystemEnvPropertyOverrides() {}

    @Override
    public ClustersProperties get() {
        final ClustersProperties.Builder properties = propertiesBuilder();

        System.getenv()
                .forEach(
                        (k, v) -> {
                            final Matcher matcher = KAFKA_PROPERTY_PATTERN.matcher(k);
                            if (!matcher.matches()) {
                                return;
                            }

                            final String cluster = matcher.group("cluster").toLowerCase();
                            final String name =
                                    matcher.group("name").replaceAll("_", ".").toLowerCase();

                            properties.put(cluster, name, v);
                        });

        return properties.build();
    }
}
