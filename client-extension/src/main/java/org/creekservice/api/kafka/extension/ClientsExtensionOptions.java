/*
 * Copyright 2022-2025 Creek Contributors (https://github.com/creek-service)
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

package org.creekservice.api.kafka.extension;

import java.util.Optional;
import org.creekservice.api.kafka.extension.config.ClustersProperties;
import org.creekservice.api.kafka.extension.config.KafkaPropertyOverrides;
import org.creekservice.api.service.extension.CreekExtensionOptions;

/**
 * Client extension options.
 *
 * <p>Used to customise how the client extension operates.
 */
public interface ClientsExtensionOptions extends CreekExtensionOptions {

    /**
     * @return the Kafka client properties
     */
    ClustersProperties.Builder propertiesBuilder();

    /**
     * Retrieve the override instance for the supplied {@code type}, if one is set.
     *
     * @param type the type to look up.
     * @return the instance to use, if set, otherwise {@link Optional#empty()}.
     * @param <T> the type to look up.
     */
    <T> Optional<T> typeOverride(Class<T> type);

    /** Build of client extension options. */
    interface Builder {

        /**
         * Set an alternate provider of Kafka property overrides.
         *
         * <p>The default overrides provider loads them from environment variables. See {@link
         * org.creekservice.api.kafka.extension.config.SystemEnvPropertyOverrides} for more info.
         *
         * <p>It is intended that the provider should return, among other things, properties such as
         * the bootstrap servers, so that these can be configured per-environment.
         *
         * <p>Note: the properties returned by the provider will <i>override</i> any properties set
         * via {@link #withKafkaProperty}.
         *
         * <p>Note: Any custom override provider implementation may want to consider if it needs to
         * be compatible with the system tests, as the system tests set properties via environment
         * variables.
         *
         * @param overridesProvider a custom provider of Kafka overrides.
         * @return self
         */
        Builder withKafkaPropertiesOverrides(KafkaPropertyOverrides overridesProvider);

        /**
         * Set a common Kafka client property.
         *
         * <p>This property will be set for all clusters unless overridden either via {@link
         * #withKafkaProperty(String, String, Object)} or via {@link #withKafkaPropertiesOverrides}.
         *
         * @param name the name of the property
         * @param value the value of the property
         * @return self
         */
        Builder withKafkaProperty(String name, Object value);

        /**
         * Set a Kafka client property for a specific cluster.
         *
         * <p>Note: Any value set here can be overridden by the {@link #withKafkaPropertiesOverrides
         * overridesProvider}.
         *
         * @param cluster the name of the Kafka cluster this property should be scoped to.
         * @param name the name of the property
         * @param value the value of the property
         * @return self
         */
        Builder withKafkaProperty(String cluster, String name, Object value);

        /**
         * Override a specific implementation of a {@code type} used internally.
         *
         * <p>Allows the customisation of certain types within the extension. See other parts of the
         * Creek documentation for example uses.
         *
         * @param <T> the type to be overridden.
         * @param type the type to be overridden.
         * @param instance the instance to use.
         * @return self.
         */
        <T> Builder withTypeOverride(Class<T> type, T instance);

        /**
         * Build the immutable options.
         *
         * @return the built options.
         */
        ClientsExtensionOptions build();
    }
}
