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

package org.creekservice.api.kafka.serde.provider;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.creekservice.api.service.extension.CreekExtensionOptions;

/**
 * SPI for serde modules to contribute extension options during system test initialisation.
 *
 * <p>Loaded via {@link java.util.ServiceLoader} by the Kafka test extension. Each implementation
 * can provide extension options that configure its serde module for test execution.
 *
 * <p>This allows serde modules (e.g. json-serde) to integrate with the test framework without the
 * test extension depending directly on them.
 */
public interface KafkaSerdeTestExtensionInitializer {

    /**
     * Create extension options for test execution.
     *
     * <p>Called during test extension initialization. The {@code schemaRegistryEndpoints} supplier
     * is populated later when schema registry containers start.
     *
     * @param schemaRegistryEndpoints supplier of schema registry endpoint config by instance name.
     * @return extension options to register, or empty list.
     */
    List<CreekExtensionOptions> extensionOptions(
            Function<String, Map<String, String>> schemaRegistryEndpoints);
}
