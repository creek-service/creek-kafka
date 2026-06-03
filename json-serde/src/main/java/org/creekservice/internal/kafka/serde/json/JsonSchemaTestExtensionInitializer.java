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

package org.creekservice.internal.kafka.serde.json;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.creekservice.api.kafka.serde.json.JsonSerdeExtensionOptions;
import org.creekservice.api.kafka.serde.provider.KafkaSerdeTestExtensionInitializer;
import org.creekservice.api.kafka.serde.schema.store.endpoint.SchemaStoreEndpoints;
import org.creekservice.api.service.extension.CreekExtensionOptions;

/** Initializer that configures the JSON Schema serde for test execution. */
public final class JsonSchemaTestExtensionInitializer
        implements KafkaSerdeTestExtensionInitializer {

    @Override
    public List<CreekExtensionOptions> extensionOptions(
            final Function<String, Map<String, String>> schemaRegistryEndpoints) {
        return List.of(
                JsonSerdeExtensionOptions.builder()
                        .withTypeOverride(
                                SchemaStoreEndpoints.Loader.class,
                                instanceName -> {
                                    final Map<String, String> config =
                                            schemaRegistryEndpoints.apply(instanceName);
                                    final String endpoints = config.getOrDefault("endpoints", "");

                                    if (endpoints.isEmpty()) {
                                        throw new IllegalStateException(
                                                "Schema registry endpoints not found for"
                                                        + " instance: "
                                                        + instanceName);
                                    }

                                    return SchemaStoreEndpoints.create(
                                            List.of(URI.create(endpoints)), Map.of());
                                })
                        .build());
    }
}
